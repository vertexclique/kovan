//! Critical sections a thread-local destructor opens after its thread's exit
//! is over (see `late_pin`): the guard protects what it loads, nested pins
//! share the section, and the section leaves the protocol as an exit does
//! when its last guard drops, giving its thread ID back.
//!
//! Each test sets a thread-local before the thread's first pin, so that its
//! destructor runs after kovan's own, then checks from inside it.

use super::{Counted, DEADLINE, exit_over, join_within, lock};
use crate::guard::{flush, pin, retire};
use crate::retired::RetiredNode;
#[cfg(feature = "nightly")]
use crate::slot::EPOCH_UNCONDITIONAL;
use std::boxed::Box;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

/// The ID and the published epoch of this thread's late section, if one is
/// live.
fn late_section() -> Option<(usize, u64)> {
    let epoch_of = |tid: usize| crate::slot::global().thread_slots(tid).epoch[0].load_lo();
    #[cfg(feature = "nightly")]
    {
        let handle = &super::HANDLE;
        if !handle.late.get() {
            return None;
        }
        handle.tid.get().map(|tid| (tid, epoch_of(tid)))
    }
    #[cfg(not(feature = "nightly"))]
    {
        crate::guard::on_late_handle(|handle| handle.tid.get().map(|tid| (tid, epoch_of(tid))))
            .flatten()
    }
}

/// Whether this thread's handles hold no thread ID any more: on nightly the
/// handle's own is cleared and the handle no longer marked late; elsewhere
/// no late section's handle is left (its drop, the section's exit, released
/// the ID). The exit clears the ID only after releasing it. Read on the
/// thread itself, not through the global ID table, where a released ID may
/// already belong to another test's thread.
fn holds_no_tid() -> bool {
    #[cfg(feature = "nightly")]
    {
        super::HANDLE.tid.get().is_none() && !super::HANDLE.late.get()
    }
    #[cfg(not(feature = "nightly"))]
    {
        crate::guard::on_late_handle(|_| ()).is_none()
    }
}

/// Runs `body` in a thread-local destructor of a fresh thread that runs
/// after the thread's exit, and returns what it returns.
fn after_exit<R: Send + 'static>(body: impl FnOnce() -> R + Send + 'static) -> R {
    struct Late(Option<Box<dyn FnOnce() + Send>>);
    impl Drop for Late {
        fn drop(&mut self) {
            if let Some(body) = self.0.take() {
                body();
            }
        }
    }
    thread_local! {
        static LATE: RefCell<Option<Late>> = const { RefCell::new(None) };
    }
    let (tx, rx) = mpsc::channel();
    let t = thread::spawn(move || {
        LATE.with(|late| {
            *late.borrow_mut() = Some(Late(Some(Box::new(move || {
                assert!(
                    exit_over(),
                    "the destructor ran before the thread's exit was over: the case is not reached"
                );
                let _ = tx.send(body());
            }))))
        });
        // The handle's destructor (the exit) is registered after `LATE`'s,
        // so it runs first.
        drop(pin());
    });
    join_within(
        t,
        "a thread whose thread-local destructor pins after its exit",
    );
    rx.recv_timeout(DEADLINE)
        .expect("the late destructor reported nothing")
}

/// A pin in a thread-local destructor that runs after the thread's exit
/// holds a reservation under an ID of its own (on nightly the unconditional
/// one) for as long as its guard lives, and gives the ID back when the guard
/// drops.
///
/// Before the fix the pin returned a guard that reserved nothing (and on
/// nightly it panicked claiming an ID), and this test failed.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_pin_after_the_exit_reserves_until_its_guard_drops() {
    let _l = lock();
    let (during, after, released) = after_exit(|| {
        let guard = pin();
        let during = late_section();
        drop(guard);
        (during, late_section(), holds_no_tid())
    });
    let (_, epoch) = during.expect("the pin after the exit holds no reservation");
    assert_ne!(epoch, 0, "the section after the exit publishes no epoch");
    #[cfg(feature = "nightly")]
    assert_eq!(
        epoch, EPOCH_UNCONDITIONAL,
        "the section after the exit does not publish the unconditional reservation"
    );
    assert!(
        after.is_none(),
        "the section after the exit outlived its guard"
    );
    assert!(released, "the section after the exit kept its thread ID");
}

/// Nested pins after the exit share one section: the inner drop leaves it
/// in place, the outer one ends it.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn nested_pins_after_the_exit_end_the_section_once() {
    let _l = lock();
    let (outer, inner, after, released) = after_exit(|| {
        let a = pin();
        let outer = late_section();
        let b = pin();
        drop(b);
        let inner = late_section();
        drop(a);
        (outer, inner, late_section(), holds_no_tid())
    });
    let outer = outer.expect("the outer pin after the exit holds no reservation");
    assert_eq!(
        inner,
        Some(outer),
        "the inner drop ended the section after the exit"
    );
    assert!(
        after.is_none(),
        "the outer drop left the section after the exit"
    );
    assert!(released, "the section after the exit kept its thread ID");
}

/// What a section after the exit loads stays allocated while its guard
/// lives, though another thread replaces it, retires it and flushes; once
/// the guard drops, it is freed.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_load_after_the_exit_is_protected() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(1));
    let first = Box::into_raw(Box::new(Counted {
        retired: RetiredNode::new(),
        live: Arc::clone(&live),
    }));
    let cell = Arc::new(crate::Atomic::new(first));
    let (loaded_tx, loaded_rx) = mpsc::channel();
    let (retired_tx, retired_rx) = mpsc::channel::<()>();
    let (c, l) = (Arc::clone(&cell), Arc::clone(&live));
    let late = thread::spawn(move || {
        after_exit(move || {
            let guard = pin();
            let held = c.load(Ordering::Acquire, &guard);
            let _ = loaded_tx.send(());
            let _ = retired_rx.recv_timeout(DEADLINE);
            // The count says whether it was freed; the read of the value
            // itself is what AddressSanitizer reports if it was.
            let alive = l.load(Ordering::SeqCst) == 1
                && unsafe { held.as_ref() }.is_some_and(|c| Arc::ptr_eq(&c.live, &l));
            drop(guard);
            alive
        })
    });
    loaded_rx
        .recv_timeout(DEADLINE)
        .expect("the late section loaded nothing");
    {
        let guard = pin();
        let old = cell.swap(
            unsafe { crate::Shared::from_raw(core::ptr::null_mut()) },
            Ordering::AcqRel,
            &guard,
        );
        let old = old.as_raw();
        drop(guard);
        unsafe { retire(old) };
    }
    for _ in 0..=crate::slot::global().max_threads() {
        flush();
    }
    retired_tx.send(()).unwrap();
    let alive = join_within(late, "the thread running the late section");
    assert!(
        alive,
        "a value loaded after the thread's exit was freed while its guard lived"
    );
    for _ in 0..=crate::slot::global().max_threads() {
        if live.load(Ordering::SeqCst) == 0 {
            break;
        }
        flush();
    }
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the value was never freed after the late section ended"
    );
}

/// A value retired inside a section after the exit goes out with the
/// section's end (its handle's exit) and is freed.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_retire_inside_a_section_after_the_exit_is_freed() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    after_exit(move || {
        let guard = pin();
        Counted::retire_one(&l);
        drop(guard);
    });
    for _ in 0..=crate::slot::global().max_threads() {
        if live.load(Ordering::SeqCst) == 0 {
            break;
        }
        flush();
    }
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "a value retired in a section after the thread's exit was never freed"
    );
}
