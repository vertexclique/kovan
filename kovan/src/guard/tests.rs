//! Reservation transitions that need the thread handle's internals: an
//! escalated critical section (forced here through `Handle::escalate`, the
//! step a protected load takes once its convergence attempts run out) and
//! the free-list cache.
//!
//! Each test runs on a fresh thread (its own slot, released at exit) and
//! the tests are serialized: the slots of every thread in the process
//! decide which batches wait for which traversal.

use super::{HANDLE, Handle, flush, pin, retire};
use crate::reclaim::MAX_CACHE;
use crate::retired::RetiredNode;
use crate::slot::{EPOCH_FREQ, EPOCH_UNCONDITIONAL, RETIRE_FREQ};
use std::boxed::Box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

static LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_handle<R>(f: impl FnOnce(&Handle) -> R) -> R {
    #[cfg(feature = "nightly")]
    {
        f(&HANDLE)
    }
    #[cfg(not(feature = "nightly"))]
    {
        HANDLE.with(f)
    }
}

/// The epoch this thread's reservation slot publishes.
fn published_epoch() -> u64 {
    with_handle(|h| h.global().thread_slots(h.tid()).epoch[0].load_lo())
}

/// A retirable value counted in `live` while it exists.
#[repr(C)]
struct Counted {
    retired: RetiredNode,
    live: Arc<AtomicUsize>,
}

impl Counted {
    fn retire_one(live: &Arc<AtomicUsize>) {
        live.fetch_add(1, Ordering::SeqCst);
        let node = Box::into_raw(Box::new(Counted {
            retired: RetiredNode::new(),
            live: Arc::clone(live),
        }));
        unsafe { retire(node) };
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A destructor that `flush()` runs may escalate its own critical section.
/// `flush()` called outside any critical section must not return with that
/// unconditional reservation still published: it would take a node of
/// every batch retired anywhere into this thread's slot until the thread
/// pins again.
#[test]
fn flush_does_not_return_escalated() {
    #[repr(C)]
    struct Escalates {
        retired: RetiredNode,
    }
    impl Drop for Escalates {
        fn drop(&mut self) {
            let guard = pin();
            with_handle(|h| h.escalate());
            drop(guard);
        }
    }

    let _l = lock();
    thread::spawn(|| {
        drop(pin());
        {
            let _guard = pin();
            let node = Box::into_raw(Box::new(Escalates {
                retired: RetiredNode::new(),
            }));
            unsafe { retire(node) };
        }
        // No other slot is active, so the batch flush() submits is freed at
        // once, running the destructor inside flush().
        flush();
        assert_ne!(published_epoch(), EPOCH_UNCONDITIONAL);
        assert_ne!(with_handle(|h| h.cached_epoch.get()), EPOCH_UNCONDITIONAL);
    })
    .join()
    .unwrap();
}

/// The transition at the outermost drop of an escalated section frees the
/// free-list cache once it is full, and a destructor it runs may retire
/// and pin. That pin must not run a second transition inside the first:
/// the inner one would store the batch it freed in the cache and the outer
/// one would overwrite the cache, losing that batch.
#[test]
fn escalated_unpin_keeps_the_batches_its_destructors_free() {
    /// Its destructor retires a full epoch's worth of counted values (two
    /// batches submitted into this thread's slot, one global epoch
    /// advance), then pins.
    #[repr(C)]
    struct RetiresThenPins {
        retired: RetiredNode,
        live: Arc<AtomicUsize>,
    }
    impl Drop for RetiresThenPins {
        fn drop(&mut self) {
            for _ in 0..EPOCH_FREQ {
                Counted::retire_one(&self.live);
            }
            drop(pin());
        }
    }

    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        drop(pin());
        // One batch: the trigger and fillers, into this thread's slot.
        {
            let _guard = pin();
            let node = Box::into_raw(Box::new(RetiresThenPins {
                retired: RetiredNode::new(),
                live: Arc::clone(&l),
            }));
            unsafe { retire(node) };
            for _ in 1..RETIRE_FREQ {
                Counted::retire_one(&l);
            }
        }
        // A transition traverses it: the batch goes to the free-list cache.
        crate::slot::advance_epoch();
        drop(pin());
        // The next traversal frees the cache first.
        with_handle(|h| h.list_count.set(MAX_CACHE));
        {
            let _guard = pin();
            with_handle(|h| h.escalate());
            // One more batch into the slot, so the transition at the drop
            // below has a list to traverse.
            for _ in 0..RETIRE_FREQ {
                Counted::retire_one(&l);
            }
        }
    })
    .join()
    .unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "values retired by a destructor were lost"
    );
}
