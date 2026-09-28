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

/// A thread's exit runs the destructors of what its slot held, and such a
/// destructor may pin and load, publishing into the thread's slot. Until
/// they have all run the thread must keep its ID: a thread that took the ID
/// over meanwhile would own the slot those publications write.
#[test]
fn exit_keeps_its_tid_until_its_destructors_ran() {
    #[repr(C)]
    struct ChecksTid {
        retired: RetiredNode,
        tid: usize,
        released_early: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for ChecksTid {
        fn drop(&mut self) {
            if crate::slot::global().tid_is_released(self.tid) {
                self.released_early.fetch_add(1, Ordering::SeqCst);
            }
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let _l = lock();
    let released_early = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&released_early);
    let d = Arc::clone(&dropped);
    thread::spawn(move || {
        drop(pin());
        let tid = with_handle(|h| h.tid());
        // A full batch into this thread's slot (no other slot is active),
        // left there for the exit to traverse and free.
        let _guard = pin();
        for _ in 0..RETIRE_FREQ {
            let node = Box::into_raw(Box::new(ChecksTid {
                retired: RetiredNode::new(),
                tid,
                released_early: Arc::clone(&r),
                dropped: Arc::clone(&d),
            }));
            unsafe { retire(node) };
        }
    })
    .join()
    .unwrap();
    // The exit freed the whole batch.
    assert_eq!(dropped.load(Ordering::SeqCst), RETIRE_FREQ);
    assert_eq!(
        released_early.load(Ordering::SeqCst),
        0,
        "destructors ran after the exiting thread released its tid"
    );
}

/// The slow path of a transition frees the full free-list cache, and a
/// destructor it runs retires a full epoch's worth, which helps the pending
/// slow-path threads, this one among them: the helper traverses this
/// thread's list into the cache. That traversal must push onto the cache
/// the outer free already took out of its cell, never onto a copy of it:
/// freeing the copy once more would free every batch in it twice.
#[test]
fn slow_path_free_survives_helping_itself() {
    /// Its destructor retires a full epoch's worth of counted values.
    #[repr(C)]
    struct RetiresAnEpoch {
        retired: RetiredNode,
        live: Arc<AtomicUsize>,
    }
    impl Drop for RetiresAnEpoch {
        fn drop(&mut self) {
            for _ in 0..EPOCH_FREQ {
                Counted::retire_one(&self.live);
            }
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        drop(pin());
        // One batch, the trigger in it, into this thread's slot.
        {
            let _guard = pin();
            l.fetch_add(1, Ordering::SeqCst);
            let node = Box::into_raw(Box::new(RetiresAnEpoch {
                retired: RetiredNode::new(),
                live: Arc::clone(&l),
            }));
            unsafe { retire(node) };
            for _ in 1..RETIRE_FREQ {
                Counted::retire_one(&l);
            }
        }
        // A transition moves it to the free-list cache.
        crate::slot::advance_epoch();
        drop(pin());
        // The next traversal frees the cache first.
        with_handle(|h| h.list_count.set(MAX_CACHE));
        // One more batch into the slot. Its last retire advances the
        // epoch, so a transition is due.
        {
            let _guard = pin();
            for _ in 0..RETIRE_FREQ {
                Counted::retire_one(&l);
            }
        }
        // The transition, taken through its slow path (as when the epoch
        // does not settle within the fast attempts), inside a pin.
        with_handle(|h| {
            h.pin_count.set(1);
            h.slow_path(0, h.tid());
            h.pin_count.set(0);
        });
    })
    .join()
    .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// A retire whose count reaches both the epoch-advance and the batch
/// boundary helps pending slow-path threads first, and the help may free
/// cached batches whose destructors retire in turn, a full batch among
/// them. The batch this retire then submits must be the one in the cells
/// at that point, never one sized from a count read before the help (the
/// nested retires may have submitted and emptied it).
#[test]
fn retire_submits_the_batch_left_after_helping() {
    /// Its destructor retires one full batch of counted values.
    #[repr(C)]
    struct RetiresABatch {
        retired: RetiredNode,
        live: Arc<AtomicUsize>,
    }
    impl Drop for RetiresABatch {
        fn drop(&mut self) {
            for _ in 0..RETIRE_FREQ {
                Counted::retire_one(&self.live);
            }
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        let global = crate::slot::global();
        drop(pin());
        // One batch, the trigger in it, into this thread's slot, then into
        // the free-list cache through a transition.
        {
            let _guard = pin();
            l.fetch_add(1, Ordering::SeqCst);
            let node = Box::into_raw(Box::new(RetiresABatch {
                retired: RetiredNode::new(),
                live: Arc::clone(&l),
            }));
            unsafe { retire(node) };
            for _ in 1..RETIRE_FREQ {
                Counted::retire_one(&l);
            }
        }
        crate::slot::advance_epoch();
        drop(pin());
        // The next traversal frees the cache first.
        with_handle(|h| h.list_count.set(MAX_CACHE));

        // Another thread pending in the slow path: an active slot whose
        // help request is open.
        let pending = global.alloc_tid();
        let slots = global.thread_slots(pending);
        slots.epoch[0].store_lo(crate::slot::epoch(), Ordering::SeqCst);
        slots.first[0].store_lo(0, Ordering::SeqCst);
        slots.state[0].pointer.store(0, Ordering::SeqCst);
        slots.state[0].parent.store(0, Ordering::SeqCst);
        slots.state[0].epoch.store(0, Ordering::SeqCst);
        let seqno = slots.epoch[0].load_hi();
        slots.state[0]
            .result
            .store(super::INVPTR as u64, seqno, Ordering::SeqCst);
        global.inc_slow();

        // Three batches: the first two leave nodes in the pending slot; the
        // third one's last retire is both an epoch-advance and a batch
        // boundary, and its help traverses the pending slot's list, which
        // frees the cache and so runs the trigger.
        {
            let _guard = pin();
            for _ in 0..3 * RETIRE_FREQ {
                Counted::retire_one(&l);
            }
        }

        global.dec_slow();
        for first in global.deactivate_slots(pending) {
            if first != 0 {
                with_handle(|h| unsafe { h.traverse_into_cache(first as *mut RetiredNode) });
            }
        }
        global.release_tid(pending);
    })
    .join()
    .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// A thread's exit runs the destructors of what its slot held and of what
/// its last batch frees, and such a destructor may load. They run before
/// the thread leaves the protocol, while its slot is still active, so their
/// loads are protected as any other load is.
#[test]
fn exit_runs_its_destructors_with_its_slot_active() {
    #[repr(C)]
    struct ChecksSlot {
        retired: RetiredNode,
        tid: usize,
        inactive: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for ChecksSlot {
        fn drop(&mut self) {
            let first = crate::slot::global().thread_slots(self.tid).first[0].load_lo();
            if first == super::INVPTR as u64 {
                self.inactive.fetch_add(1, Ordering::SeqCst);
            }
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let _l = lock();
    let inactive = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let i = Arc::clone(&inactive);
    let d = Arc::clone(&dropped);
    thread::spawn(move || {
        drop(pin());
        let tid = with_handle(|h| h.tid());
        // A full batch into this thread's slot, and a partial one left in
        // the thread's batch, both for the exit to free.
        let _guard = pin();
        for _ in 0..RETIRE_FREQ + RETIRE_FREQ / 2 {
            let node = Box::into_raw(Box::new(ChecksSlot {
                retired: RetiredNode::new(),
                tid,
                inactive: Arc::clone(&i),
                dropped: Arc::clone(&d),
            }));
            unsafe { retire(node) };
        }
    })
    .join()
    .unwrap();
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        RETIRE_FREQ + RETIRE_FREQ / 2
    );
    assert_eq!(
        inactive.load(Ordering::SeqCst),
        0,
        "destructors ran after the exiting thread deactivated its slot"
    );
}
