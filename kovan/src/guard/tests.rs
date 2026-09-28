//! Reservation transitions that need the thread handle's internals: an
//! escalated critical section (forced here through `Handle::escalate`, the
//! step a protected load takes once its convergence attempts run out), the
//! free-list cache, and threads held at a protocol step (see `stall`) while
//! other threads' operations must still complete.
//!
//! Each test runs on fresh threads (their own slots, released at exit) and
//! the tests are serialized: the slots of every thread in the process
//! decide which batches wait for which traversal.

use super::{HANDLE, Handle, flush, pin, retire};
use crate::reclaim::MAX_CACHE;
use crate::retired::RetiredNode;
use crate::slot::{EPOCH_FREQ, EPOCH_UNCONDITIONAL, RETIRE_FREQ};
use crate::stall::{self, Step};
use std::boxed::Box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

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

/// `with_handle`, also inside this thread's exit, where `HANDLE` itself no
/// longer answers on builds whose handle is a destructed thread-local.
fn with_current_handle<R>(f: impl FnOnce(&Handle) -> R) -> R {
    #[cfg(feature = "nightly")]
    {
        f(&HANDLE)
    }
    #[cfg(not(feature = "nightly"))]
    {
        match HANDLE.try_with(|h| h as *const Handle) {
            Ok(h) => f(unsafe { &*h }),
            Err(_) => super::on_exiting_handle(f).expect("no handle on this thread"),
        }
    }
}

/// The epoch this thread's reservation slot publishes.
fn published_epoch() -> u64 {
    with_handle(|h| h.global().thread_slots(h.tid()).epoch[0].load_lo())
}

/// This thread's ID, allocated by a first pin if it has none.
fn own_tid() -> usize {
    drop(pin());
    with_handle(|h| h.tid())
}

/// How long a test waits for an operation that must complete while another
/// thread is held, and how long a held thread waits to be let go.
const DEADLINE: Duration = Duration::from_secs(60);

/// Yield until `done` holds; false if `DEADLINE` passes first.
fn eventually(done: impl Fn() -> bool) -> bool {
    let end = Instant::now() + DEADLINE;
    while !done() {
        if Instant::now() > end {
            return false;
        }
        thread::yield_now();
    }
    true
}

/// Joins `t`, failing the test if its main function has not returned
/// within `DEADLINE` (the join then waits for its exit).
fn join_within<T>(t: thread::JoinHandle<T>, what: &str) -> T {
    assert!(eventually(|| t.is_finished()), "{what} did not complete");
    t.join().unwrap()
}

/// A thread held at a protocol step until the test lets it go (or until
/// `DEADLINE`, so that a failing test still ends).
#[derive(Clone)]
struct Hold {
    arrived: Arc<AtomicBool>,
    go: Arc<AtomicBool>,
}

impl Hold {
    fn new() -> Self {
        Self {
            arrived: Arc::new(AtomicBool::new(false)),
            go: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Hold the thread holding `tid` the next time it reaches `step`.
    fn arm(&self, step: Step, tid: usize) {
        let hold = self.clone();
        stall::arm(step, tid, move || {
            hold.arrived.store(true, Ordering::SeqCst);
            eventually(|| hold.go.load(Ordering::SeqCst));
            false
        });
    }

    /// Wait until the held thread reached its step.
    fn reached(&self, what: &str) {
        assert!(
            eventually(|| self.arrived.load(Ordering::SeqCst)),
            "{what} never reached its step"
        );
    }

    fn release(&self) {
        self.go.store(true, Ordering::SeqCst);
    }
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

    /// Retire one stamped with the first epoch as its birth: its batch
    /// takes a node in every active slot (an older birth only defers it
    /// more), so a lone one cannot be placed while another thread is
    /// pinned.
    fn retire_oldest(live: &Arc<AtomicUsize>) {
        live.fetch_add(1, Ordering::SeqCst);
        let retired = RetiredNode::new();
        retired.set_birth_epoch(1);
        let node = Box::into_raw(Box::new(Counted {
            retired,
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

/// The slow path of a transition frees the full free-list cache once its
/// slot is live again (never before: see `slow_path`), and a destructor it
/// runs retires a full epoch's worth, which submits two batches and helps
/// the pending slow-path threads before it advances the epoch. Traversals
/// that re-entrant work makes push onto the cache the free already took out
/// of its cell, never onto a copy of it: freeing the copy once more would
/// free every batch in it twice. Everything is freed once.
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

/// A destructor that a thread's exit runs is a critical section and a
/// retirer like any other, also where the exit runs inside the handle's own
/// thread-local destructor: a load it makes after the epoch moved raises
/// the thread's reservation (it is protected), and a value it retires is
/// freed, never leaked.
#[test]
fn exit_destructors_reach_the_handle() {
    /// Its destructor loads under a guard after an epoch advance, records
    /// whether the load raised its thread's reservation, then retires one
    /// counted value.
    #[repr(C)]
    struct LoadsAndRetires {
        retired: RetiredNode,
        tid: usize,
        shared: Arc<crate::Atomic<u64>>,
        raised: Arc<AtomicBool>,
        live: Arc<AtomicUsize>,
    }
    impl Drop for LoadsAndRetires {
        fn drop(&mut self) {
            crate::slot::advance_epoch();
            let guard = pin();
            let _ = self.shared.load(Ordering::Acquire, &guard);
            let published = crate::slot::global().thread_slots(self.tid).epoch[0].load_lo();
            self.raised
                .store(published == crate::slot::epoch(), Ordering::SeqCst);
            drop(guard);
            Counted::retire_one(&self.live);
        }
    }

    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let raised = Arc::new(AtomicBool::new(false));
    let shared = Arc::new(crate::Atomic::<u64>::null());
    let (l, r) = (Arc::clone(&live), Arc::clone(&raised));
    thread::spawn(move || {
        let tid = own_tid();
        // A lone batch, left unsubmitted: the exit submits it, no other
        // slot is active, so it is freed at once, inside the exit.
        let node = Box::into_raw(Box::new(LoadsAndRetires {
            retired: RetiredNode::new(),
            tid,
            shared,
            raised: r,
            live: l,
        }));
        unsafe { retire(node) };
    })
    .join()
    .unwrap();
    assert!(
        raised.load(Ordering::SeqCst),
        "a load made by a destructor the exit ran did not raise the reservation"
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "a value retired by a destructor the exit ran was not freed"
    );
}

/// A thread's exit takes its slot list a fixed number of times: other
/// threads that keep retiring into the slot while the exit runs, with
/// values whose destructors retire in turn, cannot keep it running. Each
/// value freed from the slot here places one more such batch in the
/// exiting thread's slot, as another thread's retire would, and retires one
/// value of its own. The exit runs one generation of them per round; the
/// next waits in the slot, is parked when the slot is deactivated, and is
/// freed by an adopter.
#[test]
fn exit_takes_its_slot_list_a_fixed_number_of_times() {
    /// Places one more generation in this thread's slot while its exit
    /// runs, `left` more at most.
    #[repr(C)]
    struct Feeds {
        retired: RetiredNode,
        left: usize,
        exiting: Arc<AtomicBool>,
        fed: Arc<AtomicUsize>,
        live: Arc<AtomicUsize>,
    }
    impl Feeds {
        /// A batch of a feeder and a counted value, submitted through
        /// try_retire into this thread's slot (the only active one).
        fn place(
            left: usize,
            exiting: &Arc<AtomicBool>,
            fed: &Arc<AtomicUsize>,
            live: &Arc<AtomicUsize>,
        ) {
            with_current_handle(|h| {
                live.fetch_add(1, Ordering::SeqCst);
                let node = Box::into_raw(Box::new(Feeds {
                    retired: RetiredNode::new(),
                    left,
                    exiting: Arc::clone(exiting),
                    fed: Arc::clone(fed),
                    live: Arc::clone(live),
                }));
                unsafe { retire(node) };
                Counted::retire_one(live);
                let (first, last) = h.take_batch().expect("the batch just retired");
                assert!(h.try_retire(first, last, None), "the batch was not placed");
            });
        }
    }
    impl Drop for Feeds {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
            if self.left == 0 || !self.exiting.load(Ordering::SeqCst) {
                return;
            }
            self.fed.fetch_add(1, Ordering::SeqCst);
            Feeds::place(self.left - 1, &self.exiting, &self.fed, &self.live);
            Counted::retire_one(&self.live);
        }
    }

    const GENERATIONS: usize = 64;
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let exiting = Arc::new(AtomicBool::new(false));
    let fed = Arc::new(AtomicUsize::new(0));
    let (l, e, f) = (Arc::clone(&live), Arc::clone(&exiting), Arc::clone(&fed));
    thread::spawn(move || {
        own_tid();
        Feeds::place(GENERATIONS, &e, &f, &l);
        e.store(true, Ordering::SeqCst);
    })
    .join()
    .unwrap();
    exiting.store(false, Ordering::SeqCst);
    // One per round: the one each take of the slot list found.
    let generations = fed.load(Ordering::SeqCst);
    assert_eq!(
        generations,
        super::EXIT_ROUNDS,
        "the exit ran {generations} generations while its slot kept being fed"
    );

    // Nothing leaks: the parked generation is adopted and freed.
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        let freed = eventually(|| {
            flush();
            l.load(Ordering::SeqCst) == 0
        });
        assert!(freed, "{} values never freed", l.load(Ordering::SeqCst));
    })
    .join()
    .unwrap();
}

mod progress;
