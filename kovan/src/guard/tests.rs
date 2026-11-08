//! Reservation transitions that need the thread handle's internals: an
//! escalated critical section (forced here through `Handle::escalate`, the
//! step a protected load takes once its convergence attempts run out), the
//! free-list cache, the fillers a padded placement adds, and threads held
//! at a protocol step (see `stall`) while other threads' operations must
//! still complete.
//!
//! Each test runs on fresh threads (their own slots, released at exit) and
//! the tests are serialized: the slots of every thread in the process
//! decide which batches wait for which traversal.

use super::{HANDLE, Handle, flush, pin, retire};
use crate::reclaim::MAX_CACHE;
use crate::retired::{INVPTR, RetiredNode};
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

/// Whether this thread's exit has run and is over: neither the handle nor
/// the exiting handle takes a retire, which goes through a handle of its own.
fn exit_over() -> bool {
    #[cfg(feature = "nightly")]
    {
        !super::exit_pending() && !HANDLE.in_reclaim.get()
    }
    #[cfg(not(feature = "nightly"))]
    {
        HANDLE.try_with(|_| ()).is_err() && super::on_exiting_handle(|_| ()).is_none()
    }
}

/// The epoch this thread's reservation slot publishes.
fn published_epoch() -> u64 {
    with_handle(|h| h.global().thread_slots(h.tid()).epoch[0].load_lo())
}

/// Whether the reservation slot of `tid` is inactive (left).
fn slot_inactive(tid: usize) -> bool {
    crate::slot::global().thread_slots(tid).first[0].load_lo() == INVPTR as u64
}

/// This thread's ID, allocated by a first pin if it has none.
fn own_tid() -> usize {
    drop(pin());
    with_handle(|h| h.tid())
}

/// Spend this thread's first retire, which advances the epoch (helping the
/// pending slow paths first, as every advance does), on a counted value it
/// submits: a test that schedules each epoch move and each help counts the
/// retires after this one.
fn spend_first_retire(live: &Arc<AtomicUsize>) {
    Counted::retire_one(live);
    flush();
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

/// A thread that pins and stays pinned until the test lets it go: its slot
/// is active and eligible for every batch of oldest-born values.
fn pinned_thread() -> (Hold, thread::JoinHandle<()>) {
    let hold = Hold::new();
    let h = hold.clone();
    let t = thread::spawn(move || {
        let guard = pin();
        h.arrived.store(true, Ordering::SeqCst);
        assert!(eventually(|| h.go.load(Ordering::SeqCst)));
        drop(guard);
    });
    hold.reached("the pinned thread");
    (hold, t)
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

    /// Retire `RETIRE_FREQ - 1` values stamped with the first epoch into
    /// this thread's empty batch, then place the batch with `try_retire`
    /// straight away: another thread's batch in every active slot, with no
    /// epoch advance after it (a retire that places a batch advances), so
    /// the advances stay the test's own schedule.
    fn place_oldest(live: &Arc<AtomicUsize>) {
        for _ in 0..RETIRE_FREQ - 1 {
            Counted::retire_oldest(live);
        }
        with_handle(|h| {
            let (first, last) = h.take_batch().expect("the batch just retired");
            assert!(h.try_retire(first, last, false), "the batch was not placed");
        });
    }

    /// Retire one stamped with the first epoch as its birth: its batch
    /// takes a node in every active slot (an older birth only defers it
    /// more), so a lone one cannot be placed unpadded while another thread
    /// is pinned.
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
/// That reservation ends with the destructor's guard: the slot is inactive
/// once `flush()` returns, so no batch retired anywhere is placed in it, and
/// the thread's next pin publishes a real epoch.
#[test]
fn an_escalated_destructor_section_ends_with_its_drop() {
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
        let tid = own_tid();
        let node = Box::into_raw(Box::new(Escalates {
            retired: RetiredNode::new(),
        }));
        unsafe { retire(node) };
        // No other slot is active, so the batch flush() submits is freed at
        // once, running the destructor inside flush().
        flush();
        assert!(slot_inactive(tid), "flush returned with its slot active");
        let guard = pin();
        assert_ne!(published_epoch(), EPOCH_UNCONDITIONAL);
        assert_eq!(published_epoch(), crate::slot::epoch());
        assert_ne!(with_handle(|h| h.cached_epoch.get()), EPOCH_UNCONDITIONAL);
        drop(guard);
    })
    .join()
    .unwrap();
}

/// The outermost drop frees what its traversal released, and a destructor
/// it runs may retire a full epoch's worth (batches submitted and freed at
/// once, an epoch advance with its help) and pin, an outermost pin that
/// enters the slot for a section of its own. Every value is freed once.
#[test]
fn leave_keeps_the_batches_its_destructors_free() {
    /// Its destructor retires a full epoch's worth of counted values, then
    /// pins and unpins.
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
        let tid = own_tid();
        // One batch, the trigger and fillers, into this thread's slot (the
        // only active one); the drop below traverses and frees it.
        let guard = pin();
        let node = Box::into_raw(Box::new(RetiresThenPins {
            retired: RetiredNode::new(),
            live: Arc::clone(&l),
        }));
        unsafe { retire(node) };
        for _ in 1..RETIRE_FREQ {
            Counted::retire_one(&l);
        }
        assert!(!slot_inactive(tid));
        drop(guard);
        assert!(slot_inactive(tid), "the drop left its slot active");
    })
    .join()
    .unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "values retired by a destructor were lost"
    );
}

/// A thread's exit runs the destructors of what it places and frees, and
/// such a destructor may pin and load, entering the thread's slot. Until
/// they have all run the thread must keep its ID: a thread that took the ID
/// over meanwhile would own the slot those sections enter.
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
            drop(pin());
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let _l = lock();
    let released_early = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&released_early);
    let d = Arc::clone(&dropped);
    thread::spawn(move || {
        let tid = own_tid();
        // A partial batch, left for the exit to place and free (no other
        // slot is active, so it is freed at once, inside the exit).
        for _ in 0..RETIRE_FREQ / 2 {
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
    assert_eq!(dropped.load(Ordering::SeqCst), RETIRE_FREQ / 2);
    assert_eq!(
        released_early.load(Ordering::SeqCst),
        0,
        "destructors ran after the exiting thread released its tid"
    );
}

/// The slow path of a transition frees the free-list cache once its slot
/// is live again (never before: see `slow_path`), and a destructor it runs
/// retires a full epoch's worth, which submits two batches into this
/// thread's slot and helps the pending slow-path threads before it advances
/// the epoch. Traversals that re-entrant work makes push onto the cache the
/// free already took out of its cell, never onto a copy of it. Everything
/// is freed once, the last of it by the guard's drop.
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
        let tid = own_tid();
        // One batch, the trigger in it, into this thread's slot, under the
        // guard whose section the slow path runs in.
        let guard = pin();
        l.fetch_add(1, Ordering::SeqCst);
        let node = Box::into_raw(Box::new(RetiresAnEpoch {
            retired: RetiredNode::new(),
            live: Arc::clone(&l),
        }));
        unsafe { retire(node) };
        for _ in 1..RETIRE_FREQ {
            Counted::retire_one(&l);
        }
        // The next free empties the cache first.
        with_handle(|h| h.list_count.set(MAX_CACHE));
        // The transition, taken through its slow path (as when the epoch
        // does not settle within the fast attempts), inside the pin: its
        // loop traverses the slot list onto the cache, and its drain runs
        // the trigger.
        with_handle(|h| h.slow_path(0, tid));
        drop(guard);
    })
    .join()
    .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// A retire that places a batch helps pending slow-path threads first, and
/// the help may free cached batches whose destructors retire in turn, a
/// full batch among them. The batch this retire then submits must be the
/// one in the cells at that point, never one sized from a count read before
/// the help (the nested retires may have submitted and emptied it).
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
        let tid = own_tid();
        let guard = pin();
        // One batch, the trigger in it, into this thread's slot, then onto
        // the free-list cache, full, by a traversal of the slot list.
        l.fetch_add(1, Ordering::SeqCst);
        let node = Box::into_raw(Box::new(RetiresABatch {
            retired: RetiredNode::new(),
            live: Arc::clone(&l),
        }));
        unsafe { retire(node) };
        for _ in 1..RETIRE_FREQ {
            Counted::retire_one(&l);
        }
        with_handle(|h| {
            let first = global.thread_slots(tid).first[0].exchange_lo(0, Ordering::AcqRel);
            assert!(
                first != 0 && first != INVPTR as u64,
                "the batch skipped the own slot"
            );
            unsafe { h.traverse_onto_cache(first as *mut RetiredNode) };
            h.list_count.set(MAX_CACHE);
        });

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

        // Three batches, each leaving a node in the pending slot and in this
        // one; the first placed helps, and its help traverses the pending
        // slot's list, which frees the cache and so runs the trigger.
        for _ in 0..3 * RETIRE_FREQ {
            Counted::retire_one(&l);
        }

        global.dec_slow();
        // The pending thread leaves as an exit would.
        let first = slots.first[0].exchange_lo(INVPTR as u64, Ordering::AcqRel);
        if first != 0 && first != INVPTR as u64 {
            with_handle(|h| unsafe { h.traverse_into_cache(first as *mut RetiredNode) });
        }
        global.release_tid(pending);
        drop(guard);
    })
    .join()
    .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// A thread's exit runs its destructors with its slot left: a destructor
/// that pins enters the slot for a section of its own (active while that
/// section lives, left at its end), as any code outside a section does, so
/// its loads are protected by its own reservation.
#[test]
fn exit_destructors_enter_the_slot_for_their_own_sections() {
    #[repr(C)]
    struct ChecksSlot {
        retired: RetiredNode,
        tid: usize,
        active_outside: Arc<AtomicUsize>,
        inactive_inside: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for ChecksSlot {
        fn drop(&mut self) {
            if !slot_inactive(self.tid) {
                self.active_outside.fetch_add(1, Ordering::SeqCst);
            }
            let guard = pin();
            if slot_inactive(self.tid) {
                self.inactive_inside.fetch_add(1, Ordering::SeqCst);
            }
            drop(guard);
            if !slot_inactive(self.tid) {
                self.active_outside.fetch_add(1, Ordering::SeqCst);
            }
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let _l = lock();
    let active_outside = Arc::new(AtomicUsize::new(0));
    let inactive_inside = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let (a, i, d) = (
        Arc::clone(&active_outside),
        Arc::clone(&inactive_inside),
        Arc::clone(&dropped),
    );
    thread::spawn(move || {
        let tid = own_tid();
        // A partial batch left in the thread's batch for the exit to place
        // and free (no other slot is active).
        for _ in 0..RETIRE_FREQ / 2 {
            let node = Box::into_raw(Box::new(ChecksSlot {
                retired: RetiredNode::new(),
                tid,
                active_outside: Arc::clone(&a),
                inactive_inside: Arc::clone(&i),
                dropped: Arc::clone(&d),
            }));
            unsafe { retire(node) };
        }
    })
    .join()
    .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), RETIRE_FREQ / 2);
    assert_eq!(
        active_outside.load(Ordering::SeqCst),
        0,
        "the exiting thread's slot was active outside a destructor's section"
    );
    assert_eq!(
        inactive_inside.load(Ordering::SeqCst),
        0,
        "a destructor's pin at exit did not enter the slot"
    );
}

/// A destructor that a thread's exit runs is a critical section and a
/// retirer like any other, also where the exit runs inside the handle's own
/// thread-local destructor: a load it makes after the epoch moved is made
/// under a reservation at the current epoch (it is protected), and a value
/// it retires is freed, never leaked.
#[test]
fn exit_destructors_reach_the_handle() {
    /// Its destructor loads under a guard after an epoch advance, records
    /// whether its thread's reservation covered the load, then retires one
    /// counted value.
    #[repr(C)]
    struct LoadsAndRetires {
        retired: RetiredNode,
        tid: usize,
        shared: Arc<crate::Atomic<u64>>,
        covered: Arc<AtomicBool>,
        live: Arc<AtomicUsize>,
    }
    impl Drop for LoadsAndRetires {
        fn drop(&mut self) {
            crate::slot::advance_epoch();
            let guard = pin();
            let _ = self.shared.load(Ordering::Acquire, &guard);
            let published = crate::slot::global().thread_slots(self.tid).epoch[0].load_lo();
            self.covered.store(
                !slot_inactive(self.tid) && published == crate::slot::epoch(),
                Ordering::SeqCst,
            );
            drop(guard);
            Counted::retire_one(&self.live);
        }
    }

    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let covered = Arc::new(AtomicBool::new(false));
    let shared = Arc::new(crate::Atomic::<u64>::null());
    let (l, c) = (Arc::clone(&live), Arc::clone(&covered));
    thread::spawn(move || {
        let tid = own_tid();
        // A lone batch, left unsubmitted: the exit submits it, no other
        // slot is active, so it is freed at once, inside the exit.
        let node = Box::into_raw(Box::new(LoadsAndRetires {
            retired: RetiredNode::new(),
            tid,
            shared,
            covered: c,
            live: l,
        }));
        unsafe { retire(node) };
    })
    .join()
    .unwrap();
    assert!(
        covered.load(Ordering::SeqCst),
        "a load made by a destructor the exit ran was not covered by its reservation"
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "a value retired by a destructor the exit ran was not freed"
    );
}

/// A thread's exit frees what its destructors retire in a chain, however
/// long: each value freed retires the next, which the exit places and frees
/// in turn, with its slot inactive throughout (no other thread's retire
/// lands in it), until nothing is left. No flush anywhere is needed.
#[test]
fn exit_frees_a_chain_of_retiring_destructors() {
    /// Retires the next link of the chain when dropped, `left` more.
    #[repr(C)]
    struct Chain {
        retired: RetiredNode,
        left: usize,
        tid: usize,
        active: Arc<AtomicUsize>,
        live: Arc<AtomicUsize>,
    }
    impl Chain {
        fn retire_link(
            left: usize,
            tid: usize,
            active: &Arc<AtomicUsize>,
            live: &Arc<AtomicUsize>,
        ) {
            live.fetch_add(1, Ordering::SeqCst);
            let node = Box::into_raw(Box::new(Chain {
                retired: RetiredNode::new(),
                left,
                tid,
                active: Arc::clone(active),
                live: Arc::clone(live),
            }));
            unsafe { retire(node) };
        }
    }
    impl Drop for Chain {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
            if !slot_inactive(self.tid) {
                self.active.fetch_add(1, Ordering::SeqCst);
            }
            if self.left > 0 {
                Chain::retire_link(self.left - 1, self.tid, &self.active, &self.live);
                Counted::retire_oldest(&self.live);
            }
        }
    }

    const GENERATIONS: usize = 64;
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let (l, a) = (Arc::clone(&live), Arc::clone(&active));
    thread::spawn(move || {
        let tid = own_tid();
        Chain::retire_link(GENERATIONS, tid, &a, &l);
    })
    .join()
    .unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the exit left values of the chain unfreed"
    );
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "the exiting thread's slot was active while its destructors ran"
    );
}

/// A thread that advances the epoch stamps its next allocations with the
/// epoch it advanced to, pinning or not: its births stay recent, so the
/// batches they end up in skip the slots of threads stalled at older
/// epochs.
#[test]
fn an_epoch_advance_refreshes_the_birth_stamp() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        let seeded = RetiredNode::new().birth_epoch();
        // Other threads' advances, then this thread's own at its
        // EPOCH_FREQ-th retire, none of them followed by a pin here.
        for _ in 0..4 {
            crate::slot::advance_epoch();
        }
        with_handle(|h| h.alloc_counter.set(0));
        for _ in 0..EPOCH_FREQ {
            Counted::retire_one(&l);
        }
        let stamped = RetiredNode::new().birth_epoch();
        assert!(
            stamped > seeded + 4,
            "births still stamped {stamped}, seeded {seeded}"
        );
        assert!(stamped <= crate::slot::epoch());
        flush();
    })
    .join()
    .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// A thread that pinned, dropped its guard and idles, with no flush, holds
/// nothing back: its slot is inactive, so batches retired afterwards by
/// other threads, even of values born before its last reservation, skip
/// its slot and are freed while it stays idle.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn an_idle_thread_holds_nothing_back() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let (idle, done) = (Hold::new(), Hold::new());
    let idler = {
        let (idle, done) = (idle.clone(), done.clone());
        thread::spawn(move || {
            let tid = own_tid();
            drop(pin());
            assert!(slot_inactive(tid), "the drop left the slot active");
            idle.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| done.go.load(Ordering::SeqCst)));
        })
    };
    idle.reached("the idle thread");
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        for _ in 0..4 * RETIRE_FREQ {
            Counted::retire_oldest(&l);
        }
        flush();
    })
    .join()
    .unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "values waited for a thread that went idle without a guard"
    );
    done.release();
    idler.join().unwrap();
}

/// A flush places its partial batch whatever other threads reserve: with
/// one node and one eligible slot (a pinned thread's), the batch gets one
/// filler, the slot takes it, and the batch waits for that slot only. The
/// pinned thread's drop traverses it and frees the value, with no flush
/// anywhere else.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_flush_pads_a_partial_batch_while_a_reader_is_pinned() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let (pinned, reader) = pinned_thread();
    let l = Arc::clone(&live);
    let fillers = thread::spawn(move || {
        own_tid();
        let before = with_handle(|h| h.fillers.get());
        Counted::retire_oldest(&l);
        flush();
        assert!(
            with_handle(|h| h.batch_count.get()) == 0,
            "the flush kept its batch"
        );
        with_handle(|h| h.fillers.get()) - before
    })
    .join()
    .unwrap();
    assert_eq!(fillers, 1, "the flush padded with {fillers} fillers");
    assert_eq!(
        live.load(Ordering::SeqCst),
        1,
        "the value was freed under the reader"
    );
    pinned.release();
    reader.join().unwrap();
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 0),
        "the reader's drop did not free the batch its slot took"
    );
}

/// The same at a thread's exit: the exiting thread's partial batch is
/// placed, padded, and freed by the pinned thread's drop.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn an_exit_places_its_partial_batch_while_a_reader_is_pinned() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let (pinned, reader) = pinned_thread();
    let l = Arc::clone(&live);
    let exited = thread::spawn(move || {
        let tid = own_tid();
        for _ in 0..3 {
            Counted::retire_oldest(&l);
        }
        tid
    })
    .join()
    .unwrap();
    assert!(
        crate::slot::global().tid_is_released(exited),
        "the exit did not release its tid"
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        3,
        "values were freed under the reader"
    );
    pinned.release();
    reader.join().unwrap();
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 0),
        "the reader's drop did not free the batch the exit placed in its slot"
    );
}

/// The drop of a nested guard leaves the slot active: the outer section's
/// loads stay protected until the outer guard drops.
#[test]
fn a_nested_drop_leaves_the_slot_active() {
    let _l = lock();
    thread::spawn(|| {
        let tid = own_tid();
        assert!(slot_inactive(tid));
        let outer = pin();
        assert!(
            !slot_inactive(tid),
            "the outermost pin did not enter the slot"
        );
        let inner = pin();
        drop(inner);
        assert!(!slot_inactive(tid), "a nested drop left the slot");
        assert_eq!(with_handle(|h| h.pin_count.get()), 1);
        drop(outer);
        assert!(
            slot_inactive(tid),
            "the outermost drop left the slot active"
        );
    })
    .join()
    .unwrap();
}

/// Thread B has never pinned and has retired nothing: its `flush()` has
/// nothing to submit, so it must not claim a thread ID, which it would hold,
/// for nothing, until it exits. The test checks that B still has no ID
/// after two flushes, and that no ID was taken (none handed out anew, no
/// released one taken back).
#[test]
fn flush_without_a_tid_or_a_batch_claims_none() {
    let _l = lock();
    let global = crate::slot::global();
    let handed_out = global.max_threads();
    let released: Vec<bool> = (0..handed_out).map(|t| global.tid_is_released(t)).collect();
    thread::spawn(|| {
        flush();
        flush();
        assert_eq!(
            with_handle(|h| h.tid.get()),
            None,
            "a flush with nothing to submit claimed a tid"
        );
    })
    .join()
    .unwrap();
    assert_eq!(
        global.max_threads(),
        handed_out,
        "a flush with nothing to submit took a fresh tid"
    );
    assert!(
        (0..handed_out).all(|t| global.tid_is_released(t) == released[t]),
        "a flush with nothing to submit took a released tid"
    );
}

mod era;
mod late;
mod progress;

/// A thread-local destructor that runs after the thread's exit retires a
/// value: one whose value the thread set before its first pin, so that its
/// destructor runs after the handle's, when neither the handle nor the
/// exiting handle takes a retire. The value goes through a handle of its
/// own, whose exit places it, and it is freed.
///
/// Before the fix the value was dropped unfreed, and this test failed.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_retire_after_the_exit_is_freed() {
    struct Late {
        live: Arc<AtomicUsize>,
        after_exit: Arc<AtomicBool>,
    }
    impl Drop for Late {
        fn drop(&mut self) {
            self.after_exit.store(exit_over(), Ordering::SeqCst);
            Counted::retire_one(&self.live);
        }
    }
    std::thread_local! {
        static LATE: core::cell::RefCell<Option<Late>> = const { core::cell::RefCell::new(None) };
    }
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let after_exit = Arc::new(AtomicBool::new(false));
    let (l, a) = (Arc::clone(&live), Arc::clone(&after_exit));
    thread::spawn(move || {
        LATE.with(|late| {
            *late.borrow_mut() = Some(Late {
                live: l,
                after_exit: a,
            })
        });
        drop(pin());
    })
    .join()
    .unwrap();
    assert!(
        after_exit.load(Ordering::SeqCst),
        "the destructor ran before the thread's exit was over: the case is not reached"
    );
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 0),
        "a value retired after its thread's exit was never freed"
    );
}

/// A structure's drop that retired nothing leaves the global epoch where it
/// was: `flush_pending` submits a batch only when this thread holds one, so
/// it does not send every other thread's next pin down its slow path. Each
/// drop ran `flush`, which always advanced the epoch. Other tests of this
/// binary may advance it meanwhile, so one quiet attempt in many is enough;
/// the old drop advanced it every time.
#[test]
fn a_drop_that_retired_nothing_leaves_the_epoch() {
    let _l = lock();
    // A thread with no ID (it never pinned) skips the whole of a flush, the
    // epoch's advance too: this one has an ID, as a thread that used kovan has.
    own_tid();
    flush();
    // With `recycle`, nodes come from the slab: a value this size puts them
    // in the 192-byte class, which no slab or magazine test reads the
    // live-chunk count of.
    type Value = [u8; 140];
    let quiet = (0..1_000).any(|_| {
        let before = crate::slot::epoch();
        drop(crate::Atom::new([1u8; 140] as Value));
        drop(crate::AtomOption::some([2u8; 140] as Value));
        crate::slot::epoch() == before
    });
    assert!(quiet, "a drop that retired nothing advanced the epoch");
    // A drop whose thread retired something still submits it at once.
    let atom = crate::Atom::new([3u8; 140] as Value);
    atom.store([4u8; 140]);
    assert!(with_handle(|h| h.batch_count.get()) > 0);
    drop(atom);
    assert_eq!(with_handle(|h| h.batch_count.get()), 0);
}
