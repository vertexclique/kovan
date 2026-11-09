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

use super::{Handle, flush, pin, retire, with_handle};
use crate::retired::{INVPTR, RetiredNode};
use crate::slot::{EPOCH_FREQ, ERA_INACTIVE, RETIRE_FREQ};
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

/// Whether this thread's exit has run and is over: the handle takes no
/// retire, which goes through a handle of its own.
fn exit_over() -> bool {
    with_handle(Handle::exit_over)
}

/// The epoch this thread's reservation slot publishes.
fn published_epoch() -> u64 {
    with_handle(|h| {
        h.global()
            .thread_slots(h.tid())
            .epoch
            .load(Ordering::Acquire)
    })
}

/// Whether the reservation slot of `tid` is inactive (left).
fn slot_inactive(tid: usize) -> bool {
    crate::slot::global()
        .thread_slots(tid)
        .first
        .load(Ordering::Acquire)
        == INVPTR
}

/// This thread's ID, allocated by a first pin if it has none.
fn own_tid() -> usize {
    drop(pin());
    with_handle(|h| h.tid())
}

/// Spend this thread's first allocation, which advances the epoch (the
/// epoch clock counts allocations), on a counted value it retires and
/// submits: a test that schedules each epoch move counts the allocations
/// after this one.
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

/// Publish the current epoch on this thread's slot, as the first protected
/// load of a section does: a section that loaded nothing reserves nothing
/// (its era word is the last published, 0 at first), so a test whose reader
/// must hold a batch back makes it load first.
fn reserve(guard: &crate::Guard) {
    let cell = crate::Atomic::<Counted>::null();
    let _ = cell.load(Ordering::Acquire, guard);
}

/// A thread that pins, loads (publishing the current epoch) and stays
/// pinned until the test lets it go: its slot is active and eligible for
/// every batch of oldest-born values.
fn pinned_thread() -> (Hold, thread::JoinHandle<()>) {
    let hold = Hold::new();
    let h = hold.clone();
    let t = thread::spawn(move || {
        let guard = pin();
        reserve(&guard);
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
        // The leave of the escalated section reset the era to 0 (tagged,
        // the slot left); the next section's first protected load publishes
        // a real epoch.
        assert_eq!(published_epoch(), ERA_INACTIVE);
        assert_eq!(with_handle(|h| h.cached_epoch.get()), 0);
        let guard = pin();
        assert_eq!(published_epoch(), 0);
        reserve(&guard);
        assert_eq!(published_epoch(), crate::slot::epoch());
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
            let published = crate::slot::global()
                .thread_slots(self.tid)
                .epoch
                .load(Ordering::Acquire);
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

/// A node allocated after the epoch moved carries the epoch it finds: the
/// birth is read from the global counter at the allocation, whatever this
/// thread did between (other threads' advances, no pin here), and a
/// thread's own `EPOCH_FREQ`th allocation advances the epoch itself before
/// the read.
#[test]
fn an_epoch_advance_refreshes_the_birth_stamp() {
    let _l = lock();
    thread::spawn(|| {
        own_tid();
        let first = RetiredNode::new().birth_epoch();
        assert_eq!(first, crate::slot::epoch());
        for _ in 0..4 {
            crate::slot::advance_epoch();
        }
        let after = RetiredNode::new().birth_epoch();
        assert_eq!(after, first + 4, "a birth stamped behind the epoch");
        // The thread's next EPOCH_FREQ-th allocation advances the epoch and
        // stamps the epoch it made.
        with_handle(|h| h.alloc_counter.set(EPOCH_FREQ));
        let before = crate::slot::epoch();
        let advanced = RetiredNode::new().birth_epoch();
        assert_eq!(advanced, before + 1);
        assert_eq!(crate::slot::epoch(), before + 1);
    })
    .join()
    .unwrap();
}

/// A thread that pinned, dropped its guard and idles, with no flush, holds
/// nothing back: its slot is inactive, so batches retired afterwards by
/// other threads, even of values born before its last reservation, skip
/// its slot and are freed while it stays idle.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
/// after two flushes and that its handle claimed none meanwhile (a claim
/// let go again would leave the handle without an ID too). The handle's
/// own count, not the global table's released bits: the threads of the
/// tests before this one release their IDs as they exit, after their
/// test's lock is gone, so a released bit may flip under any test.
#[test]
fn flush_without_a_tid_or_a_batch_claims_none() {
    let _l = lock();
    thread::spawn(|| {
        flush();
        flush();
        assert_eq!(
            with_handle(|h| h.tid.get()),
            None,
            "a flush with nothing to submit claimed a tid"
        );
        assert_eq!(
            with_handle(|h| h.claims.get()),
            0,
            "a flush with nothing to submit claimed a tid and let it go"
        );
    })
    .join()
    .unwrap();
}

/// Two retires that scanned a reader's slot active exchange their nodes
/// into it after the reader's drop left it: the first is held before its
/// rollback, the second lands on its node. The second reads the tag the
/// leave stored and takes both nodes out (`InsertUndo`), so the left slot
/// holds no list while the reader stays parked and both batches are
/// freed; the first's rollback fails and counts its node, which the
/// second traversed. Without the undo the slot kept the two nodes, read as
/// active, until the reader's next pin, and neither batch was freed.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn two_retires_racing_a_leave_leave_the_slot_empty() {
    /// A thread that claims its ID, reports it and waits to be let go.
    fn retirer(
        live: &Arc<AtomicUsize>,
        value: usize,
    ) -> (Arc<AtomicUsize>, Hold, thread::JoinHandle<()>) {
        let tid = Arc::new(AtomicUsize::new(usize::MAX));
        let hold = Hold::new();
        let (t, h) = (Arc::clone(&tid), hold.clone());
        let live = Arc::clone(live);
        let thread = thread::spawn(move || {
            t.store(own_tid(), Ordering::SeqCst);
            h.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| h.go.load(Ordering::SeqCst)));
            // Born before the reader's reservation, so its batch needs the
            // reader's slot; placed padded by the flush.
            unsafe { retire(value as *mut Counted) };
            flush();
            drop(live);
        });
        hold.reached("the retirer");
        (tid, hold, thread)
    }

    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let values: Vec<usize> = (0..2)
        .map(|_| {
            live.fetch_add(1, Ordering::SeqCst);
            Box::into_raw(Box::new(Counted {
                retired: RetiredNode::new(),
                live: Arc::clone(&live),
            })) as usize
        })
        .collect();
    // The reader: pins and loads (its slot active, its era current), drops
    // its guard when told to, then parks until the end.
    let reader_tid = Arc::new(AtomicUsize::new(usize::MAX));
    let (to_drop, parked) = (Hold::new(), Hold::new());
    let left = Arc::new(AtomicBool::new(false));
    let reader = {
        let (tid, to_drop, parked, left) = (
            Arc::clone(&reader_tid),
            to_drop.clone(),
            parked.clone(),
            Arc::clone(&left),
        );
        thread::spawn(move || {
            let guard = pin();
            reserve(&guard);
            tid.store(with_handle(|h| h.tid()), Ordering::SeqCst);
            to_drop.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| to_drop.go.load(Ordering::SeqCst)));
            drop(guard);
            left.store(true, Ordering::SeqCst);
            assert!(eventually(|| parked.go.load(Ordering::SeqCst)));
        })
    };
    to_drop.reached("the reader");
    let reader_tid = reader_tid.load(Ordering::SeqCst);

    // Both retirers scan the reader's slot active and stop before their
    // exchange.
    let (tid_a, go_a, a) = retirer(&live, values[0]);
    let (tid_b, go_b, b) = retirer(&live, values[1]);
    let (tid_a, tid_b) = (tid_a.load(Ordering::SeqCst), tid_b.load(Ordering::SeqCst));
    let (ready_a, ready_b) = (Hold::new(), Hold::new());
    ready_a.arm(Step::InsertReady, tid_a);
    ready_b.arm(Step::InsertReady, tid_b);
    go_a.release();
    go_b.release();
    ready_a.reached("retirer A");
    ready_b.reached("retirer B");

    // The reader leaves; A exchanges its node in (INVPTR behind it) and is
    // held before its rollback; B lands on A's node and completes.
    to_drop.release();
    assert!(eventually(|| left.load(Ordering::SeqCst)));
    let exchanged_a = Hold::new();
    exchanged_a.arm(Step::InsertExchanged, tid_a);
    ready_a.release();
    exchanged_a.reached("retirer A's exchange");
    ready_b.release();
    join_within(b, "retirer B");
    exchanged_a.release();
    join_within(a, "retirer A");

    assert!(
        slot_inactive(reader_tid),
        "the left slot of a parked reader holds the nodes of two retires"
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the batches of two retires that raced a leave wait for the parked reader"
    );
    parked.release();
    reader.join().unwrap();
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
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
