//! Progress: each operation kovan runs completes in a bounded number of
//! its own steps while other threads are held at chosen points of theirs
//! (see `stall`): inside a traversal, mid insert, in a slow path, mid
//! hand-over, mid exit, mid flush. Where a loop's pass count is the bound,
//! a hook at that loop's step counts it.

use super::{
    Counted, Hold, eventually, join_within, lock, own_tid, slot_inactive, spend_first_retire,
    with_handle,
};
use crate::guard::{flush, pin, retire};
use crate::reclaim::MAX_CACHE;
use crate::retired::{INVPTR, RetiredNode};
use crate::slot::{EPOCH_UNCONDITIONAL, RETIRE_FREQ};
use crate::stall::{self, Step};
use std::boxed::Box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

/// A thread stalled for good in its slow path: an active slot publishing
/// `epoch`, its help request open. `finish` resumes it as the end of its
/// slow path would (both seqnos past the transition, no request open) and
/// then leaves as an exit does, on the calling thread (which needs an ID).
struct FakePending {
    tid: usize,
    seqno: u64,
}

impl FakePending {
    fn open(epoch: u64) -> Self {
        let global = crate::slot::global();
        let tid = global.alloc_tid();
        let slots = global.thread_slots(tid);
        let seqno = slots.epoch[0].load_hi();
        slots.first[0].store_hi(seqno, Ordering::SeqCst);
        slots.epoch[0].store_lo(epoch, Ordering::SeqCst);
        slots.first[0].store_lo(0, Ordering::SeqCst);
        slots.state[0].pointer.store(0, Ordering::SeqCst);
        slots.state[0].parent.store(0, Ordering::SeqCst);
        slots.state[0].epoch.store(0, Ordering::SeqCst);
        slots.state[0]
            .result
            .store(INVPTR as u64, seqno, Ordering::SeqCst);
        global.inc_slow();
        Self { tid, seqno }
    }

    fn finish(self) {
        let global = crate::slot::global();
        let slots = global.thread_slots(self.tid);
        slots.state[0].result.store(0, 0, Ordering::SeqCst);
        slots.epoch[0].store_hi(self.seqno + 2, Ordering::SeqCst);
        slots.first[0].store_hi(self.seqno + 2, Ordering::SeqCst);
        global.dec_slow();
        // The leave an exit makes: the list word exchanged to INVPTR, what it
        // held traversed.
        let first = slots.first[0].exchange_lo(INVPTR as u64, Ordering::AcqRel);
        if first != 0 && first != INVPTR as u64 {
            with_handle(|h| unsafe { h.traverse_into_cache(first as *mut RetiredNode) });
        }
        global.release_tid(self.tid);
    }
}

/// Whether the head of `tid`'s slot list is an entry whose insert has not
/// yet linked the list it displaced (a retire held mid insert there).
fn head_is_unlinked(tid: usize) -> bool {
    let head = crate::slot::global().thread_slots(tid).first[0].load_lo() as *mut RetiredNode;
    !head.is_null()
        && head as usize != INVPTR
        && unsafe { (*head).next.load(Ordering::Acquire) }.is_null()
}

/// On a fresh thread with an ID: flush until every counted value is
/// freed, failing the test if they are not by `DEADLINE`.
pub(super) fn drain(live: &Arc<AtomicUsize>) {
    let l = Arc::clone(live);
    join_within(
        thread::spawn(move || {
            own_tid();
            let freed = eventually(|| {
                flush();
                l.load(Ordering::SeqCst) == 0
            });
            assert!(freed, "{} values never freed", l.load(Ordering::SeqCst));
        }),
        "the drain",
    );
}

/// Handing thread IDs over waits for no other thread: with one thread held
/// inside its exit, between leaving its slot and placing its batch, threads
/// keep starting (a first pin claims an ID), retiring, flushing and exiting
/// (placing their batches, releasing their IDs). The held thread's ID goes
/// to no one before it is released, and once it is let go every retired
/// value is freed exactly once.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn ids_change_hands_while_a_thread_stalls_in_its_exit() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));

    // Pinned throughout: its slot is eligible for every batch holding an
    // oldest-born value, so every placement below pads and waits for it.
    let (pinned_up, pinned_done) = (Hold::new(), Hold::new());
    let pinned = {
        let (up, done) = (pinned_up.clone(), pinned_done.clone());
        thread::spawn(move || {
            let guard = pin();
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| done.go.load(Ordering::SeqCst)));
            drop(guard);
        })
    };
    pinned_up.reached("the pinned thread");

    // Held at exit, after leaving its slot and before placing its batch.
    let leaving = Hold::new();
    let held_tid = Arc::new(AtomicUsize::new(usize::MAX));
    let exiting = {
        let (live, leaving, held_tid) = (Arc::clone(&live), leaving.clone(), Arc::clone(&held_tid));
        thread::spawn(move || {
            let tid = own_tid();
            held_tid.store(tid, Ordering::SeqCst);
            Counted::retire_oldest(&live);
            leaving.arm(Step::Left, tid);
        })
    };
    leaving.reached("the exiting thread");
    let held_tid = held_tid.load(Ordering::SeqCst);
    assert!(
        slot_inactive(held_tid),
        "the exit is held after leaving its slot"
    );

    // Threads that start (a first pin claims an ID), retire and exit while
    // it is held, or flush before they exit.
    let churn = |n: usize, flushing: bool| {
        for _ in 0..n {
            let live = Arc::clone(&live);
            let tid = join_within(
                thread::spawn(move || {
                    let tid = own_tid();
                    Counted::retire_oldest(&live);
                    if flushing {
                        flush();
                    }
                    tid
                }),
                "a thread starting, retiring, flushing and exiting",
            );
            assert_ne!(tid, held_tid, "an exiting thread's ID was taken over");
        }
    };
    churn(8, false);
    churn(8, true);

    // Still held: the exit has not released its ID (its thread's main
    // function has returned, the exit runs after it).
    assert!(!crate::slot::global().tid_is_released(held_tid));
    leaving.release();
    join_within(exiting, "the held exit");
    assert!(crate::slot::global().tid_is_released(held_tid));
    pinned_done.release();
    join_within(pinned, "the pinned thread");

    // Every placed batch is freed by the pinned thread's drop.
    drain(&live);
}

/// A pin's slow path runs no destructor before it republishes its slot. A
/// helper that completed the pending thread's request holds the slot in
/// its hand-over (the epoch seqno odd: new batches skip the slot) and is
/// held there; the pending thread, let go, finishes its slow path without
/// it, and the destructors of what it frees (its free-list cache is full,
/// and its loop traverses a list) run only once its slot is live again.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn slow_path_frees_nothing_before_it_republishes() {
    /// Records whether its thread's slot was closed to new batches (an odd
    /// epoch or list seqno) when it ran: a load it made then would be
    /// unprotected.
    #[repr(C)]
    struct ChecksOpen {
        retired: RetiredNode,
        tid: usize,
        closed: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for ChecksOpen {
        fn drop(&mut self) {
            let slots = crate::slot::global().thread_slots(self.tid);
            if (slots.epoch[0].load_hi() | slots.first[0].load_hi()) & 1 != 0 {
                self.closed.fetch_add(1, Ordering::SeqCst);
            }
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let _l = lock();
    let closed = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));
    let (pending, handing_over) = (Hold::new(), Hold::new());
    let pending_tid = Arc::new(AtomicUsize::new(usize::MAX));
    let finished = Arc::new(AtomicBool::new(false));

    let helpee = {
        let (closed, dropped, live) =
            (Arc::clone(&closed), Arc::clone(&dropped), Arc::clone(&live));
        let (pending, pending_tid, finished) = (
            pending.clone(),
            Arc::clone(&pending_tid),
            Arc::clone(&finished),
        );
        thread::spawn(move || {
            let tid = own_tid();
            let global = crate::slot::global();
            let guard = pin();
            // One batch of checkers into this thread's slot (the only
            // active one), moved to the free-list cache, full, by a
            // traversal of the slot list.
            for _ in 0..RETIRE_FREQ {
                let node = Box::into_raw(Box::new(ChecksOpen {
                    retired: RetiredNode::new(),
                    tid,
                    closed: Arc::clone(&closed),
                    dropped: Arc::clone(&dropped),
                }));
                unsafe { retire(node) };
            }
            with_handle(|h| {
                let first = global.thread_slots(tid).first[0].exchange_lo(0, Ordering::AcqRel);
                assert!(first != 0 && first != INVPTR as u64);
                unsafe { h.traverse_onto_cache(first as *mut RetiredNode) };
                h.list_count.set(MAX_CACHE);
            });
            // One more batch into the slot, for the slow path's loop to
            // traverse; no epoch advance on the way.
            with_handle(|h| h.alloc_counter.set(1));
            for _ in 0..RETIRE_FREQ {
                Counted::retire_one(&live);
            }
            // Publish the current epoch, as a protected load does, so the
            // helper's empty epoch transition closes the slot.
            let shared = crate::Atomic::<u64>::null();
            let _ = shared.load(Ordering::Acquire, &guard);
            pending_tid.store(tid, Ordering::SeqCst);
            pending.arm(Step::SlowPending, tid);
            // The transition, taken through its slow path, inside the pin.
            with_handle(|h| h.slow_path(0, tid));
            finished.store(true, Ordering::SeqCst);
            drop(guard);
        })
    };
    pending.reached("the pending thread");
    let pending_tid = pending_tid.load(Ordering::SeqCst);

    let helper = {
        let handing_over = handing_over.clone();
        thread::spawn(move || {
            let tid = own_tid();
            handing_over.arm(Step::EraClosed, tid);
            with_handle(|h| h.help_thread(pending_tid, 0, tid));
        })
    };
    handing_over.reached("the helper");
    let seqno = crate::slot::global().thread_slots(pending_tid).epoch[0].load_hi();
    assert_eq!(seqno & 1, 1, "the helper did not close the slot");

    // The pending thread completes while the helper stays held.
    pending.release();
    assert!(
        eventually(|| finished.load(Ordering::SeqCst)),
        "the slow path did not complete while its helper was held"
    );
    handing_over.release();
    join_within(helper, "the helper");
    join_within(helpee, "the pending thread");

    assert_eq!(dropped.load(Ordering::SeqCst), RETIRE_FREQ);
    assert_eq!(
        closed.load(Ordering::SeqCst),
        0,
        "destructors ran while their thread's slot was closed to new batches"
    );
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// Taking a pending thread's slot list over ends however much other
/// threads retire meanwhile. The pending thread is stalled in its slow path
/// with an old epoch published, so a helper's empty epoch transition does
/// not close its slot; each time the helper has read the slot and is about
/// to empty it, another thread retires a batch of oldest-born values,
/// eligible for the pending slot while it takes new batches. The take must
/// close the slot first and then need one pass.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn list_hand_over_ends_while_retires_keep_coming() {
    /// Batches the retiring thread submits at most: one before the
    /// hand-over, then one per pass.
    const RETIRES: usize = 9;

    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let global = crate::slot::global();

    // Places one batch of oldest-born values each time it is asked to, with
    // no advance after it, once its first retire is spent: an advance helps
    // the pending thread below, and one before the helper's would hand its
    // list over first.
    let (asked, answered) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let started = Hold::new();
    let retirer = {
        let (asked, answered, live, started) = (
            Arc::clone(&asked),
            Arc::clone(&answered),
            Arc::clone(&live),
            started.clone(),
        );
        thread::spawn(move || {
            own_tid();
            spend_first_retire(&live);
            started.arrived.store(true, Ordering::SeqCst);
            for n in 1..=RETIRES {
                assert!(eventually(|| asked.load(Ordering::SeqCst) >= n));
                Counted::place_oldest(&live);
                answered.store(n, Ordering::SeqCst);
            }
        })
    };
    started.reached("the retiring thread");

    // A thread stalled in its slow path, publishing an epoch older than the
    // global one.
    crate::slot::advance_epoch();
    let pending = FakePending::open(crate::slot::epoch() - 1);
    let (pending_tid, slots) = (pending.tid, global.thread_slots(pending.tid));

    // A first batch, placed in the pending slot among others: the hand-over
    // has a list to take.
    asked.store(1, Ordering::SeqCst);
    assert!(eventually(|| answered.load(Ordering::SeqCst) >= 1));
    assert_ne!(
        slots.first[0].load_lo(),
        0,
        "the pending slot took no batch"
    );

    let passes = Arc::new(AtomicUsize::new(0));
    let helper = {
        let (asked, answered, passes) = (
            Arc::clone(&asked),
            Arc::clone(&answered),
            Arc::clone(&passes),
        );
        thread::spawn(move || {
            let tid = own_tid();
            stall::arm(Step::DetachList, tid, move || {
                let n = passes.fetch_add(1, Ordering::SeqCst) + 2;
                if n > RETIRES {
                    return false;
                }
                // Another thread's retire, between this read of the slot
                // and the attempt to empty it.
                asked.store(n, Ordering::SeqCst);
                assert!(eventually(|| answered.load(Ordering::SeqCst) >= n));
                true
            });
            with_handle(|h| h.help_thread(pending_tid, 0, tid));
        })
    };
    join_within(helper, "the helper");
    let passes = passes.load(Ordering::SeqCst);
    assert_eq!(
        passes, 1,
        "the hand-over took {passes} passes while another thread kept retiring"
    );

    // Let the retirer finish, then leave the pending thread's slot as an
    // exit would and free everything.
    asked.store(RETIRES, Ordering::SeqCst);
    join_within(retirer, "the retiring thread");
    join_within(
        thread::spawn(move || {
            own_tid();
            pending.finish();
        }),
        "the pending thread's exit",
    );
    drain(&live);
}

/// A protected load ends within its attempts whatever the epoch does:
/// another thread advancing the epoch between every pointer load and the
/// epoch read that should confirm it costs it `MAX_LOAD_ATTEMPTS - 1`
/// publications and one escalation, then one more load. The escalated
/// section ends with its guard's drop, which leaves the slot.
#[test]
fn protected_load_ends_within_its_attempts() {
    let _l = lock();
    join_within(
        thread::spawn(|| {
            let tid = own_tid();
            let passes = Arc::new(AtomicUsize::new(0));
            let p = Arc::clone(&passes);
            stall::arm(Step::LoadAttempt, tid, move || {
                p.fetch_add(1, Ordering::SeqCst);
                crate::slot::advance_epoch();
                true
            });
            let value = Box::into_raw(Box::new(7u64));
            let shared = crate::Atomic::new(value);
            let guard = pin();
            crate::slot::advance_epoch();
            let loaded = shared.load(Ordering::Acquire, &guard);
            stall::disarm(Step::LoadAttempt, tid);
            assert_eq!(loaded.as_raw(), value);
            assert_eq!(
                passes.load(Ordering::SeqCst),
                crate::guard::MAX_LOAD_ATTEMPTS - 1
            );
            assert_eq!(with_handle(|h| h.cached_epoch.get()), EPOCH_UNCONDITIONAL);
            drop(guard);
            assert!(
                slot_inactive(tid),
                "the escalated section's drop left the slot active"
            );
            drop(unsafe { Box::from_raw(value) });
        }),
        "the loading thread",
    );
}

/// Runs `help_read` and then advances the epoch each time it is asked to,
/// as every epoch advance does: a thread that helps before it advances.
struct Advancer {
    asked: Arc<AtomicUsize>,
    answered: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

impl Advancer {
    fn start() -> Self {
        let (asked, answered) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (asked, answered, stop) =
                (Arc::clone(&asked), Arc::clone(&answered), Arc::clone(&stop));
            thread::spawn(move || {
                let tid = own_tid();
                let mut done = 0;
                while eventually(|| {
                    stop.load(Ordering::SeqCst) || asked.load(Ordering::SeqCst) > done
                }) && !stop.load(Ordering::SeqCst)
                {
                    with_handle(|h| h.help_read(tid));
                    crate::slot::advance_epoch();
                    done += 1;
                    answered.store(done, Ordering::SeqCst);
                }
            })
        };
        Self {
            asked,
            answered,
            stop,
            thread,
        }
    }

    /// Help and advance once, on the advancer's thread; wait until done.
    fn help_and_advance(asked: &AtomicUsize, answered: &AtomicUsize) {
        let n = asked.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(eventually(|| answered.load(Ordering::SeqCst) >= n));
    }

    fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        join_within(self.thread, "the advancing thread");
    }
}

/// An outermost pin ends while the epoch moves at every attempt: its
/// transition makes 16 attempts, then its slow path passes once per epoch
/// advance made by a thread that did not see its help request (two here)
/// and ends at the first advance whose thread helped first, which
/// completes the request for it.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn pin_ends_while_the_epoch_moves_at_every_attempt() {
    const UNHELPED: usize = 2;
    let _l = lock();
    let advancer = Advancer::start();
    let (asked, answered) = (Arc::clone(&advancer.asked), Arc::clone(&advancer.answered));
    let (attempts, passes) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (a, p) = (Arc::clone(&attempts), Arc::clone(&passes));
    join_within(
        thread::spawn(move || {
            let tid = own_tid();
            stall::arm(Step::TransitionAttempt, tid, move || {
                a.fetch_add(1, Ordering::SeqCst);
                crate::slot::advance_epoch();
                true
            });
            stall::arm(Step::SlowPass, tid, move || {
                let n = p.fetch_add(1, Ordering::SeqCst) + 1;
                if n <= UNHELPED {
                    crate::slot::advance_epoch();
                } else {
                    Advancer::help_and_advance(&asked, &answered);
                }
                true
            });
            drop(pin());
            stall::disarm(Step::TransitionAttempt, tid);
            stall::disarm(Step::SlowPass, tid);
        }),
        "the pinning thread",
    );
    advancer.stop();
    assert_eq!(attempts.load(Ordering::SeqCst), 16);
    assert_eq!(passes.load(Ordering::SeqCst), UNHELPED + 1);
}

/// A retire's insert held between exchanging its node into another
/// thread's slot and linking the list it displaced keeps no one waiting:
/// the slot's owner leaves (its traversal ends at the unlinked node) and
/// pins again, and a third thread retires into the same slot. Let go, the
/// insert finds the list taken and walks the rest itself; everything is
/// freed once.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn pin_and_retire_end_while_an_insert_is_held() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));

    // The slot owner, with a lower ID than the inserter: the inserter's
    // scan reaches its slot first. Pinned, one batch already in its slot.
    let (owner_up, owner_go) = (Hold::new(), Hold::new());
    let owner_pinned = Arc::new(AtomicBool::new(false));
    let owner_tid = Arc::new(AtomicUsize::new(0));
    let owner = {
        let (up, go, live, pinned, owner_tid) = (
            owner_up.clone(),
            owner_go.clone(),
            Arc::clone(&live),
            Arc::clone(&owner_pinned),
            Arc::clone(&owner_tid),
        );
        thread::spawn(move || {
            owner_tid.store(own_tid(), Ordering::SeqCst);
            let guard = pin();
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.go.load(Ordering::SeqCst)));
            // The drop takes the list whose head is the held insert and ends
            // its traversal there; the next pin enters again.
            drop(guard);
            let guard = pin();
            pinned.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.arrived.load(Ordering::SeqCst)));
            drop(guard);
        })
    };
    owner_up.reached("the slot owner");

    let inserting = Hold::new();
    let inserter = {
        let (inserting, live) = (inserting.clone(), Arc::clone(&live));
        thread::spawn(move || {
            let tid = own_tid();
            inserting.arm(Step::InsertExchanged, tid);
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    inserting.reached("the inserting thread");
    assert!(head_is_unlinked(owner_tid.load(Ordering::SeqCst)));

    // While it is held: the owner leaves and pins again, a third thread
    // retires into the owner's slot.
    owner_go.release();
    assert!(
        eventually(|| owner_pinned.load(Ordering::SeqCst)),
        "the slot owner's drop and pin did not complete while an insert into its slot was held"
    );
    let third = {
        let live = Arc::clone(&live);
        thread::spawn(move || {
            own_tid();
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    join_within(third, "a retire into the same slot");

    inserting.release();
    join_within(inserter, "the held retire");
    owner_go.arrived.store(true, Ordering::SeqCst);
    join_within(owner, "the slot owner");
    drain(&live);
}

/// A left slot takes nothing: the outermost drop's exchange captures the
/// list, and while its traversal is held another thread retires batch after
/// batch of oldest-born values, which skip the inactive slot and, with no
/// other slot active, are freed at once. Let go, the traversal frees
/// exactly the batches it captured.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_left_slot_takes_nothing_while_its_traversal_is_held() {
    const CAPTURED: usize = 4;
    const RETIRED: usize = 8;
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let traversing = Hold::new();
    let (traversed, owner_tid) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    let owner = {
        let (traversing, traversed, owner_tid, live) = (
            traversing.clone(),
            Arc::clone(&traversed),
            Arc::clone(&owner_tid),
            Arc::clone(&live),
        );
        thread::spawn(move || {
            let tid = own_tid();
            owner_tid.store(tid, Ordering::SeqCst);
            let guard = pin();
            // Batches whose one slot is this thread's (the only one active;
            // oldest-born values, so its epoch covers them however the epoch
            // moves meanwhile): the list the drop below captures.
            for _ in 0..CAPTURED * RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
            traversing.arm(Step::Traverse, tid);
            drop(guard);
            traversed.store(true, Ordering::SeqCst);
        })
    };
    traversing.reached("the traversing thread");
    let owner_tid = owner_tid.load(Ordering::SeqCst);
    assert!(
        slot_inactive(owner_tid),
        "the slot is held active past its leave"
    );
    assert_eq!(live.load(Ordering::SeqCst), CAPTURED * RETIRE_FREQ);

    // Another thread retires batches of oldest-born values: none lands in
    // the left slot, and with no slot active each is freed at once.
    let retirer = {
        let live = Arc::clone(&live);
        thread::spawn(move || {
            own_tid();
            for _ in 0..RETIRED * RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    join_within(retirer, "the retiring thread");
    assert_eq!(
        live.load(Ordering::SeqCst),
        CAPTURED * RETIRE_FREQ,
        "batches retired while the slot was left waited for it"
    );
    traversing.release();
    assert!(eventually(|| traversed.load(Ordering::SeqCst)));
    join_within(owner, "the traversing thread");
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the traversal did not free the batches it captured"
    );
}

/// The drop that ends an escalated section is one leave, and it waits for
/// no one: a thread stalled in its slow path for good and a retire held mid
/// insert into the escalated thread's slot do not delay it.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn the_drop_of_an_escalated_section_ends_while_others_are_held() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));

    // The escalating thread, with the lowest ID: the insert lands in its
    // slot first.
    let (escalated, go) = (Hold::new(), Hold::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let section_tid = Arc::new(AtomicUsize::new(0));
    let section = {
        let (escalated, go, dropped, section_tid) = (
            escalated.clone(),
            go.clone(),
            Arc::clone(&dropped),
            Arc::clone(&section_tid),
        );
        thread::spawn(move || {
            let tid = own_tid();
            section_tid.store(tid, Ordering::SeqCst);
            let guard = pin();
            with_handle(|h| h.escalate());
            escalated.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.go.load(Ordering::SeqCst)));
            drop(guard);
            assert!(
                slot_inactive(tid),
                "the escalated drop left its slot active"
            );
            dropped.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.arrived.load(Ordering::SeqCst)));
        })
    };
    escalated.reached("the escalating thread");
    let pending = FakePending::open(crate::slot::epoch());

    let inserting = Hold::new();
    let inserter = {
        let (inserting, live) = (inserting.clone(), Arc::clone(&live));
        thread::spawn(move || {
            let tid = own_tid();
            inserting.arm(Step::InsertExchanged, tid);
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    inserting.reached("the inserting thread");
    assert!(head_is_unlinked(section_tid.load(Ordering::SeqCst)));

    go.release();
    assert!(
        eventually(|| dropped.load(Ordering::SeqCst)),
        "the escalated drop did not complete while others were held"
    );
    inserting.release();
    join_within(inserter, "the held retire");
    go.arrived.store(true, Ordering::SeqCst);
    join_within(section, "the escalating thread");
    join_within(
        thread::spawn(move || {
            own_tid();
            pending.finish();
        }),
        "the pending thread's exit",
    );
    drain(&live);
}

/// flush() waits for no one either: with a thread stalled in its slow path
/// for good (flush helps it, completing its request), a traversal held
/// after its capture and a retire held mid insert, a flushing thread
/// places its batch, helps, advances and frees, and returns.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn flush_ends_while_others_are_held() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let pending = FakePending::open(crate::slot::epoch());
    let pending_tid = pending.tid;

    let traversing = Hold::new();
    let traverser = {
        let (traversing, live) = (traversing.clone(), Arc::clone(&live));
        thread::spawn(move || {
            let tid = own_tid();
            let guard = pin();
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
            traversing.arm(Step::Traverse, tid);
            drop(guard);
        })
    };
    traversing.reached("the traversing thread");

    let inserting = Hold::new();
    let inserter = {
        let (inserting, live) = (inserting.clone(), Arc::clone(&live));
        thread::spawn(move || {
            let tid = own_tid();
            inserting.arm(Step::InsertExchanged, tid);
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    inserting.reached("the inserting thread");

    let flusher = {
        let live = Arc::clone(&live);
        thread::spawn(move || {
            own_tid();
            for _ in 0..RETIRE_FREQ / 2 {
                Counted::retire_oldest(&live);
            }
            flush();
        })
    };
    join_within(flusher, "a flush while others are held");
    let result = crate::slot::global().thread_slots(pending_tid).state[0]
        .result
        .load_lo();
    assert_ne!(
        result, INVPTR as u64,
        "the flush did not complete the stalled thread's request"
    );

    traversing.release();
    join_within(traverser, "the held traversal");
    inserting.release();
    join_within(inserter, "the held retire");
    join_within(
        thread::spawn(move || {
            own_tid();
            pending.finish();
        }),
        "the pending thread's exit",
    );
    drain(&live);
}

/// A helper stays with the request it started helping: once that request
/// is answered, even if the pending thread opens its next one at once, the
/// helper's loop ends. Following the thread into its next request would let
/// epoch advances made for that one keep the helper looping.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn help_ends_when_its_request_changes() {
    let _l = lock();
    let mut pending = FakePending::open(crate::slot::epoch());
    let (pending_tid, seqno) = (pending.tid, pending.seqno);
    let passes = Arc::new(AtomicUsize::new(0));
    let p = Arc::clone(&passes);
    join_within(
        thread::spawn(move || {
            let tid = own_tid();
            stall::arm(Step::HelpPass, tid, move || {
                if p.fetch_add(1, Ordering::SeqCst) == 0 {
                    // During the first pass the epoch moves, another helper
                    // answers the request, and the pending thread ends that
                    // cycle and opens its next request.
                    crate::slot::advance_epoch();
                    let slots = crate::slot::global().thread_slots(pending_tid);
                    let result = &slots.state[0].result;
                    result.store(0, crate::slot::epoch(), Ordering::SeqCst);
                    slots.epoch[0].store_hi(seqno + 2, Ordering::SeqCst);
                    slots.first[0].store_hi(seqno + 2, Ordering::SeqCst);
                    result.store(INVPTR as u64, seqno + 2, Ordering::SeqCst);
                }
                true
            });
            with_handle(|h| h.help_thread(pending_tid, 0, tid));
            stall::disarm(Step::HelpPass, tid);
        }),
        "the helper",
    );
    assert_eq!(passes.load(Ordering::SeqCst), 1);
    let result = crate::slot::global().thread_slots(pending_tid).state[0]
        .result
        .load();
    assert_eq!(
        result,
        (INVPTR as u64, seqno + 2),
        "the helper answered a later request than the one it was helping"
    );
    pending.seqno += 2;
    join_within(
        thread::spawn(move || {
            own_tid();
            pending.finish();
        }),
        "the pending thread's exit",
    );
}

/// A helper's loop ends with the cycle of the request it helps, even when
/// its check of that request reads the request's two words across two
/// cycles. The helper reads a thread's first request, `(INVPTR, 0)`. During
/// each of its passes the epoch moves, the thread self-completes its current
/// request and opens its next one, and between the two halves of the
/// helper's check the thread self-completes that one too: the check reads a
/// later request's INVPTR and the 0 its self-completion leaves, the pair
/// the helper started with. The loop still passes at most T + 2 times.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn help_ends_with_its_cycle_across_a_torn_request_read() {
    let _l = lock();
    // The pending thread: an ID whose slot never took a slow path, so its
    // first request has seqno 0. IDs whose slots did are held meanwhile.
    let mut others = std::vec::Vec::new();
    let mut pending = loop {
        let p = FakePending::open(crate::slot::epoch());
        if p.seqno == 0 {
            break p;
        }
        others.push(p);
    };
    let pending_tid = pending.tid;
    // The era's seqno the pending thread's cycles leave, the helper's passes
    // and their bound, T + 2 with T the IDs handed out.
    let era = Arc::new(AtomicUsize::new(0));
    let passes = Arc::new(AtomicUsize::new(0));
    let bound = Arc::new(AtomicUsize::new(0));
    let (e, p) = (Arc::clone(&era), Arc::clone(&passes));
    let (e2, p2, b) = (Arc::clone(&era), Arc::clone(&passes), Arc::clone(&bound));
    join_within(
        thread::spawn(move || {
            let tid = own_tid();
            b.store(crate::slot::global().max_threads() + 2, Ordering::SeqCst);
            // One cycle of the pending thread per pass, for more passes than
            // the bound allows.
            let cycles = b.load(Ordering::SeqCst) + 2;
            let slots = crate::slot::global().thread_slots(pending_tid);
            let result = &slots.state[0].result;
            let inv = INVPTR as u64;
            // The pending thread ends the cycle it is in: a self-completion.
            let self_complete = move |era: &AtomicUsize| {
                let seqno = era.load(Ordering::SeqCst) as u64;
                assert!(result.compare_exchange(inv, seqno, 0, 0).is_ok());
                slots.epoch[0].store_hi(seqno + 2, Ordering::SeqCst);
                slots.first[0].store_hi(seqno + 2, Ordering::SeqCst);
                era.store(seqno as usize + 2, Ordering::SeqCst);
            };
            stall::arm(Step::HelpPass, tid, move || {
                let pass = p.fetch_add(1, Ordering::SeqCst);
                if pass < cycles {
                    // The first request ends; after that, none is open.
                    if pass == 0 {
                        self_complete(&e);
                    }
                    // The pending thread opens its next request, and the
                    // epoch moves during this pass.
                    result.store(inv, e.load(Ordering::SeqCst) as u64, Ordering::SeqCst);
                    crate::slot::advance_epoch();
                }
                true
            });
            stall::arm(Step::HelpRecheck, tid, move || {
                // The request just opened ends between the check's two
                // loads: the low word read was its INVPTR, the high word
                // read is its self-completion's 0.
                if p2.load(Ordering::SeqCst) <= cycles {
                    self_complete(&e2);
                }
                true
            });
            with_handle(|h| h.help_thread(pending_tid, 0, tid));
            stall::disarm(Step::HelpPass, tid);
            stall::disarm(Step::HelpRecheck, tid);
        }),
        "the helper",
    );
    let (passes, bound) = (passes.load(Ordering::SeqCst), bound.load(Ordering::SeqCst));
    assert!(
        passes <= bound,
        "the helper made {passes} passes, more than T + 2 = {bound}: it followed the pending \
         thread through later cycles"
    );
    let slots = crate::slot::global().thread_slots(pending_tid);
    assert_eq!(slots.state[0].result.load().0, 0, "a request was left open");
    pending.seqno = era.load(Ordering::SeqCst) as u64;
    join_within(
        thread::spawn(move || {
            own_tid();
            pending.finish();
            for other in others {
                other.finish();
            }
        }),
        "the pending threads' exits",
    );
}

/// An insert that finds its slot left when it exchanges its node in (the
/// owner dropped its guard and exited after the scan found the slot
/// eligible) takes the node back out with one compare-exchange of the list
/// word, and the slot is inactive again: it holds no node of the batch,
/// which is freed.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn insert_into_a_left_slot_rolls_back() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));

    // The slot owner, with the lower ID, pinned: the insert targets its slot
    // first.
    let (owner_up, owner_exit) = (Hold::new(), Hold::new());
    let owner_tid = Arc::new(AtomicUsize::new(0));
    let owner = {
        let (up, exit, owner_tid) = (owner_up.clone(), owner_exit.clone(), Arc::clone(&owner_tid));
        thread::spawn(move || {
            owner_tid.store(own_tid(), Ordering::SeqCst);
            let guard = pin();
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| exit.go.load(Ordering::SeqCst)));
            drop(guard);
        })
    };
    owner_up.reached("the slot owner");
    let owner_tid = owner_tid.load(Ordering::SeqCst);

    let ready = Hold::new();
    let inserter = {
        let (ready, live) = (ready.clone(), Arc::clone(&live));
        thread::spawn(move || {
            let tid = own_tid();
            ready.arm(Step::InsertReady, tid);
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    ready.reached("the inserting thread");

    // The owner drops its guard and exits while the insert is held after its
    // checks.
    owner_exit.release();
    join_within(owner, "the slot owner's exit");
    assert!(slot_inactive(owner_tid));

    ready.release();
    join_within(inserter, "the insert");
    assert!(
        slot_inactive(owner_tid),
        "the left slot kept the node an insert put into it"
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the batch waited for a slot that holds no node of it"
    );
}

/// The other side of the rollback race: the insert exchanged its node into
/// a left slot and, before its rollback, the owner pins again. The owner's
/// enter takes the node and traverses it; the insert's rollback fails and
/// counts the node as inserted, so the batch's count is exact: it reaches
/// zero with that traversal and the batch is freed once.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn an_insert_a_re_entering_owner_takes_is_counted_once() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));

    let (owner_up, owner_leave, owner_pin) = (Hold::new(), Hold::new(), Hold::new());
    let owner_tid = Arc::new(AtomicUsize::new(0));
    let owner = {
        let (up, leave, pin_again, owner_tid) = (
            owner_up.clone(),
            owner_leave.clone(),
            owner_pin.clone(),
            Arc::clone(&owner_tid),
        );
        thread::spawn(move || {
            owner_tid.store(own_tid(), Ordering::SeqCst);
            let guard = pin();
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| leave.go.load(Ordering::SeqCst)));
            drop(guard);
            leave.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| pin_again.go.load(Ordering::SeqCst)));
            let guard = pin();
            pin_again.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| up.go.load(Ordering::SeqCst)));
            drop(guard);
        })
    };
    owner_up.reached("the slot owner");
    let owner_tid = owner_tid.load(Ordering::SeqCst);

    // The insert, held after its checks found the owner's slot eligible,
    // then held again right after exchanging its node in.
    let (ready, exchanged) = (Hold::new(), Hold::new());
    let inserter = {
        let (ready, exchanged, live) = (ready.clone(), exchanged.clone(), Arc::clone(&live));
        thread::spawn(move || {
            let tid = own_tid();
            ready.arm(Step::InsertReady, tid);
            exchanged.arm(Step::InsertExchanged, tid);
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    ready.reached("the inserting thread");

    // The owner leaves: the exchange below finds INVPTR.
    owner_leave.release();
    owner_leave.reached("the owner's drop");
    assert!(slot_inactive(owner_tid));
    ready.release();
    exchanged.reached("the inserting thread's exchange");
    assert!(
        !slot_inactive(owner_tid),
        "the exchange did not put the node into the left slot"
    );

    // The owner pins again before the rollback: its enter takes the node.
    owner_pin.release();
    owner_pin.reached("the owner's pin");
    assert!(!slot_inactive(owner_tid), "the owner's pin did not enter");
    exchanged.release();
    join_within(inserter, "the insert");
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the batch's count did not reach zero with the node the owner took"
    );
    owner_up.release();
    join_within(owner, "the slot owner");
}
