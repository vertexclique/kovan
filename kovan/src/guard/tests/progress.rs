//! Progress: each operation kovan runs completes in a bounded number of
//! its own steps while other threads are held at chosen points of theirs
//! (see `stall`): inside a traversal, mid insert, in a slow path, mid
//! hand-over, mid exit, mid flush. Where a loop's pass count is the bound,
//! a hook at that loop's step counts it.

use super::{Counted, Hold, eventually, join_within, lock, own_tid, published_epoch, with_handle};
use crate::guard::{flush, pin, retire};
use crate::reclaim::MAX_CACHE;
use crate::retired::RetiredNode;
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
            .store(crate::retired::INVPTR as u64, seqno, Ordering::SeqCst);
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
        for first in global.deactivate_slots(self.tid) {
            if first != 0 {
                with_handle(|h| unsafe { h.traverse_into_cache(first as *mut RetiredNode) });
            }
        }
        global.release_tid(self.tid);
    }
}

/// Whether the head of `tid`'s slot list is an entry whose insert has not
/// yet linked the list it displaced (a retire held mid insert there).
fn head_is_unlinked(tid: usize) -> bool {
    let head = crate::slot::global().thread_slots(tid).first[0].load_lo() as *mut RetiredNode;
    !head.is_null()
        && head as usize != crate::retired::INVPTR
        && unsafe { (*head).next.load(Ordering::Acquire) }.is_null()
}

/// On a fresh thread with an ID: flush until every counted value is
/// freed, failing the test if they are not by `DEADLINE`.
fn drain(live: &Arc<AtomicUsize>) {
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

/// Parking orphans, adopting them and handing thread IDs over wait for no
/// other thread: with one thread held between the two steps of parking
/// (its ID's orphan word swapped to null, the joined chain not yet stored)
/// and another held after adopting a chain and before merging it, threads
/// keep starting (a first pin claims an ID), retiring, flushing (adopting)
/// and exiting (parking, releasing their ID). The held thread's ID goes to
/// no one before it is released, and once both are let go every retired
/// value is freed exactly once.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn orphans_and_ids_change_hands_while_threads_stall() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));

    // Pinned throughout: its slot is eligible for every batch holding an
    // oldest-born value, so an exiting thread's one-node batch of one
    // cannot be placed and is parked.
    let (pinned_up, pinned_done) = (Hold::new(), Hold::new());
    let pinned = {
        let (up, done) = (pinned_up.clone(), pinned_done.clone());
        thread::spawn(move || {
            let guard = pin();
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| done.go.load(Ordering::SeqCst)));
            drop(guard);
            flush();
        })
    };
    pinned_up.reached("the pinned thread");

    // Held at exit, between the two steps of parking its batch.
    let parking = Hold::new();
    let held_tid = Arc::new(AtomicUsize::new(usize::MAX));
    let exiting = {
        let (live, parking, held_tid) = (Arc::clone(&live), parking.clone(), Arc::clone(&held_tid));
        thread::spawn(move || {
            let tid = own_tid();
            held_tid.store(tid, Ordering::SeqCst);
            Counted::retire_oldest(&live);
            parking.arm(Step::OrphanPark, tid);
        })
    };
    parking.reached("the exiting thread");
    let held_tid = held_tid.load(Ordering::SeqCst);

    // Threads that start (a first pin claims an ID), retire and exit while
    // it is held, parking their batch, or flush before they exit, adopting
    // a parked chain.
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

    // Held after taking a chain the churn parked, before merging it.
    let adopting = Hold::new();
    let adopter = {
        let adopting = adopting.clone();
        thread::spawn(move || {
            let tid = own_tid();
            adopting.arm(Step::OrphanAdopt, tid);
            flush();
        })
    };
    adopting.reached("the adopting thread");
    churn(8, false);
    churn(8, true);

    // Still held: the exit has not released its ID (its thread's main
    // function has returned, the exit runs after it), the adopter has not
    // returned from flush().
    assert!(!crate::slot::global().tid_is_released(held_tid));
    assert!(!adopter.is_finished());
    parking.release();
    join_within(exiting, "the held exit");
    adopting.release();
    join_within(adopter, "the held adopter");
    pinned_done.release();
    join_within(pinned, "the pinned thread");

    // Every parked batch is adopted and freed.
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
            // One batch of checkers into this thread's slot (the only
            // active one), moved to the free-list cache by a transition.
            {
                let _guard = pin();
                for _ in 0..RETIRE_FREQ {
                    let node = Box::into_raw(Box::new(ChecksOpen {
                        retired: RetiredNode::new(),
                        tid,
                        closed: Arc::clone(&closed),
                        dropped: Arc::clone(&dropped),
                    }));
                    unsafe { retire(node) };
                }
            }
            crate::slot::advance_epoch();
            drop(pin());
            // The next traversal frees the cache first.
            with_handle(|h| h.list_count.set(MAX_CACHE));
            // One more batch into the slot, for the slow path's loop to
            // traverse; no epoch advance on the way.
            with_handle(|h| h.alloc_counter.set(1));
            {
                let _guard = pin();
                for _ in 0..RETIRE_FREQ {
                    Counted::retire_one(&live);
                }
                // Publish the current epoch, as a protected load does, so
                // the helper's empty epoch transition closes the slot.
                let shared = crate::Atomic::<u64>::null();
                let _ = shared.load(Ordering::Acquire, &_guard);
            }
            pending_tid.store(tid, Ordering::SeqCst);
            pending.arm(Step::SlowPending, tid);
            // The transition, taken through its slow path, inside a pin.
            with_handle(|h| {
                h.pin_count.set(1);
                h.slow_path(0, tid);
                h.pin_count.set(0);
            });
            finished.store(true, Ordering::SeqCst);
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

    // A thread stalled in its slow path, publishing an epoch older than the
    // global one.
    crate::slot::advance_epoch();
    let pending = FakePending::open(crate::slot::epoch() - 1);
    let (pending_tid, slots) = (pending.tid, global.thread_slots(pending.tid));

    // Retires one batch of oldest-born values each time it is asked to.
    let (asked, answered) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let retirer = {
        let (asked, answered, live) =
            (Arc::clone(&asked), Arc::clone(&answered), Arc::clone(&live));
        thread::spawn(move || {
            own_tid();
            for n in 1..=RETIRES {
                assert!(eventually(|| asked.load(Ordering::SeqCst) >= n));
                for _ in 0..RETIRE_FREQ {
                    Counted::retire_oldest(&live);
                }
                answered.store(n, Ordering::SeqCst);
            }
        })
    };

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
/// section ends in one transition at its guard's drop.
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
            assert_ne!(published_epoch(), EPOCH_UNCONDITIONAL);
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
            crate::slot::advance_epoch();
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
/// the slot's owner transitions (its traversal ends at the unlinked node),
/// and a third thread retires into the same slot. Let go, the insert finds
/// the list taken and walks the rest itself; everything is freed once.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn pin_and_retire_end_while_an_insert_is_held() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));

    // The slot owner, with a lower ID than the inserter: the inserter's
    // scan reaches its slot first. One batch already in its slot.
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
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.go.load(Ordering::SeqCst)));
            // A transition: take the list whose head is the held insert.
            crate::slot::advance_epoch();
            drop(pin());
            pinned.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.arrived.load(Ordering::SeqCst)));
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

    // While it is held: the owner transitions, a third thread retires.
    owner_go.release();
    assert!(
        eventually(|| owner_pinned.load(Ordering::SeqCst)),
        "the slot owner's pin did not complete while an insert into its slot was held"
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

/// A traversal walks the list its exchange captured and no more: held
/// after the capture while another thread retires batch after batch into
/// the same slot, the transition, let go, frees exactly the batches it
/// captured, and every entry inserted meanwhile is still in the slot.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn traversal_walks_only_the_list_it_captured() {
    const CAPTURED: usize = 4;
    const INSERTED: usize = 8;
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let traversing = Hold::new();
    let (traversed, owner_tid) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    let checked = Hold::new();
    let owner = {
        let (traversing, traversed, owner_tid, checked, live) = (
            traversing.clone(),
            Arc::clone(&traversed),
            Arc::clone(&owner_tid),
            checked.clone(),
            Arc::clone(&live),
        );
        thread::spawn(move || {
            let tid = own_tid();
            owner_tid.store(tid, Ordering::SeqCst);
            // Batches whose one slot is this thread's (the only one
            // active): the list the transition below captures.
            for _ in 0..CAPTURED * RETIRE_FREQ {
                Counted::retire_one(&live);
            }
            with_handle(|h| h.list_count.set(0));
            traversing.arm(Step::Traverse, tid);
            crate::slot::advance_epoch();
            drop(pin());
            traversed.store(true, Ordering::SeqCst);
            // What the traversal freed waits in the cache: one traversal,
            // the captured batches.
            let (traversals, freed) = with_handle(|h| {
                let mut n = 0;
                let mut refs = h.free_list.get();
                while !refs.is_null() {
                    n += 1;
                    refs = unsafe { (*refs).next.load(Ordering::Relaxed) };
                }
                (h.list_count.get(), n)
            });
            assert_eq!((traversals, freed), (1, CAPTURED));
            assert!(eventually(|| checked.go.load(Ordering::SeqCst)));
        })
    };
    traversing.reached("the traversing thread");
    let owner_tid = owner_tid.load(Ordering::SeqCst);

    // Another thread retires batches of oldest-born values: each takes an
    // entry in the held thread's slot.
    let retirer = {
        let live = Arc::clone(&live);
        thread::spawn(move || {
            own_tid();
            for _ in 0..INSERTED * RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    join_within(retirer, "the retiring thread");
    traversing.release();
    assert!(eventually(|| traversed.load(Ordering::SeqCst)));

    // Every entry inserted during the traversal is still in the slot.
    let mut in_slot = 0;
    let mut at =
        crate::slot::global().thread_slots(owner_tid).first[0].load_lo() as *mut RetiredNode;
    while !at.is_null() && at as usize != crate::retired::INVPTR {
        in_slot += 1;
        at = unsafe { (*at).next.load(Ordering::Acquire) };
    }
    assert_eq!(
        in_slot, INSERTED,
        "the traversal consumed entries inserted after its capture"
    );
    checked.release();
    join_within(owner, "the traversing thread");
    drain(&live);
}

/// The drop that ends an escalated section is one transition, and it waits
/// for no one: a thread stalled in its slow path for good and a retire held
/// mid insert into the escalated thread's slot do not delay it.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn escalated_drop_ends_while_others_are_held() {
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
            section_tid.store(own_tid(), Ordering::SeqCst);
            let guard = pin();
            with_handle(|h| h.escalate());
            escalated.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.go.load(Ordering::SeqCst)));
            drop(guard);
            assert_ne!(published_epoch(), EPOCH_UNCONDITIONAL);
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
/// submits, adopts, helps, advances and frees, and returns.
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
            for _ in 0..RETIRE_FREQ {
                Counted::retire_one(&live);
            }
            traversing.arm(Step::Traverse, tid);
            crate::slot::advance_epoch();
            drop(pin());
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
        result,
        crate::retired::INVPTR as u64,
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
                    result.store(crate::retired::INVPTR as u64, seqno + 2, Ordering::SeqCst);
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
        (crate::retired::INVPTR as u64, seqno + 2),
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
