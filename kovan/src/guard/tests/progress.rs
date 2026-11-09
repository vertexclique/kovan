//! Progress: each operation kovan runs completes in a bounded number of
//! its own steps while other threads are held at chosen points of theirs
//! (see `stall`): inside a traversal, mid insert, mid exit, mid flush.
//! Where a loop's pass count is the bound, a hook at that loop's step
//! counts it.

use super::{
    Counted, Hold, eventually, join_within, lock, own_tid, pinned_thread, reserve, slot_inactive,
    with_handle,
};
use crate::guard::{flush, pin};
use crate::retired::{INVPTR, RetiredNode};
use crate::slot::{EPOCH_UNCONDITIONAL, RETIRE_FREQ};
use crate::stall::{self, Step};
use std::boxed::Box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

/// Whether the head of `tid`'s slot list is an entry whose insert has not
/// yet linked the list it displaced (a retire held mid insert there).
fn head_is_unlinked(tid: usize) -> bool {
    let head = crate::slot::global()
        .thread_slots(tid)
        .first
        .load(Ordering::Acquire) as *mut RetiredNode;
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
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
            reserve(&guard);
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

/// A retire's insert held between exchanging its node into another
/// thread's slot and linking the list it displaced keeps no one waiting:
/// the slot's owner leaves (its traversal ends at the unlinked node) and
/// pins again, and a third thread retires into the same slot. Let go, the
/// insert finds the list taken and walks the rest itself; everything is
/// freed once.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
            reserve(&guard);
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.go.load(Ordering::SeqCst)));
            // The drop takes the list whose head is the held insert and ends
            // its traversal there; the next pin enters again.
            drop(guard);
            let guard = pin();
            reserve(&guard);
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
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
            reserve(&guard);
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
/// no one: a retire held mid insert into the escalated thread's slot does
/// not delay it.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
    drain(&live);
}

/// flush() waits for no one either: with a reader pinned (the slot every
/// batch below is placed in), a traversal held after its capture and a
/// retire held mid insert, a flushing thread places its batch and frees,
/// and returns.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn flush_ends_while_others_are_held() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let (pinned, reader) = pinned_thread();

    let traversing = Hold::new();
    let traverser = {
        let (traversing, live) = (traversing.clone(), Arc::clone(&live));
        thread::spawn(move || {
            let tid = own_tid();
            let guard = pin();
            reserve(&guard);
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

    traversing.release();
    join_within(traverser, "the held traversal");
    inserting.release();
    join_within(inserter, "the held retire");
    pinned.release();
    join_within(reader, "the pinned reader");
    drain(&live);
}

/// An insert that finds its slot left when it exchanges its node in (the
/// owner dropped its guard and exited after the scan found the slot
/// eligible) takes the node back out with one compare-exchange of the list
/// word, and the slot is inactive again: it holds no node of the batch,
/// which is freed.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
            reserve(&guard);
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
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
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
            reserve(&guard);
            up.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| leave.go.load(Ordering::SeqCst)));
            drop(guard);
            leave.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| pin_again.go.load(Ordering::SeqCst)));
            let guard = pin();
            reserve(&guard);
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
