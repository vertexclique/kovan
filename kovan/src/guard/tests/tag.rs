//! The era tag of a left slot: a retire whose insert raced a leave takes its
//! node out again (`two_retires_racing_a_leave_leave_the_slot_empty`), and a
//! pin that finds the epoch unchanged still reactivates the slot, so a batch
//! retired next is placed in it (`a_pin_at_an_unchanged_epoch_reactivates_the_slot`).

use super::era::slot_list;
use super::progress::drain;
use super::{
    Counted, DESTRUCTOR_IN_TRY_RETIRE, Hold, eventually, join_within, lock, own_tid,
    published_epoch, slot_inactive, with_handle,
};
use crate::guard::{flush, pin, retire};
use crate::retired::RetiredNode;
use crate::slot::RETIRE_FREQ;
use crate::stall::Step;
use std::boxed::Box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::vec::Vec;

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
    DESTRUCTOR_IN_TRY_RETIRE.store(false, Ordering::SeqCst);
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
    // The reader: pins (its slot active, its era current), drops its guard
    // when told to, then parks until the end.
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
    assert!(
        !DESTRUCTOR_IN_TRY_RETIRE.load(Ordering::SeqCst),
        "a destructor ran inside a submission"
    );
    parked.release();
    reader.join().unwrap();
}

/// The reference's read after a clear took its fast path with the list
/// still INVPTR when the era had not moved, an unprotected section. The
/// outermost pin here reactivates the slot whatever the epoch did: after a
/// leave (list INVPTR, era tagged) a pin at an unchanged epoch finds the
/// slot active with the era untagged, and a batch another thread retires
/// next takes an entry in it, which the guard's drop traverses.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn a_pin_at_an_unchanged_epoch_reactivates_the_slot() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let tid = own_tid();
    // A pin at an unchanged epoch: the slot's era is the epoch the last pin
    // published, as often as other tests leave the epoch alone.
    let guard = (0..1_000)
        .find_map(|_| {
            drop(pin());
            assert!(slot_inactive(tid));
            assert!(crate::slot::era_left(published_epoch()));
            let era = published_epoch() & !crate::slot::ERA_INACTIVE;
            let guard = pin();
            (crate::slot::epoch() == era).then_some(guard)
        })
        .expect("the epoch moved at every pin");
    assert!(!slot_inactive(tid), "the pin left the slot inactive");
    assert!(
        !crate::slot::era_left(published_epoch()),
        "the pin left the era word tagged"
    );
    let retirer = {
        let live = Arc::clone(&live);
        thread::spawn(move || {
            own_tid();
            // Oldest-born: an entry in every active slot, the pinned one's
            // too; the last of these places the batch.
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
        })
    };
    join_within(retirer, "the retiring thread");
    let entry = *slot_list(tid)
        .last()
        .expect("the batch took no entry in the reactivated slot");
    drop(guard);
    assert!(
        !slot_list(tid).contains(&entry),
        "the drop left the placed batch in its slot"
    );
    drain(&live);
}
