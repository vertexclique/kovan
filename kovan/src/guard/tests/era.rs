//! The epoch's advance and the traversals it drives: a thread advances the
//! epoch at its first retire, at every `EPOCH_FREQ`th after it, and at every
//! retire or exit that placed a batch; a reader's next outermost pin then
//! traverses what was placed in its slot, and with the epoch where its last
//! transition left it, a pin skips.

use super::progress::drain;
use super::{
    Counted, Hold, eventually, join_within, lock, own_tid, spend_first_retire, with_handle,
};
use crate::guard::pin;
use crate::retired::{INVPTR, RetiredNode};
use crate::slot::{EPOCH_FREQ, RETIRE_FREQ};
use crate::stall::{self, Step};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::vec::Vec;

/// A thread advances the epoch at its first retire and at every
/// `EPOCH_FREQ`th after it, as Crystalline's `alloc_cnt++ % FREQ == 0`
/// does (threads that each retire a few values and exit still move it), and
/// at every retire that placed a batch (the `RETIRE_FREQ`th ones here, the
/// batch going to this thread's slot among the active ones). An advance
/// stamps the thread's next births with the epoch it made, and nothing else
/// moves the stamp between pins, so the retires that advanced are the ones
/// the stamp moved at, whatever other threads do meanwhile.
#[test]
fn a_thread_advances_at_its_first_retire_every_epoch_freq_th_and_every_placed_batch() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        let stamp = || with_handle(|h| h.cached_birth_epoch.get());
        let mut advanced = Vec::new();
        for i in 0..2 * EPOCH_FREQ + 1 {
            let before = stamp();
            Counted::retire_one(&l);
            if stamp() != before {
                advanced.push(i);
            }
        }
        let expected: Vec<usize> = (0..2 * EPOCH_FREQ + 1)
            .filter(|i| i % EPOCH_FREQ == 0 || (i + 1) % RETIRE_FREQ == 0)
            .collect();
        assert_eq!(advanced, expected);
    })
    .join()
    .unwrap();
    drain(&live);
}

/// The entries of `tid`'s slot list, head first.
fn slot_list(tid: usize) -> Vec<*mut RetiredNode> {
    let mut entries = Vec::new();
    let mut at = crate::slot::global().thread_slots(tid).first[0].load_lo() as *mut RetiredNode;
    while !at.is_null() && at as usize != INVPTR {
        entries.push(at);
        at = unsafe { (*at).next.load(Ordering::Acquire) };
    }
    entries
}

/// A reader that has pinned once and keeps its slot active, held until the
/// test lets it go; then it checks that its next pin takes the entry the
/// placed batch put first in its slot. Each entry is inserted once, so that
/// entry is gone from the slot once the pin took the list, whatever other
/// tests of this binary insert meanwhile.
fn reader(pinned: Hold) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let tid = own_tid();
        pinned.arrived.store(true, Ordering::SeqCst);
        assert!(eventually(|| pinned.go.load(Ordering::SeqCst)));
        let entry = *slot_list(tid)
            .last()
            .expect("the batch took no entry in the reader's slot");
        drop(pin());
        assert!(
            !slot_list(tid).contains(&entry),
            "the pin left the placed batch in its slot"
        );
    })
}

/// A batch a retire places in a reader's slot is traversed at the reader's
/// next outermost pin, though the reader never retires or flushes: the
/// retire that placed it advanced the epoch. The retiring thread stays until
/// the reader checked, so the batch, held by its slot too, is not freed and
/// its entry not reused.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_batch_a_retire_places_in_a_readers_slot_is_traversed_at_its_next_pin() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    // The retirer spends its first retire, which advances the epoch on its
    // own, before the reader pins.
    let (spent, retired, done) = (Hold::new(), Hold::new(), Hold::new());
    let retirer = {
        let (live, spent, retired, done) = (
            Arc::clone(&live),
            spent.clone(),
            retired.clone(),
            done.clone(),
        );
        thread::spawn(move || {
            own_tid();
            spend_first_retire(&live);
            spent.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| spent.go.load(Ordering::SeqCst)));
            // Oldest-born: an entry in every active slot, the reader's too;
            // the last of these places the batch.
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
            retired.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| done.go.load(Ordering::SeqCst)));
        })
    };
    spent.reached("the retiring thread");
    let pinned = Hold::new();
    let reading = reader(pinned.clone());
    pinned.reached("the reading thread");
    spent.release();
    retired.reached("the retiring thread's batch");
    pinned.release();
    join_within(reading, "the reading thread");
    done.release();
    join_within(retirer, "the retiring thread");
    drain(&live);
}

/// A batch a thread's exit places in a reader's slot is traversed at the
/// reader's next outermost pin: the exit that placed it advanced the epoch.
/// The exiting thread retires fewer values than a batch holds, so only its
/// exit submits them; its own slot is left out of that submission.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_batch_an_exit_places_in_a_readers_slot_is_traversed_at_its_next_pin() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let spent = Hold::new();
    let exiting = {
        let (live, spent) = (Arc::clone(&live), spent.clone());
        thread::spawn(move || {
            own_tid();
            spend_first_retire(&live);
            spent.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| spent.go.load(Ordering::SeqCst)));
            for _ in 0..3 {
                Counted::retire_oldest(&live);
            }
        })
    };
    spent.reached("the exiting thread");
    let pinned = Hold::new();
    let reading = reader(pinned.clone());
    pinned.reached("the reading thread");
    spent.release();
    join_within(exiting, "the exiting thread");
    pinned.release();
    join_within(reading, "the reading thread");
    drain(&live);
}

/// An outermost pin with the epoch where its last transition left it skips
/// its transition. Other tests of this binary may advance the epoch
/// meanwhile, so one quiet pin in many is enough; a pin that transitioned
/// every time would show none.
#[test]
#[cfg_attr(miri, ignore)] // the stall registry's hooks: a Mutex Miri runs slowly
fn a_pin_with_the_epoch_unchanged_skips_its_transition() {
    let _l = lock();
    thread::spawn(|| {
        let tid = own_tid();
        let transitions = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&transitions);
        stall::arm(Step::TransitionAttempt, tid, move || {
            counted.fetch_add(1, Ordering::SeqCst);
            true
        });
        let quiet = (0..1_000).any(|_| {
            let (before, epoch) = (transitions.load(Ordering::SeqCst), crate::slot::epoch());
            drop(pin());
            transitions.load(Ordering::SeqCst) == before && crate::slot::epoch() == epoch
        });
        stall::disarm(Step::TransitionAttempt, tid);
        assert!(quiet, "every pin transitioned with the epoch unchanged");
    })
    .join()
    .unwrap();
}
