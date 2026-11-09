//! The epoch's advance and the traversals around it: a thread advances the
//! epoch at its first allocation and at every `EPOCH_FREQ`th after it; a
//! reader's outermost drop traverses what was placed in its slot; every
//! outermost pin activates the slot; and a protected load of a node born
//! after the thread's reservation refreshes the published epoch to cover
//! it.

use super::progress::drain;
use super::{
    Counted, Hold, eventually, join_within, lock, own_tid, published_epoch, reserve, slot_inactive,
    spend_first_retire, with_handle,
};
use crate::guard::pin;
use crate::retired::{INVPTR, RetiredNode};
use crate::slot::{EPOCH_FREQ, ERA_INACTIVE, RETIRE_FREQ};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::vec::Vec;

/// A thread advances the epoch at its first allocation and at every
/// `EPOCH_FREQ`th after it, as the algorithm's `alloc_cnt++ % FREQ == 0`
/// does (threads that each allocate a few values and exit still move it),
/// and at no retire or placement: the retires here place batches (freed at
/// once with no slot active) and move nothing.
#[test]
fn a_thread_advances_at_its_first_allocation_and_every_epoch_freq_th() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        let mut advanced = Vec::new();
        for i in 0..2 * EPOCH_FREQ + 1 {
            let before = crate::slot::epoch();
            // One allocation (the counted value's node), then its retire.
            Counted::retire_one(&l);
            if crate::slot::epoch() != before {
                advanced.push(i);
            }
        }
        let expected: Vec<usize> = (0..2 * EPOCH_FREQ + 1)
            .filter(|i| i % EPOCH_FREQ == 0)
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
    let mut at = crate::slot::global()
        .thread_slots(tid)
        .first
        .load(Ordering::Acquire) as *mut RetiredNode;
    while !at.is_null() && at as usize != INVPTR {
        entries.push(at);
        at = unsafe { (*at).next.load(Ordering::Acquire) };
    }
    entries
}

/// A reader pinned and loaded (its slot active at the current epoch), held
/// until the test lets it go; then it
/// checks that the drop of its guard takes the entry a placed batch put in
/// its slot and leaves the slot inactive. Each entry is inserted once, so
/// that entry is gone from the slot once the drop took the list, whatever
/// other tests of this binary insert meanwhile.
fn reader(pinned: Hold) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let tid = own_tid();
        let guard = pin();
        reserve(&guard);
        pinned.arrived.store(true, Ordering::SeqCst);
        assert!(eventually(|| pinned.go.load(Ordering::SeqCst)));
        let entry = *slot_list(tid)
            .last()
            .expect("the batch took no entry in the reader's slot");
        drop(guard);
        assert!(slot_inactive(tid), "the drop left the slot active");
        assert!(
            !slot_list(tid).contains(&entry),
            "the drop left the placed batch in its slot"
        );
    })
}

/// A batch a retire places in a reader's slot is traversed at the drop of
/// the reader's guard, though the reader never retires, flushes or pins
/// again. The retiring thread stays until the reader checked, so the batch,
/// held by its slot too, is not freed and its entry not reused.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn a_batch_a_retire_places_in_a_readers_slot_is_traversed_at_its_drop() {
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
            let guard = pin();
            for _ in 0..RETIRE_FREQ {
                Counted::retire_oldest(&live);
            }
            retired.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| done.go.load(Ordering::SeqCst)));
            drop(guard);
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
/// drop of the reader's guard. The exiting thread retires fewer values than
/// a batch holds, so only its exit submits them, padded so that the one
/// eligible slot, the reader's, takes an entry.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn a_batch_an_exit_places_in_a_readers_slot_is_traversed_at_its_drop() {
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
    assert_eq!(
        live.load(Ordering::SeqCst),
        3,
        "the exit freed values the reader's slot was eligible for"
    );
    pinned.release();
    join_within(reading, "the reading thread");
    drain(&live);
}

/// Every outermost pin activates the slot, with the epoch unchanged too
/// (the slot was left at the last drop, so there is nothing to skip to):
/// the slot is active when the pin returns and inactive when the guard
/// drops, and the published epoch is the one the last protected load
/// published, which no pin touches.
#[test]
fn every_outermost_pin_enters() {
    let _l = lock();
    thread::spawn(|| {
        let tid = own_tid();
        let cell = crate::Atomic::<Counted>::null();
        let guard = pin();
        let _ = cell.load(Ordering::Acquire, &guard);
        drop(guard);
        let published = crate::slot::epoch();
        assert_eq!(
            published_epoch(),
            published | ERA_INACTIVE,
            "the drop did not tag the era word"
        );
        for _ in 0..1_000 {
            let guard = pin();
            assert!(
                !slot_inactive(tid),
                "an outermost pin left the slot inactive"
            );
            assert_eq!(published_epoch(), published, "a pin published an epoch");
            drop(guard);
            assert!(slot_inactive(tid));
        }
    })
    .join()
    .unwrap();
}

/// A section pinned at one epoch loads a node allocated after the epoch
/// moved (born later than the section's reservation): the load meets a
/// later global epoch and raises the published epoch to at least the
/// node's birth before it returns, so a batch retiring that node is
/// placed in this slot. One thread, so it runs under Miri too.
#[test]
fn a_load_of_a_node_born_after_the_reservation_refreshes_the_published_epoch() {
    let _l = lock();
    thread::spawn(|| {
        own_tid();
        let guard = pin();
        let reserved = published_epoch();
        // The epoch moves (this thread's own advance, as a retire's would),
        // and a node is allocated after it: its birth is later than the
        // reservation.
        crate::slot::advance_epoch();
        let value = Box::into_raw(Box::new(Counted {
            retired: RetiredNode::new(),
            live: Arc::new(AtomicUsize::new(1)),
        }));
        let birth = unsafe { (*value).retired.birth_epoch() };
        assert!(
            birth > reserved,
            "the node was not born after the reservation"
        );
        let cell = crate::Atomic::new(value);
        let loaded = cell.load(Ordering::Acquire, &guard);
        assert_eq!(loaded.as_raw(), value);
        assert!(
            published_epoch() >= birth,
            "the load left the published epoch {} below the node's birth {birth}",
            published_epoch()
        );
        assert!(with_handle(|h| h.cached_epoch.get()) >= birth);
        drop(guard);
        drop(unsafe { Box::from_raw(value) });
    })
    .join()
    .unwrap();
}

/// The same across threads, to the end: a reader pinned at an early epoch
/// loads a node a writer allocated after advancing the epoch; the writer
/// then retires it and flushes. The node stays allocated while the reader's
/// guard lives (its batch was placed in the reader's slot) and is freed by
/// the reader's drop.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn a_batch_of_a_node_born_after_a_reservation_waits_for_the_reader_that_loaded_it() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let cell = Arc::new(crate::Atomic::<Counted>::null());
    let (pinned, published, loaded, retired) = (Hold::new(), Hold::new(), Hold::new(), Hold::new());
    let freed_under_guard = Arc::new(AtomicBool::new(false));
    let reader = {
        let (cell, live, freed) = (
            Arc::clone(&cell),
            Arc::clone(&live),
            Arc::clone(&freed_under_guard),
        );
        let (pinned, published, loaded, retired) = (
            pinned.clone(),
            published.clone(),
            loaded.clone(),
            retired.clone(),
        );
        thread::spawn(move || {
            own_tid();
            let guard = pin();
            pinned.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| published.go.load(Ordering::SeqCst)));
            let held = cell.load(Ordering::Acquire, &guard);
            assert!(!held.is_null());
            loaded.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| retired.go.load(Ordering::SeqCst)));
            // The value is still allocated: the count says so, and the read
            // through the pointer is what AddressSanitizer reports otherwise.
            let alive = live.load(Ordering::SeqCst) == 1
                && unsafe { held.as_ref() }.is_some_and(|c| Arc::ptr_eq(&c.live, &live));
            freed.store(!alive, Ordering::SeqCst);
            drop(guard);
        })
    };
    pinned.reached("the reader");
    let writer = {
        let (cell, live) = (Arc::clone(&cell), Arc::clone(&live));
        let (published, loaded) = (published.clone(), loaded.clone());
        thread::spawn(move || {
            own_tid();
            // The epoch moves after the reader's reservation; the node is
            // born after that.
            crate::slot::advance_epoch();
            live.fetch_add(1, Ordering::SeqCst);
            let value = Box::into_raw(Box::new(Counted {
                retired: RetiredNode::new(),
                live: Arc::clone(&live),
            }));
            let guard = pin();
            let old = cell.swap(
                unsafe { crate::Shared::from_raw(value) },
                Ordering::AcqRel,
                &guard,
            );
            assert!(old.is_null());
            drop(guard);
            published.release();
            loaded.reached("the reader's load");
            let guard = pin();
            let old = cell
                .swap(
                    unsafe { crate::Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::AcqRel,
                    &guard,
                )
                .as_raw();
            drop(guard);
            unsafe { crate::guard::retire(old) };
            crate::guard::flush();
        })
    };
    join_within(writer, "the writer");
    assert_eq!(
        live.load(Ordering::SeqCst),
        1,
        "the batch skipped the reader that loaded a node born after its reservation"
    );
    retired.release();
    join_within(reader, "the reader");
    assert!(
        !freed_under_guard.load(Ordering::SeqCst),
        "the node was freed while the reader's guard lived"
    );
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 0),
        "the reader's drop did not free the node"
    );
}
