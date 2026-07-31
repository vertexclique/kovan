//! Shuttle model-checked tests for `SegQueue`, the unbounded segment-based
//! MPMC queue `kovan-channel`'s `WaitList` is built on.
//!
//! # Why shuttle needs the `shuttle` feature
//!
//! `SegQueue`'s `head`/`tail`/each `Segment::next` are `kovan::Atomic<T>`,
//! already shuttle-instrumented by this crate's `shuttle` feature cascading
//! into `kovan/shuttle`. `Slot::state` and `len` are this queue's own plain
//! atomics on top of that; under the `shuttle` feature they become
//! `shuttle::sync::atomic::AtomicUsize` (see `kovan-queue/src/seg_queue.rs`),
//! giving the scheduler a yield point at every slot-state check and CAS, and
//! at the `len` accounting -- the exact places a lost-update, a torn view
//! between two independent atomics, or a scan that gives up too early could
//! hide.
//!
//! # Replaying a failure
//!
//! On failure shuttle prints a line like:
//! `test panicked in task "task-0" with schedule: "910102ccdedf9592aba2afd70104"`
//! Reproduce it deterministically (single run, no search) with:
//! ```ignore
//! shuttle::replay(|| { /* paste the closure body below */ }, "<the printed schedule>");
//! ```

#![cfg(feature = "shuttle")]

use kovan_queue::seg_queue::SegQueue;
use std::sync::Arc;

/// `SEGMENT_SIZE` is private to `seg_queue.rs` (an implementation detail,
/// not part of the public API), so the segment-boundary test below tracks
/// it by hand. Keep this in sync if `seg_queue.rs`'s constant changes.
const SEGMENT_SIZE: u64 = 32;

/// (a) MPMC no-loss: N=3 producers x M=2 consumers, every pushed item
/// popped exactly once. Same claim-counter pattern as
/// `shuttle_array_queue.rs`'s `mpmc_no_loss_no_duplication` (each consumer
/// atomically claims the right to pop one more item before calling `pop`,
/// so the two consumers together claim exactly `PRODUCERS *
/// ITEMS_PER_PRODUCER` items with no shared mutable `Vec`), adapted to
/// `SegQueue`'s unbounded `push` (infallible, no capacity to retry
/// against).
const ITEMS_PER_PRODUCER: u64 = 4;
const PRODUCERS: u64 = 3;
const CONSUMERS: u64 = 2;

fn mpmc_no_loss_no_duplication() {
    let queue = Arc::new(SegQueue::<u64>::new());

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let queue = Arc::clone(&queue);
            shuttle::thread::spawn(move || {
                for i in 0..ITEMS_PER_PRODUCER {
                    // Globally unique tag: identifies exactly one push, so
                    // the final multiset check catches both loss and
                    // duplication precisely.
                    queue.push(p * ITEMS_PER_PRODUCER + i);
                }
            })
        })
        .collect();

    let remaining = Arc::new(std::sync::atomic::AtomicIsize::new(
        (PRODUCERS * ITEMS_PER_PRODUCER) as isize,
    ));
    let consumers: Vec<_> = (0..CONSUMERS)
        .map(|_| {
            let queue = Arc::clone(&queue);
            let remaining = Arc::clone(&remaining);
            shuttle::thread::spawn(move || {
                let mut popped = Vec::new();
                loop {
                    if remaining.fetch_sub(1, std::sync::atomic::Ordering::AcqRel) <= 0 {
                        remaining.fetch_add(1, std::sync::atomic::Ordering::AcqRel); // undo: nothing left to claim
                        break;
                    }
                    loop {
                        if let Some(v) = queue.pop() {
                            popped.push(v);
                            break;
                        }
                        // A slot was claimed but its push may not have
                        // landed yet; keep polling. `pop` reads
                        // shuttle-backed atomics, so this yields a
                        // scheduling point every iteration.
                        shuttle::hint::spin_loop();
                    }
                }
                popped
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    let mut all_popped: Vec<u64> = consumers
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .collect();
    all_popped.sort_unstable();

    let mut expected: Vec<u64> = (0..PRODUCERS * ITEMS_PER_PRODUCER).collect();
    expected.sort_unstable();

    assert_eq!(
        all_popped, expected,
        "MPMC push/pop lost or duplicated an item"
    );
}

#[test]
fn shuttle_seg_queue_mpmc_no_loss_no_duplication() {
    shuttle::check_pct(mpmc_no_loss_no_duplication, 5000, 5);
}

/// (b) The F-16 linearizability probe: a push that COMPLETES (returns,
/// joined) strictly before a pop begins must never see that pop return
/// `None`. This is the exact property `WaitList::notify_one` leans on when
/// it calls `SegQueue::pop` after its own `wakeup_fence`: a registration
/// whose `push` has already returned must be visible to a `pop` that
/// starts afterward, or a real wakeup can be lost the same way F-16 lost
/// one. Two producers race each other for segment slots (genuine
/// contention on the left-to-right slot-claim order, not a single thread
/// filling it deterministically) before both are joined; the popping
/// thread then runs with no synchronization beyond that join.
fn push_then_pop_never_sees_none() {
    let queue = Arc::new(SegQueue::<u64>::new());

    let p1 = {
        let queue = Arc::clone(&queue);
        shuttle::thread::spawn(move || queue.push(1u64))
    };
    let p2 = {
        let queue = Arc::clone(&queue);
        shuttle::thread::spawn(move || queue.push(2u64))
    };
    p1.join().unwrap();
    p2.join().unwrap();

    let popper = {
        let queue = Arc::clone(&queue);
        shuttle::thread::spawn(move || {
            let mut got = vec![queue.pop(), queue.pop()];
            got.sort_unstable();
            got
        })
    };
    let got = popper.join().unwrap();

    assert_eq!(
        got,
        vec![Some(1), Some(2)],
        "pop() returned None despite both pushes having already completed"
    );
}

#[test]
fn shuttle_seg_queue_push_then_pop_never_sees_none() {
    shuttle::check_pct(push_then_pop_never_sees_none, 5000, 5);
}

/// (c) Segment-boundary race: a producer pushes past `SEGMENT_SIZE` (32,
/// forcing exactly one "segment full, allocate a new one" transition in
/// `push`) while a consumer concurrently drains, forcing that allocation
/// to interleave with `pop`'s "every slot consumed, retire the old
/// segment and advance head" transition.
fn segment_boundary_race() {
    let items = SEGMENT_SIZE + 3;
    let queue = Arc::new(SegQueue::<u64>::new());

    let producer = {
        let queue = Arc::clone(&queue);
        shuttle::thread::spawn(move || {
            for i in 0..items {
                queue.push(i);
            }
        })
    };

    let consumer = {
        let queue = Arc::clone(&queue);
        shuttle::thread::spawn(move || {
            let mut popped = Vec::new();
            while (popped.len() as u64) < items {
                if let Some(v) = queue.pop() {
                    popped.push(v);
                } else {
                    shuttle::hint::spin_loop();
                }
            }
            popped
        })
    };

    producer.join().unwrap();
    let mut popped = consumer.join().unwrap();
    popped.sort_unstable();

    let expected: Vec<u64> = (0..items).collect();
    assert_eq!(
        popped, expected,
        "an item was lost or duplicated crossing a segment boundary"
    );
}

#[test]
fn shuttle_seg_queue_segment_boundary_race() {
    shuttle::check_pct(segment_boundary_race, 3000, 5);
}

/// (d) `len`/`is_empty` vs. concurrent push -- the race family behind the
/// "is_empty relocation race" kovan pin bump (a derived, approximate
/// quantity read concurrently with the mutation it summarizes). `len` is a
/// plain counter alongside the real state (the slot state machine and the
/// segment list): incremented after publish in `push`, decremented after
/// claim in `pop`, both `fetch_add`/`fetch_sub` -- atomic read-modify-write,
/// so the counter itself cannot lose an update regardless of ordering.
/// This proves concurrent pushes racing concurrent `len`/`is_empty` reads
/// never desync the count: readers mid-flight get no assertion (both are
/// documented as approximate then), but once every push has joined, `len`
/// must equal the exact net total, and draining it back to zero must bring
/// `is_empty` back to `true`.
fn len_matches_net_pushes_under_concurrent_reads() {
    let queue = Arc::new(SegQueue::<u64>::new());

    let producers: Vec<_> = (0..2)
        .map(|p| {
            let queue = Arc::clone(&queue);
            shuttle::thread::spawn(move || {
                for i in 0..2 {
                    queue.push(p * 2 + i);
                }
            })
        })
        .collect();

    // Concurrent reader: samples len()/is_empty() while pushes are still
    // landing. No assertion on what a given sample returns (both are
    // documented approximate mid-flight); this just proves reading them
    // concurrently with push is race-free (no panic, no hang) and puts
    // real contention on the same atomics the final assertions check.
    let reader = {
        let queue = Arc::clone(&queue);
        shuttle::thread::spawn(move || {
            for _ in 0..4 {
                let _ = queue.is_empty();
                let _ = queue.len();
                shuttle::hint::spin_loop();
            }
        })
    };

    for p in producers {
        p.join().unwrap();
    }
    reader.join().unwrap();

    assert_eq!(
        queue.len(),
        4,
        "len() did not match the net number of completed pushes"
    );
    assert!(
        !queue.is_empty(),
        "is_empty() reported true with 4 published items outstanding"
    );

    for _ in 0..4 {
        assert!(
            queue.pop().is_some(),
            "an item counted by len() was not actually poppable"
        );
    }
    assert_eq!(
        queue.len(),
        0,
        "len() did not return to 0 after draining every pushed item"
    );
    assert!(
        queue.is_empty(),
        "is_empty() reported false after the queue was fully drained"
    );
}

#[test]
fn shuttle_seg_queue_len_matches_net_pushes_under_concurrent_reads() {
    shuttle::check_pct(len_matches_net_pushes_under_concurrent_reads, 5000, 5);
}
