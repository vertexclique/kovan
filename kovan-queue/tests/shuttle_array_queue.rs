//! Shuttle model-checked MPMC test for `ArrayQueue`: 2 producers, 2
//! consumers, exact multiset accounting (no loss, no duplication).
//!
//! # What this catches
//!
//! `push`/`pop` share a closed CAS protocol over `head`, `tail`, and each
//! slot's `stamp` (a generation counter distinguishing "ready to write"
//! from "ready to read" for a given lap around the ring buffer). A bug in
//! that protocol (e.g. a stamp update landing in the wrong order relative
//! to the head/tail CAS) could let two consumers read the same slot before
//! it's overwritten (duplication) or let a push's slot never become
//! visible to any consumer (loss). Both are exact-identity bugs: every
//! pushed value is a globally unique tag, so the popped multiset must
//! equal the pushed multiset exactly.
//!
//! # Why shuttle needs the `shuttle` feature
//!
//! `ArrayQueue` is self-contained (no `kovan` dependency): `head`, `tail`,
//! and each slot's `stamp` are its entire concurrency surface. Under the
//! `shuttle` feature they become shuttle-instrumented `AtomicUsize`s (see
//! `kovan-queue/src/array_queue.rs`), giving the scheduler a yield point at
//! every one of the handful of atomic operations push/pop actually
//! performs. No other plumbing is needed: every retry loop here re-reads
//! one of those three atomics each iteration, so there is no un-instrumented
//! spin to hang on.
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

use kovan_queue::array_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

/// Deliberately small and non-power-friendly relative to the item count so
/// the ring wraps several times over the run, forcing producers and
/// consumers to repeatedly contend for the same slots.
const CAPACITY: usize = 4;
const ITEMS_PER_PRODUCER: u64 = 6;
const PRODUCERS: u64 = 2;
const CONSUMERS: u64 = 2;

fn mpmc_no_loss_no_duplication(capacity: usize) {
    let queue = Arc::new(ArrayQueue::<u64>::new(capacity));

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let queue = Arc::clone(&queue);
            shuttle::thread::spawn(move || {
                for i in 0..ITEMS_PER_PRODUCER {
                    // Globally unique tag: identifies exactly one push, so
                    // the final multiset check can catch both loss and
                    // duplication precisely.
                    let mut value = p * ITEMS_PER_PRODUCER + i;
                    while let Err(rejected) = queue.push(value) {
                        value = rejected;
                        shuttle::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    // Claim-counter pattern: each consumer atomically claims the right to
    // pop one more item before it may call `pop()`, so the two consumers
    // together claim exactly `PRODUCERS * ITEMS_PER_PRODUCER` items between
    // them with no shared mutable Vec (each returns its own via the
    // `JoinHandle`, merged after every thread has joined).
    let remaining = Arc::new(AtomicIsize::new((PRODUCERS * ITEMS_PER_PRODUCER) as isize));
    let consumers: Vec<_> = (0..CONSUMERS)
        .map(|_| {
            let queue = Arc::clone(&queue);
            let remaining = Arc::clone(&remaining);
            shuttle::thread::spawn(move || {
                let mut popped = Vec::new();
                loop {
                    if remaining.fetch_sub(1, Ordering::AcqRel) <= 0 {
                        remaining.fetch_add(1, Ordering::AcqRel); // undo: nothing left to claim
                        break;
                    }
                    loop {
                        if let Some(v) = queue.pop() {
                            popped.push(v);
                            break;
                        }
                        // A slot was claimed but its push may not have
                        // landed yet; keep polling. Instrumented (`pop`
                        // reads shuttle-backed atomics), so this yields a
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
fn shuttle_array_queue_mpmc_no_loss_no_duplication() {
    shuttle::check_pct(|| mpmc_no_loss_no_duplication(CAPACITY), 5000, 5);
}

/// Capacity 1 is the degenerate ring where the pre-fix stamp arithmetic
/// collided (a push past capacity was accepted, then pop/Drop livelocked);
/// model-check the same no-loss/no-duplication contract on it. Every
/// push/pop crosses the lap boundary, so this maximizes contention on the
/// lap-jump head/tail arithmetic.
#[test]
fn shuttle_array_queue_capacity_one_no_loss_no_duplication() {
    shuttle::check_pct(|| mpmc_no_loss_no_duplication(1), 5000, 5);
}
