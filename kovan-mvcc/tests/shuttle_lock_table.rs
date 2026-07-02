//! Shuttle model-checked test for `LockTable::try_lock`'s documented
//! contract: "Returns Ok(()) if successful, Err if key is already locked".
//!
//! # Why this crate, and why it's a small, self-contained addition
//!
//! `kovan-mvcc`'s only other concurrency-relevant primitive is
//! `parking_lot::Mutex` (used in `percolator.rs`'s SSI commit lock), which
//! would need a shuttle-aware `parking_lot` shim (a real OS mutex used
//! inside a shuttle-scheduled task can genuinely deadlock the whole test
//! process: the thread holding it can be "descheduled" indefinitely from
//! shuttle's point of view while still holding the real lock) -- out of
//! scope for a small addition. `LockTable` avoids that entirely: it has no
//! atomics or locks of its own, and is built solely on `kovan_map::HashMap`.
//!
//! # What this catches
//!
//! `try_lock` calls `HashMap::insert_if_absent`, which must be atomic: two
//! transactions racing to lock the same key must not both observe "key was
//! absent" and both proceed as lock holders. That would be a
//! `kovan_map::HashMap::insert_if_absent` correctness bug (already covered
//! more directly by that crate's own
//! `hashmap_insert_if_absent_atomicity.rs`), exercised here through a real
//! caller's contract instead of the primitive directly.
//!
//! # Why shuttle needs the `shuttle` feature
//!
//! Cascades `kovan-map/shuttle` (see that crate's doc) so `HashMap`'s
//! internal `kovan::Atomic<T>` pointer operations -- the CAS
//! `insert_if_absent` depends on -- are shuttle-instrumented.
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

use kovan_mvcc::{LockInfo, LockTable, LockType};
use std::sync::Arc;

fn concurrent_try_lock_exactly_one_wins() {
    let table = Arc::new(LockTable::new());

    let handles: Vec<_> = (0..2u128)
        .map(|txn_id| {
            let table = Arc::clone(&table);
            shuttle::thread::spawn(move || {
                table.try_lock(
                    "key",
                    LockInfo {
                        txn_id,
                        start_ts: 0,
                        primary_key: "key".to_string(),
                        lock_type: LockType::Put,
                        short_value: None,
                    },
                )
            })
        })
        .collect();

    let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let winners: Vec<u128> = (0..2u128)
        .zip(outcomes.iter())
        .filter(|(_, r)| r.is_ok())
        .map(|(txn_id, _)| txn_id)
        .collect();

    assert_eq!(
        winners.len(),
        1,
        "expected exactly one winner for a concurrent try_lock on the same key, got {winners:?}"
    );
    assert!(
        table.is_locked_by("key", winners[0]),
        "the lock table's recorded holder doesn't match the transaction try_lock said won"
    );
}

#[test]
fn shuttle_lock_table_concurrent_try_lock_exactly_one_wins() {
    // `LockTable::new()` has no `with_capacity` variant: it always
    // allocates through `HashMap::new()`'s `DEFAULT_CAPACITY`
    // (524_288 buckets), zeroed fresh on every shuttle iteration (measured
    // ~0.1s/iteration -- allocator cost, not shuttle exploration cost).
    // With only two threads making one call each, the reachable-
    // interleaving space is small enough that 100 iterations is already a
    // thorough sweep; raising this toward the other tests' 5000-8000 would
    // mostly pay for reallocating that oversized table thousands more
    // times, so it's kept low to stay well inside the per-crate budget.
    shuttle::check_pct(concurrent_try_lock_exactly_one_wins, 100, 5);
}
