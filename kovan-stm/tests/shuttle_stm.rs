//! Shuttle model-checked test for `kovan-stm`'s documented commit contract:
//! two conflicting transactions on one `TVar` serialize correctly (exactly
//! one commits per round; the loser's `commit()` returns `false` and
//! `Stm::atomically` retries it) rather than both silently applying a lost
//! update.
//!
//! # The documented contract (from `transaction.rs`'s `commit`)
//!
//! This is a TL2-style STM: reads are optimistic (versioned, no locks);
//! writes are buffered and applied only at commit time, which acquires a
//! per-`TVar` lock bit (`version_lock`'s bit 0) via CAS. If a transaction
//! finds the bit already set, it aborts immediately (`commit` returns
//! `false`) rather than waiting -- the caller (`Stm::atomically`) then
//! re-runs the whole closure. So two transactions racing to commit a write
//! to the *same* `TVar` can never both succeed in the same attempt: one
//! wins the CAS and commits: the other observes the bit set, aborts, and
//! retries with a fresh read snapshot.
//!
//! # What this test asserts
//!
//! The classic STM correctness property that contract implies: concurrent
//! read-modify-write transactions incrementing one shared counter must
//! serialize with no lost update. Two threads each run
//! `INCREMENTS_PER_THREAD` retrying `atomically(|tx| { load; store(+1) })`
//! transactions on one `TVar<i64>`; if the lock-CAS/version-validation
//! protocol ever let both sides commit against the same pre-increment
//! snapshot, the final value would be short of the expected total.
//!
//! # Why shuttle needs the `shuttle` feature
//!
//! `TVarInner::version_lock` is a plain `AtomicU64`; `transaction.rs`'s
//! commit-time lock CAS and `load_version_lock`'s reads are this whole
//! protocol's concurrency surface. Under the `shuttle` feature
//! (`kovan-stm/shuttle`, cascading `kovan/shuttle` and `kovan-map/shuttle`
//! for completeness even though this test doesn't exercise a `kovan_map`
//! path directly), `version_lock` becomes shuttle's `AtomicUsize` (see
//! `var.rs`, `transaction.rs`), giving the scheduler a yield point at every
//! lock CAS and version read -- exactly where a serialization bug would
//! live.
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

use kovan_stm::Stm;

const INCREMENTS_PER_THREAD: i64 = 8;
const THREADS: i64 = 2;

fn conflicting_increments_serialize() {
    let stm = Stm::new();
    let var = stm.tvar(0i64);

    shuttle::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                for _ in 0..INCREMENTS_PER_THREAD {
                    stm.atomically(|tx| {
                        let v = tx.load(&var)?;
                        tx.store(&var, v + 1)?;
                        Ok(())
                    });
                }
            });
        }
    });

    let final_value = stm.atomically(|tx| tx.load(&var));
    assert_eq!(
        final_value,
        THREADS * INCREMENTS_PER_THREAD,
        "lost update: two conflicting transactions committed against the same snapshot \
         instead of serializing"
    );
}

#[test]
fn shuttle_stm_conflicting_increments_serialize() {
    shuttle::check_pct(conflicting_increments_serialize, 5000, 5);
}
