//! Shuttle model-checked analog of `resize_leak_probe.rs`: same
//! create/drop counting oracle, but the interleaving between a table
//! resizer and concurrent writers is *searched for* by shuttle's scheduler
//! instead of hoped for from `std::thread` timing.
//!
//! # What this catches
//!
//! `HashMap::try_resize` clones every live entry into a new table, installs
//! it, then retires the old one. A writer that captured the old table
//! pointer just before the swap (and hasn't finished its own CAS against
//! that table yet) must remain protected until it is done -- otherwise the
//! resize's retirement can free the old table's chains out from under it,
//! a use-after-free that this crate's `Counted` create/drop oracle surfaces
//! as a leak (create without a matching drop) or a double-free (two drops).
//! This is exactly the bug closed by the `hashmap.rs` proxy-birth-epoch fix
//! (commit `30bc947`): see the revert-validation note in the task report.
//!
//! # Why shuttle needs the `shuttle` feature, not just the public API
//!
//! `HashMap`'s table and node pointers are `kovan::Atomic<T>`. Under the
//! `shuttle` feature (`kovan-map/shuttle`, which cascades `kovan/shuttle`),
//! that becomes shuttle-instrumented, so the scheduler gets a yield point
//! at every pointer load/store/CAS -- precisely where this race lives.
//! Two more things had to be fixed for that instrumentation to actually pay
//! off (see `kovan/src/guard.rs` and `kovan-map/src/hashmap.rs` for the
//! full reasoning in place):
//! - `HANDLE`'s `thread_local!` (`kovan/src/guard.rs`): shuttle's "threads"
//!   are cooperative coroutines multiplexed onto one real OS thread, so a
//!   plain `std::thread_local!` handle is shared by every kovan-based
//!   "thread" a test spawns -- collapsing every per-thread epoch slot this
//!   algorithm depends on into one, and making any interleaving-dependent
//!   bug in it unreachable regardless of scheduler or iteration count.
//!   Shadowing the macro with `shuttle::thread_local!` under the feature
//!   fixes this; it was required before this test could reproduce anything.
//! - `resize_spin_hint` (`kovan-map/src/hashmap.rs`): `wait_for_resize`'s
//!   spin needs an explicit yield (`shuttle::hint::spin_loop`, which calls
//!   `shuttle::thread::yield_now`), not just an instrumented load, or a
//!   scheduler can legitimately keep re-running the same spinning thread
//!   forever and exceed shuttle's step budget. The `resizing` flag itself
//!   stays a plain `AtomicBool` under every build, shuttle included --
//!   swapping its type is unnecessary (the yield alone gives the resizer
//!   its turn) and, empirically, actively harmful (an earlier version of
//!   this test that did swap it hit an unrelated, unexplained shuttle-only
//!   heap corruption, reproducible even with a resize that never triggers).
//!
//! # Reliability of the repro (read before tightening this test)
//!
//! This is a narrow race: it needs `EPOCH_FREQ` (128) `retire()` calls to
//! actually move kovan's global epoch, *and* a specific relative timing
//! across all four writer threads' churn, not just one or two well-placed
//! preemptions. `shuttle::check_random` found it far more often than
//! `shuttle::check_pct` at every depth tried (1 through 20) -- this looks
//! like a case where the bug's real "depth" (simultaneous-ordering
//! constraints) is higher than PCT's shallow-preemption bias handles well,
//! so uniform random sampling covers the space better.
//!
//! Even with `check_random`, this is a probabilistic search, not an
//! exhaustive one, and the observed hit rate at the iteration count below
//! is well under 100% per run: reverting the fix and re-running this test
//! repeatedly during development surfaced the bug (a hard allocator abort:
//! "corrupted double-linked list", "double free or corruption (!prev)",
//! "corrupted size vs. prev_size", "free(): double free detected in
//! tcache", ...) on a minority of runs, not on every one, at 20000
//! iterations -- and stayed clean through a run at 150000. Shuttle's own
//! docs are explicit that it is a sound-scheduling, *not* exhaustive,
//! checker for exactly this reason. Treat a clean run as "did not happen
//! to find it this time", not proof of absence; treat a red run here as a
//! real finding, not flakiness to retry away.
//!
//! # Replaying a failure
//!
//! On an assertion-based failure shuttle prints a line like:
//! `test panicked in task "task-0" with schedule: "910102ccdedf9592aba2afd70104"`,
//! replayable with `shuttle::replay(|| { .. }, "<the printed schedule>")`.
//! This bug tends to manifest as a hard allocator abort (SIGABRT: "double
//! free or corruption", "corrupted size vs. prev_size", ...) instead, since
//! the corruption is severe enough to trip glibc before shuttle's panic
//! hook gets a chance to print the schedule; in that case there is no
//! schedule string to replay, only the process exit code / crash log.

#![cfg(feature = "shuttle")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Per-iteration create/drop counters. Built fresh inside the shuttle
/// closure on every iteration (never a global `static`), so iterations --
/// and other `#[test]` functions running concurrently in this binary --
/// can never cross-contaminate each other's counts.
#[derive(Clone)]
struct Counters {
    creates: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Counters {
    fn new() -> Self {
        Self {
            creates: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct Counted {
    #[allow(dead_code)]
    val: u64,
    counters: Counters,
}

impl Counted {
    fn new(val: u64, counters: &Counters) -> Self {
        counters.creates.fetch_add(1, Ordering::SeqCst);
        Self {
            val,
            counters: counters.clone(),
        }
    }
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.counters.creates.fetch_add(1, Ordering::SeqCst);
        Self {
            val: self.val,
            counters: self.counters.clone(),
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.counters.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// Number of insert/remove cycles each thread runs. Each thread churns a
/// small, thread-private key window hard enough on its own to cross
/// kovan's `EPOCH_FREQ` (128 `retire()` calls per thread advances the
/// global epoch once) several times over, so the proxy-birth-epoch gap the
/// fixed bug depended on actually opens up during the run -- not just a
/// single insert each, which never advances the epoch at all and makes the
/// bug structurally unreachable regardless of scheduling.
const OPS_PER_THREAD: u64 = 200;
/// Distinct keys each thread cycles through (insert then remove), chosen
/// so four threads collectively push the table (`MIN_CAPACITY` == 64, grow
/// past 0.75 load factor == 49 live entries) through multiple resizes.
const KEYS_PER_THREAD: u64 = 20;
const THREADS: u64 = 4;

fn resize_vs_concurrent_writers() {
    let counters = Counters::new();
    let map = Arc::new(kovan_map::HashMap::with_capacity(64));

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let map = Arc::clone(&map);
            let counters = counters.clone();
            shuttle::thread::spawn(move || {
                for i in 0..OPS_PER_THREAD {
                    let key = t * KEYS_PER_THREAD + (i % KEYS_PER_THREAD);
                    map.insert(key, Counted::new(key, &counters));
                    if i % 2 == 0 {
                        map.remove(&key);
                    }
                }
                kovan::flush();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    drop(map);

    // A handful of extra flushes drains any batch still parked in a slot
    // (mirrors `resize_leak_probe.rs`'s convergence loop).
    for _ in 0..16 {
        kovan::flush();
    }

    let created = counters.creates.load(Ordering::SeqCst);
    let dropped = counters.drops.load(Ordering::SeqCst);
    assert_eq!(
        created, dropped,
        "resize vs. concurrent writers: created {created}, dropped {dropped} (leak or double-free)"
    );
}

#[test]
fn shuttle_hashmap_resize_vs_concurrent_writers() {
    // See the module doc for why `check_random` over `check_pct`, and for
    // the honest reliability note: this is a probabilistic search, not an
    // exhaustive one. 20000 iterations runs in ~20-25s on a clean pass
    // (measured locally) and caught the reverted bug (a hard allocator
    // abort) in multiple independent runs during development.
    shuttle::check_random(resize_vs_concurrent_writers, 20000);
}
