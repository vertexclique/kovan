//! Shuttle model-checked test for kovan's core guarantee: a value is never
//! reclaimed while a guard that could still observe it is live.
//!
//! # What this catches
//!
//! `Atom<T>::load` pins a `Guard` and returns an `AtomGuard` borrowing
//! through it; `Atom::store` retires the old value; `flush()` forces
//! reclamation of anything eligible. If the epoch/slot protocol ever
//! mis-tracked an active guard as "not protecting" its loaded value (the
//! general bug class -- see `kovan-map`'s `shuttle_resize.rs` for the
//! specific historical instance of it, a proxy stamped with the wrong
//! birth epoch), a concurrent `store` + `flush` could reclaim the value
//! while the reader's guard is still alive: a use-after-free. Like
//! `resize_leak_probe.rs` (this crate's existing, non-shuttle regression
//! test for the same class of property), this is checked via exact
//! create/drop accounting rather than an in-flight snapshot assertion:
//! kovan's reclamation is deferred and eventually-consistent by design, so
//! asserting "not dropped yet" at one instant is inherently racy, while a
//! double-free (premature reclaim colliding with the real one) or a leak
//! (a node the protocol got confused about) both show up reliably as
//! `created != dropped` once every retired node has had a chance to drain.
//!
//! # Why shuttle needs the `shuttle` feature
//!
//! `Atom<T>` is built on `kovan::Atomic<T>`, whose pointer word is
//! shuttle's `AtomicUsize` under the `shuttle` feature (see `atomic.rs`),
//! giving the scheduler a yield point at every load/store/CAS. This test
//! also needs `guard.rs`'s `HANDLE` `thread_local!` shadow (shuttle's
//! `thread::LocalKey` instead of `std`'s): shuttle's "threads" are
//! cooperative coroutines multiplexed onto one real OS thread, so without
//! it, the reader and writer below would silently share one `Handle` --
//! one `tid`, one epoch slot -- collapsing the exact per-thread distinction
//! this property depends on.
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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

impl Drop for Counted {
    fn drop(&mut self) {
        self.counters.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn guard_outlives_concurrent_retire() {
    let counters = Counters::new();
    let atom = Arc::new(kovan::Atom::new(Counted::new(1, &counters)));

    let reader = {
        let atom = Arc::clone(&atom);
        shuttle::thread::spawn(move || {
            // Pins a guard-backed read of whatever value is current, holds
            // it, and reads through it once more before releasing --
            // exercising the memory, not just holding an unused pointer.
            let guard = atom.load();
            let seen = guard.val;
            assert!(seen == 1 || seen == 2, "value corrupted: saw {seen}");
            drop(guard);
            // Required, not just tidy: a retired batch can land in this
            // thread's slot (it was an active guard when the writer
            // retired the old value) and `flush()` is what drains/
            // deactivates that slot. `flush()` only ever touches the
            // *calling* thread's state, so skipping this would leave the
            // batch parked here indefinitely -- the other thread's flushes
            // below can never reach it, showing up as a false-positive
            // leak, not a real one.
            kovan::flush();
        })
    };
    let writer = {
        let atom = Arc::clone(&atom);
        let counters = counters.clone();
        shuttle::thread::spawn(move || {
            // Retires the old value and immediately forces reclamation:
            // the reader above may still be holding a guard over it.
            atom.store(Counted::new(2, &counters));
            kovan::flush();
        })
    };

    reader.join().unwrap();
    writer.join().unwrap();
    drop(atom);

    // A handful of extra flushes drains any batch still parked in a slot
    // (mirrors `resize_leak_probe.rs`'s convergence loop).
    for _ in 0..8 {
        kovan::flush();
    }

    let created = counters.creates.load(Ordering::SeqCst);
    let dropped = counters.drops.load(Ordering::SeqCst);
    assert_eq!(
        created, dropped,
        "created {created}, dropped {dropped}: a retired value was reclaimed \
         while a guard could still observe it (or leaked)"
    );
}

#[test]
fn shuttle_guard_outlives_concurrent_retire() {
    shuttle::check_pct(guard_outlives_concurrent_retire, 5000, 5);
}
