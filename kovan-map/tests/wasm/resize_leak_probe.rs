//! Single-threaded, wasm-capable adaptation of `tests/resize_leak_probe.rs`.
//!
//! 2 of the original's 4 tests were already single-threaded and port
//! verbatim (`hopscotch_resize_frees_old_entries`,
//! `hashmap_resize_frees_old_entries`). The other 2
//! (`hashmap_concurrent_resize_accounting`,
//! `hopscotch_concurrent_resize_accounting`) are adapted to run the same
//! total number of create/drop-tracked inserts and removes from one thread
//! instead of across `CONCURRENT_THREADS` real threads. The property under
//! test — exact create/drop parity across grows, shrinks, and retires, with
//! no leak and no double-free — doesn't depend on concurrency to hold; what
//! is lost is coverage of a resize racing with several writers touching
//! *different* keys at the same time (accounting staying exact under that
//! interleaving specifically), which has no single-threaded analogue. The
//! threaded originals remain the real coverage on native.
//!
//! Not ported (race-only): none.

// On wasm32 there is no libtest runner; the wasm-bindgen harness supplies one.
// Aliasing its attribute to `test` lets every case below run unmodified on both
// native (libtest) and wasm32-unknown-unknown (wasm-bindgen-test).
// wasm-bindgen-test only registers its harness export under
// `target_os = "unknown"`. wasm32-wasip1 is `target_os = "wasi"`, where the
// attribute compiles but libtest never sees the function -- the binary would
// report "0 tests" and exit 0. So wasip1 keeps the plain libtest attribute and
// runs under wasmtime; only unknown-unknown swaps in the bindgen harness.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

// vertexia: miri (especially with -Zmiri-weak-memory-emulation, used to
// falsify resize/migration ordering) is orders of magnitude slower than
// native execution. These counts keep the interleaving space small enough
// to finish in practical time while still exercising a grow and a shrink,
// which is the shape that matters for this file's purpose. The native
// (non-miri) counts are unchanged from `tests/resize_leak_probe.rs`.
#[cfg(miri)]
const SEQUENTIAL_ITERS: u64 = 24;
#[cfg(not(miri))]
const SEQUENTIAL_ITERS: u64 = 300;

#[cfg(miri)]
const LOAD_THREADS_EQUIVALENT: u64 = 3;
#[cfg(not(miri))]
const LOAD_THREADS_EQUIVALENT: u64 = 8;

#[cfg(miri)]
const LOAD_ITERS: u64 = 16;
#[cfg(not(miri))]
const LOAD_ITERS: u64 = 1000;

static CREATES: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);
static LOCK: Mutex<()> = Mutex::new(());

struct Counted(#[allow(dead_code)] u64);
impl Counted {
    fn new(v: u64) -> Self {
        CREATES.fetch_add(1, Ordering::SeqCst);
        Counted(v)
    }
}
impl Clone for Counted {
    fn clone(&self) -> Self {
        CREATES.fetch_add(1, Ordering::SeqCst);
        Counted(self.0)
    }
}
impl Drop for Counted {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

fn check(label: &str) {
    // Reclamation is eventually-consistent (deferred, slot-based; flush()
    // adopts one orphan per call). Drain to a fixed point: flush until the
    // drop count stops moving, then assert. A real leak converges *below*
    // `created` (and fails); merely in-flight nodes converge up to it.
    let mut last = DROPS.load(Ordering::SeqCst);
    let mut stable = 0u32;
    for _ in 0..2000 {
        kovan::flush();
        let d = DROPS.load(Ordering::SeqCst);
        if d == last {
            stable += 1;
            if stable >= 16 {
                break;
            }
        } else {
            stable = 0;
            last = d;
        }
    }
    let c = CREATES.load(Ordering::SeqCst);
    let d = DROPS.load(Ordering::SeqCst);
    assert_eq!(
        c, d,
        "{label}: created {c}, dropped {d} (leak or double-free)"
    );
}

#[test]
fn hopscotch_resize_frees_old_entries() {
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = kovan_map::HopscotchMap::with_capacity(64);
        for i in 0..SEQUENTIAL_ITERS {
            map.insert(i, Counted::new(i)); // forces several grows
        }
        for i in 0..SEQUENTIAL_ITERS {
            map.remove(&i); // forces shrinks
        }
        drop(map);
    }
    check("hopscotch grow/shrink");
}

#[test]
fn hashmap_resize_frees_old_entries() {
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = kovan_map::HashMap::with_capacity(64);
        for i in 0..SEQUENTIAL_ITERS {
            map.insert(i, Counted::new(i));
        }
        for i in 0..SEQUENTIAL_ITERS {
            map.remove(&i);
        }
        drop(map);
    }
    check("hashmap grow/shrink");
}

/// Sequential adaptation of `resize_leak_probe::hashmap_concurrent_resize_accounting`.
/// The original spread the same total insert/remove workload across
/// `CONCURRENT_THREADS` real threads to drive a grow while several writers
/// raced on different keys. This runs the identical total number of
/// operations (`LOAD_THREADS_EQUIVALENT * LOAD_ITERS`) from one thread — it
/// still forces the same growth pattern and checks the same exact
/// create/drop parity, just without a resize racing concurrent writers.
#[test]
fn hashmap_resize_accounting_sequential_load() {
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = kovan_map::HashMap::with_capacity(64);
        for t in 0..LOAD_THREADS_EQUIVALENT {
            for i in 0..LOAD_ITERS {
                let key = t * 10_000 + i;
                map.insert(key, Counted::new(key));
                if i % 3 == 0 {
                    map.remove(&key);
                }
            }
        }
        kovan::flush();
        drop(map);
    }
    check("hashmap sequential-load grow");
}

/// Sequential adaptation of `resize_leak_probe::hopscotch_concurrent_resize_accounting`.
/// Same shape as `hashmap_resize_accounting_sequential_load` above, on the
/// other map implementation.
#[test]
fn hopscotch_resize_accounting_sequential_load() {
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = kovan_map::HopscotchMap::with_capacity(64);
        for t in 0..LOAD_THREADS_EQUIVALENT {
            for i in 0..LOAD_ITERS {
                let key = t * 10_000 + i;
                map.insert(key, Counted::new(key));
                if i % 3 == 0 {
                    map.remove(&key);
                }
            }
        }
        kovan::flush();
        drop(map);
    }
    check("hopscotch sequential-load grow");
}
