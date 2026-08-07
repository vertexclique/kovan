//! Single-threaded, wasm-capable adaptation of `tests/get_or_insert_test.rs`.
//!
//! 2 of the original's 3 tests port verbatim (no threading at all). 1 is
//! not ported.
//!
//! Not ported (race-only):
//! - `test_concurrent_get_or_insert` — 10 threads race to call
//!   `get_or_insert("shared_key", ...)`, checking every thread's returned
//!   `Arc` points at the same allocation (i.e. exactly one insert wins even
//!   under contention). Run from one thread there is no race to win: the
//!   first call always inserts and every later call always finds the
//!   existing entry, which is exactly what `test_get_or_insert_identity`
//!   (below) already checks with two calls. Porting this would just repeat
//!   that same assertion N times without covering the concurrent-claim
//!   property the original exists to prove.

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

use kovan_map::HopscotchMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn test_get_or_insert_returns_inserted_value() {
    let map = HopscotchMap::new();

    // Create an Arc with a counter
    let counter1 = Arc::new(AtomicU64::new(1));

    // Insert it
    let returned = map.get_or_insert("key", counter1.clone());

    // Modify the returned value
    returned.store(100, Ordering::Relaxed);

    // Get it again - should see the modification
    let retrieved = map.get_or_insert("key", Arc::new(AtomicU64::new(999)));

    // This should be 100, not 1 or 999
    assert_eq!(
        retrieved.load(Ordering::Relaxed),
        100,
        "get_or_insert should return the SAME Arc that was inserted, not a clone!"
    );
}

#[test]
fn test_get_or_insert_identity() {
    let map = HopscotchMap::new();

    let arc1 = Arc::new(AtomicU64::new(42));
    let returned1 = map.get_or_insert("key", arc1.clone());

    // The returned Arc should point to the SAME allocation as what was inserted
    assert!(
        Arc::ptr_eq(&returned1, &arc1) || Arc::ptr_eq(&returned1, &Arc::new(AtomicU64::new(42))),
        "First call should return value pointing to inserted data"
    );

    let arc2 = Arc::new(AtomicU64::new(99));
    let returned2 = map.get_or_insert("key", arc2);

    // Second call should return the EXISTING value, and it should be the same as first
    assert!(
        Arc::ptr_eq(&returned1, &returned2),
        "Second call should return same Arc as first call"
    );
}
