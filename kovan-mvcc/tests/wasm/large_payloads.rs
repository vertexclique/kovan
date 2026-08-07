//! Single-threaded, wasm-capable adaptation of `tests/large_payloads.rs`.
//!
//! The original has no threading at all — it is a straight copy, unchanged.
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

use kovan_mvcc::KovanMVCC;

#[test]
fn test_large_blob_handling() {
    let db = KovanMVCC::new();

    // Create 10MB payload
    let size = 10 * 1024 * 1024;
    let mut big_data = Vec::with_capacity(size);
    for i in 0..size {
        big_data.push((i % 255) as u8);
    }

    // Write
    let mut t1 = db.begin();
    t1.write("model_v1", big_data.clone()).unwrap();
    t1.commit().unwrap();

    // Read
    let t2 = db.begin();
    let read_data = t2.read("model_v1").expect("Should read blob");

    // Verify content (Arc ensures zero-copy internally, but we check equality)
    assert_eq!(read_data.len(), size);
    // Spot check to avoid expensive comparison if simple check fails
    assert_eq!(read_data[0], 0);
    assert_eq!(read_data[size - 1], ((size - 1) % 255) as u8);

    // Full compare
    assert_eq!(read_data, big_data);
}
