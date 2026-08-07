//! Single-threaded, wasm-capable adaptation of `tests/basic_ops.rs`.
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
fn test_single_txn_read_write() {
    let db = KovanMVCC::new();

    // 1. Write initial value
    let mut txn = db.begin();
    txn.write("key1", b"value1".to_vec()).unwrap();
    let commit_ts = txn.commit().unwrap();
    assert!(commit_ts > 0);

    // 2. Read back
    let txn = db.begin();
    let val = txn.read("key1").expect("Should find key");
    assert_eq!(val, b"value1");
}

#[test]
fn test_overwrite_and_delete() {
    let db = KovanMVCC::new();

    // Setup: Insert A
    let mut t1 = db.begin();
    t1.write("doc", b"v1".to_vec()).unwrap();
    t1.commit().unwrap();

    // Verify V1
    let t2 = db.begin();
    assert_eq!(t2.read("doc").unwrap(), b"v1");

    // Update: A -> B
    let mut t3 = db.begin();
    t3.write("doc", b"v2".to_vec()).unwrap();
    t3.commit().unwrap();

    // Verify V2
    let t4 = db.begin();
    assert_eq!(t4.read("doc").unwrap(), b"v2");

    // Delete
    let mut t5 = db.begin();
    t5.delete("doc").unwrap();
    t5.commit().unwrap();

    // Verify Gone
    let t6 = db.begin();
    assert!(t6.read("doc").is_none());
}

#[test]
fn test_multiple_keys() {
    let db = KovanMVCC::new();

    let mut t1 = db.begin();
    t1.write("a", b"1".to_vec()).unwrap();
    t1.write("b", b"2".to_vec()).unwrap();
    t1.commit().unwrap();

    let t2 = db.begin();
    assert_eq!(t2.read("a").unwrap(), b"1");
    assert_eq!(t2.read("b").unwrap(), b"2");
    assert!(t2.read("c").is_none());
}
