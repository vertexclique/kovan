//! Single-threaded, wasm-capable adaptation of `tests/concurrent_stress.rs`.
//!
//! 4 of the original's 6 tests are adapted below (unchanged in substance —
//! none of them actually depend on threading, only on the sequence of
//! operations). 2 are not ported.
//!
//! Not ported (race-only):
//! - `test_concurrent_writes_different_keys` — 4 threads each writing 200
//!   disjoint keys, then verifying every key. Because the keys never
//!   overlap there is no lock contention and no conflict to resolve: the
//!   entire value of the original is proving that concurrent access from
//!   real OS threads to *different* keys doesn't corrupt shared state
//!   (torn writes, lost inserts, racy map internals). Run from one thread
//!   the operations reduce to "write N keys, read them back", which is
//!   already covered by `test_many_keys_single_txn` in this same file and
//!   by `tests/wasm/basic_ops.rs`; porting it would just pad the suite.
//! - `test_concurrent_readers` — 8 threads concurrently reading 50
//!   pre-populated (already-committed) keys. With no concurrent writer,
//!   the only thing under test is that parallel *reads* from real threads
//!   don't race each other on the read path. Sequentially that is just
//!   "read 50 keys back", already covered elsewhere in this file.

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
use std::sync::Arc;

#[test]
#[cfg_attr(miri, ignore)]
fn test_overwrite_same_key() {
    let db = KovanMVCC::new();

    for i in 0..100u8 {
        let mut txn = db.begin();
        txn.write("key", vec![i]).unwrap();
        txn.commit().unwrap();
    }

    let txn = db.begin();
    let val = txn.read("key").unwrap();
    assert_eq!(val, vec![99u8]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_delete_and_reinsert() {
    let db = KovanMVCC::new();

    let mut txn = db.begin();
    txn.write("key", b"first".to_vec()).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin();
    txn.delete("key").unwrap();
    txn.commit().unwrap();

    let txn = db.begin();
    assert!(txn.read("key").is_none());

    let mut txn = db.begin();
    txn.write("key", b"second".to_vec()).unwrap();
    txn.commit().unwrap();

    let txn = db.begin();
    assert_eq!(txn.read("key").unwrap(), b"second".to_vec());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_snapshot_consistency() {
    let db = Arc::new(KovanMVCC::new());

    let mut txn = db.begin();
    txn.write("a", b"1".to_vec()).unwrap();
    txn.write("b", b"1".to_vec()).unwrap();
    txn.commit().unwrap();

    // Start a read transaction
    let read_txn = db.begin();

    // Write new values
    let mut txn = db.begin();
    txn.write("a", b"2".to_vec()).unwrap();
    txn.write("b", b"2".to_vec()).unwrap();
    txn.commit().unwrap();

    // Read transaction should see old values (snapshot isolation)
    let a = read_txn.read("a").unwrap();
    let b = read_txn.read("b").unwrap();
    assert_eq!(a, b, "snapshot should be consistent");
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_many_keys_single_txn() {
    let db = KovanMVCC::new();

    let mut txn = db.begin();
    for i in 0..500 {
        txn.write(&format!("key_{}", i), format!("value_{}", i).into_bytes())
            .unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin();
    for i in 0..500 {
        let val = txn.read(&format!("key_{}", i)).unwrap();
        assert_eq!(val, format!("value_{}", i).into_bytes());
    }
}
