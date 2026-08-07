//! Single-threaded, wasm-capable adaptation of `tests/minimal_bug.rs`.
//!
//! The original used a `Barrier` to force both transactions to read the
//! starting balance before either committed, then let both threads race to
//! commit. Percolator's write-write conflict check compares each txn's
//! `start_ts` against CF_WRITE's `commit_ts` (see `Txn::prewrite` in
//! `src/percolator.rs`), not wall-clock scheduling, so beginning both txns
//! and reading with both before either writes/commits reproduces the exact
//! interleaving the barrier enforced -- deterministically, in one thread.
//! What's lost is coverage of the storage/lock-table layer under genuine
//! concurrent commit attempts from two OS threads; that has no
//! single-threaded analogue and remains covered by the threaded original
//! on native. Renamed from `test_minimal_lost_update` since "lost update"
//! is exactly the anomaly this test proves does NOT happen -- the sequential
//! name states the property directly.
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
fn sequential_lost_update_prevented() {
    let db = KovanMVCC::new();

    // Initialize account
    {
        let mut t = db.begin();
        t.write("balance", 100u64.to_le_bytes().to_vec()).unwrap();
        t.commit().unwrap();
    }

    // Both transactions begin and read the balance before either commits,
    // matching the original's barrier point.
    let mut txn1 = db.begin();
    let val1 = u64::from_le_bytes(txn1.read("balance").unwrap().try_into().unwrap());

    let mut txn2 = db.begin();
    let val2 = u64::from_le_bytes(txn2.read("balance").unwrap().try_into().unwrap());

    txn1.write("balance", (val1 + 10).to_le_bytes().to_vec())
        .unwrap();
    txn2.write("balance", (val2 + 10).to_le_bytes().to_vec())
        .unwrap();

    let r1 = txn1.commit().is_ok();
    let r2 = txn2.commit().is_ok();

    // Both should NOT succeed - one must abort due to write conflict
    assert!(!(r1 && r2), "Both transactions succeeded - lost update!");

    // Check final balance
    let txn = db.begin();
    let final_val = u64::from_le_bytes(txn.read("balance").unwrap().try_into().unwrap());

    if r1 && !r2 {
        assert_eq!(final_val, 110, "T1 succeeded, expected 110");
    } else if !r1 && r2 {
        assert_eq!(final_val, 110, "T2 succeeded, expected 110");
    } else {
        // Neither succeeded
        assert_eq!(final_val, 100, "Both aborted, expected 100");
    }
}
