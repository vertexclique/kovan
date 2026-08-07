//! Single-threaded, wasm-capable adaptation of `tests/simple_conflict.rs`.
//!
//! Same scenario as `tests/two_key_bug.rs` (two txns transferring between
//! the same pair of accounts) kept as a separate file to mirror the
//! original's structure. The original used a `Barrier` so both
//! transactions read the starting balances before either committed.
//! Percolator's write-write conflict check is driven by
//! `start_ts`/`commit_ts` comparison (see `Txn::prewrite` in
//! `src/percolator.rs`), not by which OS thread runs first, so beginning
//! both txns and reading with both before either writes/commits reproduces
//! the same interleaving deterministically in one thread. What's lost is
//! coverage of the storage/lock-table layer under a genuine concurrent
//! commit race between two OS threads; that has no single-threaded
//! analogue and remains covered by the threaded original on native.
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

use kovan_mvcc::{KovanMVCC, MvccError};

fn transfer(mut txn: kovan_mvcc::Txn, b0: u64, b1: u64) -> Result<u64, MvccError> {
    txn.write("acc_0", (b0 - 10).to_le_bytes().to_vec())
        .map_err(|_| MvccError::StorageError("write0 failed".to_string()))?;
    txn.write("acc_1", (b1 + 10).to_le_bytes().to_vec())
        .map_err(|_| MvccError::StorageError("write1 failed".to_string()))?;
    txn.commit()
}

#[test]
fn sequential_simple_conflict_preserves_total() {
    let db = KovanMVCC::new();

    // Initialize two accounts
    {
        let mut t = db.begin();
        t.write("acc_0", 1000u64.to_le_bytes().to_vec()).unwrap();
        t.write("acc_1", 1000u64.to_le_bytes().to_vec()).unwrap();
        t.commit().unwrap();
    }

    // T1 and T2: both transfer 10 from acc_0 to acc_1, both reading before
    // either writes/commits, matching the original's barrier point.
    let txn1 = db.begin();
    let t1_b0 = u64::from_le_bytes(txn1.read("acc_0").unwrap().try_into().unwrap());
    let t1_b1 = u64::from_le_bytes(txn1.read("acc_1").unwrap().try_into().unwrap());
    eprintln!("[T1] Read: acc_0={}, acc_1={}", t1_b0, t1_b1);

    let txn2 = db.begin();
    let t2_b0 = u64::from_le_bytes(txn2.read("acc_0").unwrap().try_into().unwrap());
    let t2_b1 = u64::from_le_bytes(txn2.read("acc_1").unwrap().try_into().unwrap());
    eprintln!("[T2] Read: acc_0={}, acc_1={}", t2_b0, t2_b1);

    let r1 = transfer(txn1, t1_b0, t1_b1);
    eprintln!("[T1] Commit result: {:?}", r1);

    let r2 = transfer(txn2, t2_b0, t2_b1);
    eprintln!("[T2] Commit result: {:?}", r2);

    eprintln!("[RESULT] T1: {:?}, T2: {:?}", r1, r2);

    // Check final balances
    let txn = db.begin();
    let b0 = u64::from_le_bytes(txn.read("acc_0").unwrap().try_into().unwrap());
    let b1 = u64::from_le_bytes(txn.read("acc_1").unwrap().try_into().unwrap());

    eprintln!("[FINAL] acc_0={}, acc_1={}, total={}", b0, b1, b0 + b1);

    assert_eq!(b0 + b1, 2000, "Money was created or destroyed!");
}
