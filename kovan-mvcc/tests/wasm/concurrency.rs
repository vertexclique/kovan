//! Single-threaded, wasm-capable adaptation of `tests/concurrency.rs`.
//!
//! The original's single test used two threads and a pair of `Barrier`s to
//! force a specific interleaving (both txns write, T1 commits, THEN T2
//! tries to commit). Percolator conflict detection is driven entirely by
//! `start_ts`/`commit_ts` comparisons against the lock table and CF_WRITE
//! (see `Txn::prewrite` in `src/percolator.rs`), not by wall-clock thread
//! scheduling, so that exact interleaving is reproduced deterministically
//! by calling the same operations in the same order from one thread — no
//! `Barrier` needed. What's lost is coverage of the storage/lock-table
//! layer under genuine concurrent access from two OS threads; that part has
//! no single-threaded analogue and remains covered by the threaded
//! original on native.
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
fn sequential_write_write_conflict_first_committer_wins() {
    let db = KovanMVCC::new();

    // Initial setup
    let mut t0 = db.begin();
    t0.write("target", b"v0".to_vec()).unwrap();
    t0.commit().unwrap();

    // T1 and T2 both begin (and buffer their write) before either commits,
    // exactly like the barrier-synchronized original.
    let mut t1 = db.begin();
    t1.write("target", b"v1".to_vec()).unwrap();

    let mut t2 = db.begin();
    t2.write("target", b"v2".to_vec()).unwrap();

    // T1 commits first.
    t1.commit().unwrap();

    // T2 tries to commit. Should fail because T1 committed a newer version
    // during T2's lifetime.
    let res = t2.commit();
    assert!(res.is_err(), "T2 should fail due to write conflict");

    // Verify T1 won
    let tf = db.begin();
    assert_eq!(tf.read("target").unwrap(), b"v1");
}
