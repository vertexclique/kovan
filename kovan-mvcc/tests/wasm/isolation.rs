//! Single-threaded, wasm-capable adaptation of `tests/isolation.rs`.
//!
//! `test_snapshot_isolation_readers_ignore_new_commits` spawned a thread to
//! run the concurrent writer, joined it, then asserted the reader's
//! snapshot. Snapshot isolation here is governed by `start_ts` order, not
//! by which OS thread performed the write (see `Txn::read` in
//! `src/percolator.rs`: RepeatableRead reads pin to `start_ts`), so calling
//! the writer's `begin`/`write`/`commit` synchronously in place of
//! `thread::spawn(..).join()` reproduces the exact same operation order and
//! the exact same assertions. `test_read_your_own_writes` has no threading
//! at all.
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

use kovan_mvcc::{IsolationLevel, KovanMVCC};

#[test]
fn test_snapshot_isolation_readers_ignore_new_commits() {
    let db = KovanMVCC::new();

    // 1. Setup initial state
    let mut t0 = db.begin();
    t0.write("x", b"initial".to_vec()).unwrap();
    t0.commit().unwrap();

    // 2. Start Long-Running Reader with RepeatableRead (Snapshot T1)
    let reader_txn = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let val_start = reader_txn.read("x").unwrap();
    assert_eq!(val_start, b"initial");

    // 3. Writer updates 'x' (Commits at T2). Run synchronously in place of
    // the original's spawned thread -- the operation order is what matters,
    // not which thread performs it.
    let mut writer = db.begin();
    writer.write("x", b"updated".to_vec()).unwrap();
    writer.commit().unwrap();

    // 4. Reader should still see "initial" (Snapshot T1)
    // It must NOT see "updated" because T2 > T1
    let val_end = reader_txn.read("x").unwrap();
    assert_eq!(
        val_end, b"initial",
        "Snapshot isolation violated! Reader saw future data."
    );

    // 5. New reader should see "updated"
    let new_reader = db.begin();
    assert_eq!(new_reader.read("x").unwrap(), b"updated");
}

#[test]
fn test_read_your_own_writes() {
    let db = KovanMVCC::new();

    let mut t1 = db.begin();
    t1.write("key", b"old".to_vec()).unwrap();
    t1.commit().unwrap();

    // Begin T2
    let mut t2 = db.begin();
    // Read old
    assert_eq!(t2.read("key").unwrap(), b"old");

    // Write new
    t2.write("key", b"new".to_vec()).unwrap();

    t2.commit().unwrap();

    let t3 = db.begin();
    assert_eq!(t3.read("key").unwrap(), b"new");
}
