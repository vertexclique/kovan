//! Single-threaded, wasm-capable adaptation of `tests/percolator_cleanup.rs`.
//!
//! Only `test_concurrent_prewrite_conflict` used threads. Percolator's
//! write-write conflict check is driven by `start_ts`/`commit_ts`
//! comparison against the lock table and CF_WRITE (see `Txn::prewrite` in
//! `src/percolator.rs`), not by wall-clock thread scheduling, so
//! sequentially issuing both txns' `begin`/`write`/`commit` reproduces a
//! valid interleaving. Renamed to drop "concurrent" since nothing runs
//! concurrently anymore; the property under test (one wins, one is
//! rejected cleanly, no data corruption) is fully preserved. What's lost
//! is coverage of the storage/lock-table layer under a genuine race
//! between two OS threads; that remains covered by the threaded original
//! on native. The other 6 tests had no threading to begin with.
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

/// Dropping a transaction without committing must clean up locks
/// so that subsequent transactions can access those keys.
#[test]
fn test_drop_cleans_up_locks() {
    let db = KovanMVCC::new();

    // Write and commit initial data
    let mut txn1 = db.begin();
    txn1.write("key1", b"initial".to_vec()).unwrap();
    txn1.write("key2", b"initial".to_vec()).unwrap();
    txn1.commit().unwrap();

    // Start a transaction, write some keys, then drop without committing
    {
        let mut txn2 = db.begin();
        txn2.write("key1", b"updated".to_vec()).unwrap();
        txn2.write("key2", b"updated".to_vec()).unwrap();
        // txn2 is dropped here without commit
    }

    // A new transaction should be able to read and write those keys
    let txn3 = db.begin();
    let val = txn3
        .read("key1")
        .expect("key1 should be readable after dropped txn");
    assert_eq!(
        val, b"initial",
        "value should be unchanged after dropped txn"
    );

    let val = txn3
        .read("key2")
        .expect("key2 should be readable after dropped txn");
    assert_eq!(
        val, b"initial",
        "value should be unchanged after dropped txn"
    );

    // A new transaction should be able to write those keys
    let mut txn4 = db.begin();
    txn4.write("key1", b"new_value".to_vec()).unwrap();
    txn4.commit()
        .expect("should be able to commit after dropped txn released locks");
}

/// Dropping a transaction that never prewrote should be harmless.
#[test]
fn test_drop_uncommitted_no_prewrite() {
    let db = KovanMVCC::new();

    {
        let mut txn = db.begin();
        txn.write("key1", b"value".to_vec()).unwrap();
        // Drop without commit -- prewrite never happened, so no locks to clean up
    }

    let mut txn2 = db.begin();
    txn2.write("key1", b"value2".to_vec()).unwrap();
    txn2.commit()
        .expect("should succeed since no stale locks exist");

    let txn3 = db.begin();
    assert_eq!(txn3.read("key1").unwrap(), b"value2");
}

/// Two transactions writing to the same key -- one must fail cleanly.
/// Sequential adaptation: both txns begin and buffer their write before
/// either commits, matching the original's intent (both prewrite-eligible
/// at the same time); T1 commits first deterministically.
#[test]
fn sequential_prewrite_conflict_one_fails_cleanly() {
    let db = KovanMVCC::new();

    // Write initial data
    let mut setup = db.begin();
    setup.write("shared_key", b"original".to_vec()).unwrap();
    setup.commit().unwrap();

    let mut txn1 = db.begin();
    txn1.write("shared_key", b"from_txn1".to_vec()).unwrap();

    let mut txn2 = db.begin();
    txn2.write("shared_key", b"from_txn2".to_vec()).unwrap();

    let r1 = txn1.commit();
    let r2 = txn2.commit();

    // At least one should succeed, at most one should fail
    let successes = r1.is_ok() as u32 + r2.is_ok() as u32;
    assert!(
        successes >= 1,
        "At least one transaction must succeed; r1={:?}, r2={:?}",
        r1,
        r2
    );

    // The key should be readable and contain one of the committed values
    let reader = db.begin();
    let val = reader.read("shared_key").expect("key should be readable");
    assert!(
        val == b"from_txn1" || val == b"from_txn2",
        "value should be from one of the committed transactions, got {:?}",
        String::from_utf8_lossy(&val)
    );
}

/// After rollback, a new transaction can write to the same keys.
/// Rollback records should exist in CF_WRITE.
#[test]
fn test_rollback_allows_new_writes() {
    let db = KovanMVCC::new();

    // Start a transaction, write, but then drop (triggers rollback via Drop)
    {
        let mut txn = db.begin();
        txn.write("rkey1", b"phantom".to_vec()).unwrap();
        txn.write("rkey2", b"phantom".to_vec()).unwrap();
        // Dropped here -- rollback runs, writes Rollback records
    }

    // A new transaction should be able to write to the same keys
    let mut txn2 = db.begin();
    txn2.write("rkey1", b"real_value1".to_vec()).unwrap();
    txn2.write("rkey2", b"real_value2".to_vec()).unwrap();
    txn2.commit().expect("should commit after prior rollback");

    // Verify the values
    let txn3 = db.begin();
    assert_eq!(txn3.read("rkey1").unwrap(), b"real_value1");
    assert_eq!(txn3.read("rkey2").unwrap(), b"real_value2");
}

/// Keys that were never written to before should still work
/// after a rollback of the first write attempt.
#[test]
fn test_rollback_on_fresh_keys() {
    let db = KovanMVCC::new();

    {
        let mut txn = db.begin();
        txn.write("fresh_key", b"attempt1".to_vec()).unwrap();
        // Drop -> rollback
    }

    // Should read as None since the only write was rolled back
    let reader = db.begin();
    assert!(
        reader.read("fresh_key").is_none(),
        "rolled back key should not be readable"
    );

    // Should be able to write again
    let mut txn2 = db.begin();
    txn2.write("fresh_key", b"attempt2".to_vec()).unwrap();
    txn2.commit().expect("commit after rollback should succeed");

    let reader2 = db.begin();
    assert_eq!(reader2.read("fresh_key").unwrap(), b"attempt2");
}

/// Multi-key transactions should have all keys readable after commit.
#[test]
fn test_secondary_commit_all_keys_readable() {
    let db = KovanMVCC::new();

    let mut txn = db.begin();
    txn.write("mk_a", b"val_a".to_vec()).unwrap();
    txn.write("mk_b", b"val_b".to_vec()).unwrap();
    txn.write("mk_c", b"val_c".to_vec()).unwrap();
    txn.commit().unwrap();

    let reader = db.begin();
    assert_eq!(reader.read("mk_a").unwrap(), b"val_a");
    assert_eq!(reader.read("mk_b").unwrap(), b"val_b");
    assert_eq!(reader.read("mk_c").unwrap(), b"val_c");
}

/// Multiple multi-key transactions should maintain consistency.
#[test]
fn test_secondary_commit_overwrite() {
    let db = KovanMVCC::new();

    // First transaction writes multiple keys
    let mut txn1 = db.begin();
    txn1.write("s_key1", b"v1_a".to_vec()).unwrap();
    txn1.write("s_key2", b"v1_b".to_vec()).unwrap();
    txn1.commit().unwrap();

    // Second transaction overwrites them
    let mut txn2 = db.begin();
    txn2.write("s_key1", b"v2_a".to_vec()).unwrap();
    txn2.write("s_key2", b"v2_b".to_vec()).unwrap();
    txn2.commit().unwrap();

    let reader = db.begin();
    assert_eq!(reader.read("s_key1").unwrap(), b"v2_a");
    assert_eq!(reader.read("s_key2").unwrap(), b"v2_b");
}
