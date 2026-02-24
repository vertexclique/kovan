use kovan_mvcc::KovanMVCC;
use std::sync::Arc;

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
#[test]
fn test_concurrent_prewrite_conflict() {
    let db = Arc::new(KovanMVCC::new());

    // Write initial data
    let mut setup = db.begin();
    setup.write("shared_key", b"original".to_vec()).unwrap();
    setup.commit().unwrap();

    let db1 = db.clone();
    let db2 = db.clone();

    let h1 = std::thread::spawn(move || {
        let mut txn = db1.begin();
        txn.write("shared_key", b"from_txn1".to_vec()).unwrap();
        txn.commit()
    });

    let h2 = std::thread::spawn(move || {
        let mut txn = db2.begin();
        txn.write("shared_key", b"from_txn2".to_vec()).unwrap();
        txn.commit()
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

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
