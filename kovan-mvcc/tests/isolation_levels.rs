//! Tests for true PostgreSQL isolation levels in kovan-mvcc.
//!
//! - ReadCommitted: per-read fresh snapshot
//! - RepeatableRead: transaction-wide snapshot (standard SI)
//! - Serializable: SI + read-set validation (SSI)

use kovan_mvcc::{IsolationLevel, KovanMVCC, MvccError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

// ===========================================================================
// IsolationLevel enum tests
// ===========================================================================

#[test]
fn test_isolation_level_default() {
    // PostgreSQL default is Read Committed
    assert_eq!(IsolationLevel::default(), IsolationLevel::ReadCommitted);
}

#[test]
fn test_isolation_level_variants() {
    let rc = IsolationLevel::ReadCommitted;
    let rr = IsolationLevel::RepeatableRead;
    let sr = IsolationLevel::Serializable;
    assert_ne!(rc, rr);
    assert_ne!(rr, sr);
    assert_ne!(rc, sr);
}

// ===========================================================================
// READ COMMITTED tests
// ===========================================================================

#[test]
fn test_rc_sees_latest_committed_per_read() {
    // T1 writes "v1" commits, T2(RC) reads (sees "v1"),
    // T3 writes "v2" commits, T2 reads again (sees "v2")
    let db = KovanMVCC::new();

    let mut t1 = db.begin();
    t1.write("key", b"v1".to_vec()).unwrap();
    t1.commit().unwrap();

    let t2 = db.begin_with_isolation(IsolationLevel::ReadCommitted);
    assert_eq!(t2.read("key").unwrap(), b"v1");

    let mut t3 = db.begin();
    t3.write("key", b"v2".to_vec()).unwrap();
    t3.commit().unwrap();

    // RC: second read sees the new committed value
    assert_eq!(t2.read("key").unwrap(), b"v2");
}

#[test]
fn test_rc_no_dirty_reads() {
    // T1 writes but doesn't commit, T2(RC) doesn't see T1's uncommitted write
    let db = KovanMVCC::new();

    let barrier = Arc::new(Barrier::new(2));

    let db1 = Arc::new(db);
    let db2 = db1.clone();
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    let writer = std::thread::spawn(move || {
        let mut txn = db1.begin();
        txn.write("key", b"uncommitted".to_vec()).unwrap();
        b1.wait(); // Writer has written but NOT committed
        b1.wait(); // Wait for reader to finish
        drop(txn); // Rollback
    });

    let reader = std::thread::spawn(move || {
        b2.wait(); // Wait for writer to write
        let txn = db2.begin_with_isolation(IsolationLevel::ReadCommitted);
        let result = txn.read("key");
        assert!(result.is_none(), "RC must not see uncommitted data");
        b2.wait();
    });

    writer.join().unwrap();
    reader.join().unwrap();
}

#[test]
fn test_rc_non_repeatable_read_allowed() {
    // Demonstrates that RC allows non-repeatable reads
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("key", b"v1".to_vec()).unwrap();
    setup.commit().unwrap();

    let t_rc = db.begin_with_isolation(IsolationLevel::ReadCommitted);
    let read1 = t_rc.read("key").unwrap();
    assert_eq!(read1, b"v1");

    // Concurrent commit changes the value
    let mut writer = db.begin();
    writer.write("key", b"v2".to_vec()).unwrap();
    writer.commit().unwrap();

    // RC: second read may return different value (non-repeatable read)
    let read2 = t_rc.read("key").unwrap();
    assert_eq!(read2, b"v2", "RC allows non-repeatable reads");
    assert_ne!(read1, read2);
}

#[test]
fn test_rc_phantom_read_allowed() {
    // New keys appearing between reads are visible to RC
    let db = KovanMVCC::new();

    let t_rc = db.begin_with_isolation(IsolationLevel::ReadCommitted);
    assert!(t_rc.read("phantom").is_none());

    // Another txn inserts the key
    let mut writer = db.begin();
    writer.write("phantom", b"appeared".to_vec()).unwrap();
    writer.commit().unwrap();

    // RC: sees the phantom
    assert_eq!(t_rc.read("phantom").unwrap(), b"appeared");
}

#[test]
fn test_rc_write_conflict_still_detected() {
    // RC doesn't bypass write-write conflict detection
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("key", b"init".to_vec()).unwrap();
    setup.commit().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let t1_ok = Arc::new(AtomicBool::new(false));
    let t2_ok = Arc::new(AtomicBool::new(false));

    let db = Arc::new(db);
    let db1 = db.clone();
    let db2 = db.clone();
    let b1 = barrier.clone();
    let b2 = barrier.clone();
    let t1c = t1_ok.clone();
    let t2c = t2_ok.clone();

    let h1 = std::thread::spawn(move || {
        let mut txn = db1.begin_with_isolation(IsolationLevel::ReadCommitted);
        txn.write("key", b"v1".to_vec()).unwrap();
        b1.wait();
        if txn.commit().is_ok() {
            t1c.store(true, Ordering::SeqCst);
        }
    });

    let h2 = std::thread::spawn(move || {
        let mut txn = db2.begin_with_isolation(IsolationLevel::ReadCommitted);
        txn.write("key", b"v2".to_vec()).unwrap();
        b2.wait();
        std::thread::yield_now();
        if txn.commit().is_ok() {
            t2c.store(true, Ordering::SeqCst);
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    let t1 = t1_ok.load(Ordering::SeqCst);
    let t2 = t2_ok.load(Ordering::SeqCst);
    assert!(
        !(t1 && t2),
        "Write-write conflict must be detected even under RC"
    );
    assert!(t1 || t2, "At least one should commit");
}

// ===========================================================================
// REPEATABLE READ tests
// ===========================================================================

#[test]
fn test_rr_repeatable_reads() {
    // Same key returns same value throughout transaction
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("key", b"stable".to_vec()).unwrap();
    setup.commit().unwrap();

    let t_rr = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let read1 = t_rr.read("key").unwrap();

    let mut writer = db.begin();
    writer.write("key", b"changed".to_vec()).unwrap();
    writer.commit().unwrap();

    let read2 = t_rr.read("key").unwrap();
    assert_eq!(read1, read2, "RR must return same value for same key");
}

#[test]
fn test_rr_no_dirty_reads() {
    let db = KovanMVCC::new();

    let mut writer = db.begin();
    writer.write("key", b"dirty".to_vec()).unwrap();

    let reader = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    assert!(reader.read("key").is_none());

    drop(writer);
}

#[test]
fn test_rr_phantom_prevented() {
    // New keys committed after start_ts are invisible
    let db = KovanMVCC::new();

    let reader = db.begin_with_isolation(IsolationLevel::RepeatableRead);

    let mut writer = db.begin();
    writer.write("phantom", b"new".to_vec()).unwrap();
    writer.commit().unwrap();

    assert!(
        reader.read("phantom").is_none(),
        "RR prevents phantom reads"
    );
}

#[test]
fn test_rr_write_skew_allowed() {
    // Classic write skew: T1 reads X writes Y, T2 reads Y writes X
    // Both should commit under RR (disjoint write sets)
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("alice_oncall", b"true".to_vec()).unwrap();
    setup.write("bob_oncall", b"true".to_vec()).unwrap();
    setup.commit().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let t1_ok = Arc::new(AtomicBool::new(false));
    let t2_ok = Arc::new(AtomicBool::new(false));

    let db = Arc::new(db);
    let db1 = db.clone();
    let db2 = db.clone();
    let b1 = barrier.clone();
    let b2 = barrier.clone();
    let t1c = t1_ok.clone();
    let t2c = t2_ok.clone();

    let h1 = std::thread::spawn(move || {
        let mut txn = db1.begin_with_isolation(IsolationLevel::RepeatableRead);
        let alice = txn.read("alice_oncall").unwrap();
        let bob = txn.read("bob_oncall").unwrap();
        b1.wait();
        if alice == b"true" && bob == b"true" {
            txn.write("alice_oncall", b"false".to_vec()).unwrap();
        }
        b1.wait();
        if txn.commit().is_ok() {
            t1c.store(true, Ordering::SeqCst);
        }
    });

    let h2 = std::thread::spawn(move || {
        let mut txn = db2.begin_with_isolation(IsolationLevel::RepeatableRead);
        let alice = txn.read("alice_oncall").unwrap();
        let bob = txn.read("bob_oncall").unwrap();
        b2.wait();
        if alice == b"true" && bob == b"true" {
            txn.write("bob_oncall", b"false".to_vec()).unwrap();
        }
        b2.wait();
        if txn.commit().is_ok() {
            t2c.store(true, Ordering::SeqCst);
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    assert!(
        t1_ok.load(Ordering::SeqCst) && t2_ok.load(Ordering::SeqCst),
        "RR allows write skew: both should commit"
    );
}

#[test]
fn test_begin_defaults_to_read_committed() {
    // begin() produces ReadCommitted (the PostgreSQL default)
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("key", b"v1".to_vec()).unwrap();
    setup.commit().unwrap();

    let t_begin = db.begin();
    let t_explicit = db.begin_with_isolation(IsolationLevel::ReadCommitted);

    assert_eq!(t_begin.isolation_level(), IsolationLevel::ReadCommitted);
    assert_eq!(t_begin.isolation_level(), t_explicit.isolation_level());

    // Both see committed value
    assert_eq!(t_begin.read("key").unwrap(), b"v1");
    assert_eq!(t_explicit.read("key").unwrap(), b"v1");

    // After concurrent commit, RC sees the new value (non-repeatable read)
    let mut writer = db.begin();
    writer.write("key", b"v2".to_vec()).unwrap();
    writer.commit().unwrap();

    assert_eq!(t_begin.read("key").unwrap(), b"v2");
    assert_eq!(t_explicit.read("key").unwrap(), b"v2");
}

// ===========================================================================
// SERIALIZABLE tests
// ===========================================================================

#[test]
fn test_serializable_write_skew_prevented() {
    // Classic write skew: T1 reads X,Y writes X. T2 reads X,Y writes Y.
    // Under Serializable, one must abort with SerializationFailure.
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("alice_oncall", b"true".to_vec()).unwrap();
    setup.write("bob_oncall", b"true".to_vec()).unwrap();
    setup.commit().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let t1_ok = Arc::new(AtomicBool::new(false));
    let t2_ok = Arc::new(AtomicBool::new(false));
    let serialization_failure = Arc::new(AtomicBool::new(false));

    let db = Arc::new(db);
    let db1 = db.clone();
    let db2 = db.clone();
    let b1 = barrier.clone();
    let b2 = barrier.clone();
    let t1c = t1_ok.clone();
    let t2c = t2_ok.clone();
    let sf1 = serialization_failure.clone();
    let sf2 = serialization_failure.clone();

    let h1 = std::thread::spawn(move || {
        let mut txn = db1.begin_with_isolation(IsolationLevel::Serializable);
        let alice = txn.read("alice_oncall").unwrap();
        let bob = txn.read("bob_oncall").unwrap();
        b1.wait(); // Both have read
        if alice == b"true" && bob == b"true" {
            txn.write("alice_oncall", b"false".to_vec()).unwrap();
        }
        b1.wait(); // Both have written
        match txn.commit() {
            Ok(_) => t1c.store(true, Ordering::SeqCst),
            Err(MvccError::SerializationFailure { .. }) => {
                sf1.store(true, Ordering::SeqCst);
            }
            Err(_) => {}
        }
    });

    let h2 = std::thread::spawn(move || {
        let mut txn = db2.begin_with_isolation(IsolationLevel::Serializable);
        let alice = txn.read("alice_oncall").unwrap();
        let bob = txn.read("bob_oncall").unwrap();
        b2.wait(); // Both have read
        if alice == b"true" && bob == b"true" {
            txn.write("bob_oncall", b"false".to_vec()).unwrap();
        }
        b2.wait(); // Both have written
        match txn.commit() {
            Ok(_) => t2c.store(true, Ordering::SeqCst),
            Err(MvccError::SerializationFailure { .. }) => {
                sf2.store(true, Ordering::SeqCst);
            }
            Err(_) => {}
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    let t1 = t1_ok.load(Ordering::SeqCst);
    let t2 = t2_ok.load(Ordering::SeqCst);
    let sf = serialization_failure.load(Ordering::SeqCst);

    // Under Serializable: at most one commits, at least one gets SerializationFailure
    assert!(
        !(t1 && t2),
        "Serializable must prevent write skew: both committed!"
    );
    assert!(
        sf,
        "At least one transaction should get SerializationFailure"
    );
}

#[test]
fn test_serializable_no_false_positive_disjoint() {
    // Two transactions with completely disjoint read/write sets both commit
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("a", b"1".to_vec()).unwrap();
    setup.write("b", b"2".to_vec()).unwrap();
    setup.commit().unwrap();

    // T1 reads "a", writes "a"
    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    t1.read("a");
    t1.write("a", b"10".to_vec()).unwrap();

    // T2 reads "b", writes "b"
    let mut t2 = db.begin_with_isolation(IsolationLevel::Serializable);
    t2.read("b");
    t2.write("b", b"20".to_vec()).unwrap();

    // Both should commit since their read/write sets are disjoint
    assert!(t1.commit().is_ok(), "T1 should commit (disjoint sets)");
    assert!(t2.commit().is_ok(), "T2 should commit (disjoint sets)");
}

#[test]
fn test_serializable_read_only_txn_no_conflict() {
    // Read-only serializable transaction never causes SerializationFailure
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("key", b"value".to_vec()).unwrap();
    setup.commit().unwrap();

    let reader = db.begin_with_isolation(IsolationLevel::Serializable);
    assert_eq!(reader.read("key").unwrap(), b"value");

    // Concurrent write
    let mut writer = db.begin();
    writer.write("key", b"new_value".to_vec()).unwrap();
    writer.commit().unwrap();

    // Read again — still sees old value (snapshot)
    assert_eq!(reader.read("key").unwrap(), b"value");

    // Drop reader — no commit, no prewrite, no SSI check → no error
    drop(reader);
}

#[test]
fn test_serializable_phantom_prevention() {
    // T1 reads non-existent key K, T2 inserts K and commits,
    // T1 writes something and tries to commit → SerializationFailure
    let db = KovanMVCC::new();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    // Read non-existent key — added to read_set
    assert!(t1.read("phantom_key").is_none());

    // T2 inserts the key and commits
    let mut t2 = db.begin();
    t2.write("phantom_key", b"inserted".to_vec()).unwrap();
    t2.commit().unwrap();

    // T1 writes something (to trigger prewrite & SSI check)
    t1.write("other_key", b"val".to_vec()).unwrap();
    let result = t1.commit();
    assert!(
        matches!(result, Err(MvccError::SerializationFailure { .. })),
        "Should detect phantom: read-set includes 'phantom_key' which was inserted concurrently. Got: {:?}",
        result
    );
}

#[test]
fn test_serializable_concurrent_write_same_key() {
    // Two serializable txns writing the same key → write-write conflict (not serialization failure)
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("key", b"init".to_vec()).unwrap();
    setup.commit().unwrap();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    let mut t2 = db.begin_with_isolation(IsolationLevel::Serializable);

    t1.write("key", b"v1".to_vec()).unwrap();
    t2.write("key", b"v2".to_vec()).unwrap();

    // First committer wins
    assert!(t1.commit().is_ok());

    // Second gets WriteConflict (not SerializationFailure, since it didn't read the key)
    let result = t2.commit();
    assert!(
        matches!(result, Err(MvccError::WriteConflict { .. })),
        "Should be WriteConflict, not SerializationFailure. Got: {:?}",
        result
    );
}

#[test]
fn test_serializable_read_set_includes_misses() {
    // Reading a key that doesn't exist still adds it to read-set
    let db = KovanMVCC::new();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    // Read a non-existent key
    assert!(t1.read("nonexistent").is_none());

    // Another txn creates it
    let mut t2 = db.begin();
    t2.write("nonexistent", b"now_exists".to_vec()).unwrap();
    t2.commit().unwrap();

    // T1 writes something to trigger commit
    t1.write("unrelated", b"x".to_vec()).unwrap();

    let result = t1.commit();
    assert!(
        matches!(result, Err(MvccError::SerializationFailure { .. })),
        "Read miss should be in read-set. Got: {:?}",
        result
    );
}

// ===========================================================================
// Cross-level tests
// ===========================================================================

#[test]
fn test_mixed_isolation_levels_same_db() {
    // RC, RR, and Serializable transactions coexist on same KovanMVCC instance
    let db = KovanMVCC::new();

    let mut setup = db.begin();
    setup.write("key", b"v1".to_vec()).unwrap();
    setup.commit().unwrap();

    let t_rc = db.begin_with_isolation(IsolationLevel::ReadCommitted);
    let t_rr = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let t_sr = db.begin_with_isolation(IsolationLevel::Serializable);

    // All see the committed value
    assert_eq!(t_rc.read("key").unwrap(), b"v1");
    assert_eq!(t_rr.read("key").unwrap(), b"v1");
    assert_eq!(t_sr.read("key").unwrap(), b"v1");

    // Concurrent commit
    let mut writer = db.begin();
    writer.write("key", b"v2".to_vec()).unwrap();
    writer.commit().unwrap();

    // RC sees new value, RR and Serializable see old value
    assert_eq!(t_rc.read("key").unwrap(), b"v2");
    assert_eq!(t_rr.read("key").unwrap(), b"v1");
    assert_eq!(t_sr.read("key").unwrap(), b"v1");
}

#[test]
fn test_serializable_vs_rr_write_skew() {
    // Same write-skew scenario: RR allows it, Serializable prevents it
    let db = KovanMVCC::new();

    // --- RR: write skew succeeds ---
    let mut setup = db.begin();
    setup.write("X", 1u64.to_le_bytes().to_vec()).unwrap();
    setup.write("Y", 1u64.to_le_bytes().to_vec()).unwrap();
    setup.commit().unwrap();

    let mut t1_rr = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let mut t2_rr = db.begin_with_isolation(IsolationLevel::RepeatableRead);

    let x1 = u64::from_le_bytes(t1_rr.read("X").unwrap().try_into().unwrap());
    let y1 = u64::from_le_bytes(t1_rr.read("Y").unwrap().try_into().unwrap());
    let x2 = u64::from_le_bytes(t2_rr.read("X").unwrap().try_into().unwrap());
    let y2 = u64::from_le_bytes(t2_rr.read("Y").unwrap().try_into().unwrap());

    if x1 + y1 >= 1 {
        t1_rr.write("X", (x1 - 1).to_le_bytes().to_vec()).unwrap();
    }
    if x2 + y2 >= 1 {
        t2_rr.write("Y", (y2 - 1).to_le_bytes().to_vec()).unwrap();
    }

    assert!(t1_rr.commit().is_ok(), "RR T1 should commit");
    assert!(t2_rr.commit().is_ok(), "RR T2 should commit (write skew)");

    // --- Serializable: write skew prevented ---
    // Reset values
    let mut reset = db.begin();
    reset.write("X", 1u64.to_le_bytes().to_vec()).unwrap();
    reset.write("Y", 1u64.to_le_bytes().to_vec()).unwrap();
    reset.commit().unwrap();

    let mut t1_sr = db.begin_with_isolation(IsolationLevel::Serializable);
    let mut t2_sr = db.begin_with_isolation(IsolationLevel::Serializable);

    let x1 = u64::from_le_bytes(t1_sr.read("X").unwrap().try_into().unwrap());
    let y1 = u64::from_le_bytes(t1_sr.read("Y").unwrap().try_into().unwrap());
    let x2 = u64::from_le_bytes(t2_sr.read("X").unwrap().try_into().unwrap());
    let y2 = u64::from_le_bytes(t2_sr.read("Y").unwrap().try_into().unwrap());

    if x1 + y1 >= 1 {
        t1_sr.write("X", (x1 - 1).to_le_bytes().to_vec()).unwrap();
    }
    if x2 + y2 >= 1 {
        t2_sr.write("Y", (y2 - 1).to_le_bytes().to_vec()).unwrap();
    }

    let r1 = t1_sr.commit();
    let r2 = t2_sr.commit();

    // One must fail
    let both_ok = r1.is_ok() && r2.is_ok();
    assert!(
        !both_ok,
        "Serializable must prevent write skew: both committed!"
    );

    // At least one should get SerializationFailure
    let has_sf = matches!(r1, Err(MvccError::SerializationFailure { .. }))
        || matches!(r2, Err(MvccError::SerializationFailure { .. }));
    assert!(
        has_sf,
        "At least one should get SerializationFailure. r1={:?}, r2={:?}",
        r1, r2
    );
}
