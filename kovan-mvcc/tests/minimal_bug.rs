use kovan_mvcc::KovanMVCC;
use std::sync::Arc;
use std::thread;

#[test]
fn test_minimal_lost_update() {
    let db = Arc::new(KovanMVCC::new());

    // Initialize account
    {
        let mut t = db.begin();
        t.write("balance", 100u64.to_le_bytes().to_vec()).unwrap();
        t.commit().unwrap();
    }

    let db1 = db.clone();
    let db2 = db.clone();

    // Barrier ensures both transactions begin and read before either commits.
    // Without this, one thread can complete its entire transaction before the
    // other even calls begin(), making the test non-deterministic.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    // Both transactions try to add 10 to the balance
    let h1 = thread::spawn(move || {
        let mut txn = db1.begin();
        let val = u64::from_le_bytes(txn.read("balance").unwrap().try_into().unwrap());
        b1.wait(); // ensure both have read before either commits
        txn.write("balance", (val + 10).to_le_bytes().to_vec())
            .unwrap();
        txn.commit().is_ok()
    });

    let h2 = thread::spawn(move || {
        let mut txn = db2.begin();
        let val = u64::from_le_bytes(txn.read("balance").unwrap().try_into().unwrap());
        b2.wait(); // ensure both have read before either commits
        txn.write("balance", (val + 10).to_le_bytes().to_vec())
            .unwrap();
        txn.commit().is_ok()
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

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
