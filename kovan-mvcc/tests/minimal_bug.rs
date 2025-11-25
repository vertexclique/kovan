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

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    // Both transactions try to add 10 to the balance
    let h1 = thread::spawn(move || {
        b1.wait();
        let mut txn = db1.begin();
        let val = u64::from_le_bytes(txn.read("balance").unwrap().try_into().unwrap());
        eprintln!("[T1] Read: {}", val);
        let result = txn.write("balance", (val + 10).to_le_bytes().to_vec());
        eprintln!("[T1] Write result: {:?}", result);
        if result.is_ok() {
            let commit_result = txn.commit();
            eprintln!("[T1] Commit result: {:?}", commit_result);
            commit_result.is_ok()
        } else {
            false
        }
    });

    let h2 = thread::spawn(move || {
        b2.wait();
        let mut txn = db2.begin();
        let val = u64::from_le_bytes(txn.read("balance").unwrap().try_into().unwrap());
        eprintln!("[T2] Read: {}", val);
        let result = txn.write("balance", (val + 10).to_le_bytes().to_vec());
        eprintln!("[T2] Write result: {:?}", result);
        if result.is_ok() {
            let commit_result = txn.commit();
            eprintln!("[T2] Commit result: {:?}", commit_result);
            commit_result.is_ok()
        } else {
            false
        }
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    eprintln!("[RESULT] T1 success: {}, T2 success: {}", r1, r2);

    // Both should NOT succeed - one must abort
    assert!(!(r1 && r2), "Both transactions succeeded - lost update!");

    // Check final balance
    let txn = db.begin();
    let final_val = u64::from_le_bytes(txn.read("balance").unwrap().try_into().unwrap());
    eprintln!("[FINAL] balance={}", final_val);

    if r1 && !r2 {
        assert_eq!(final_val, 110, "T1 succeeded, expected 110");
    } else if !r1 && r2 {
        assert_eq!(final_val, 110, "T2 succeeded, expected 110");
    } else {
        // Neither succeeded
        assert_eq!(final_val, 100, "Both aborted, expected 100");
    }
}
