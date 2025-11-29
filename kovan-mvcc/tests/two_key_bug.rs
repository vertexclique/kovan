use kovan_mvcc::KovanMVCC;
use std::sync::Arc;
use std::thread;

#[test]
fn test_two_key_lost_update() {
    let db = Arc::new(KovanMVCC::new());

    // Initialize two accounts
    {
        let mut t = db.begin();
        t.write("acc_0", 1000u64.to_le_bytes().to_vec()).unwrap();
        t.write("acc_1", 1000u64.to_le_bytes().to_vec()).unwrap();
        t.commit().unwrap();
    }

    let db1 = db.clone();
    let db2 = db.clone();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    // T1: Transfer 10 from acc_0 to acc_1
    let h1 = thread::spawn(move || {
        b1.wait();
        let mut txn = db1.begin();
        let v0 = u64::from_le_bytes(txn.read("acc_0").unwrap().try_into().unwrap());
        let v1 = u64::from_le_bytes(txn.read("acc_1").unwrap().try_into().unwrap());
        eprintln!("[T1] Read: acc_0={}, acc_1={}", v0, v1);

        let w0 = txn.write("acc_0", (v0 - 10).to_le_bytes().to_vec());
        eprintln!("[T1] Write acc_0={} result: {:?}", v0 - 10, w0);

        if w0.is_ok() {
            let w1 = txn.write("acc_1", (v1 + 10).to_le_bytes().to_vec());
            eprintln!("[T1] Write acc_1={} result: {:?}", v1 + 10, w1);

            if w1.is_ok() {
                let commit_result = txn.commit();
                eprintln!("[T1] Commit result: {:?}", commit_result);
                commit_result.is_ok()
            } else {
                false
            }
        } else {
            false
        }
    });

    // T2: Also transfer 10 from acc_0 to acc_1 (same operation)
    let h2 = thread::spawn(move || {
        b2.wait();
        let mut txn = db2.begin();
        let v0 = u64::from_le_bytes(txn.read("acc_0").unwrap().try_into().unwrap());
        let v1 = u64::from_le_bytes(txn.read("acc_1").unwrap().try_into().unwrap());
        eprintln!("[T2] Read: acc_0={}, acc_1={}", v0, v1);

        let w0 = txn.write("acc_0", (v0 - 10).to_le_bytes().to_vec());
        eprintln!("[T2] Write acc_0={} result: {:?}", v0 - 10, w0);

        if w0.is_ok() {
            let w1 = txn.write("acc_1", (v1 + 10).to_le_bytes().to_vec());
            eprintln!("[T2] Write acc_1={} result: {:?}", v1 + 10, w1);

            if w1.is_ok() {
                let commit_result = txn.commit();
                eprintln!("[T2] Commit result: {:?}", commit_result);
                commit_result.is_ok()
            } else {
                false
            }
        } else {
            false
        }
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    eprintln!("[RESULT] T1 success: {}, T2 success: {}", r1, r2);

    // Check final balance
    let txn = db.begin();
    let v0 = u64::from_le_bytes(txn.read("acc_0").unwrap().try_into().unwrap());
    let v1 = u64::from_le_bytes(txn.read("acc_1").unwrap().try_into().unwrap());
    eprintln!("[FINAL] acc_0={}, acc_1={}, total={}", v0, v1, v0 + v1);

    assert_eq!(v0 + v1, 2000, "Money was created or destroyed!");
}
