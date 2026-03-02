use kovan_mvcc::{KovanMVCC, MvccError};
use std::sync::Arc;
use std::thread;

#[test]
#[cfg_attr(miri, ignore)]
fn test_simple_conflict() {
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

    // Start both transactions at the same read_ts
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();

    // T1: Transfer 10 from acc_0 to acc_1
    let h1 = thread::spawn(move || {
        barrier1.wait(); // Wait for both threads to be ready
        let mut txn = db1.begin();
        let b0 = u64::from_le_bytes(txn.read("acc_0").unwrap().try_into().unwrap());
        let b1 = u64::from_le_bytes(txn.read("acc_1").unwrap().try_into().unwrap());
        eprintln!("[T1] Read: acc_0={}, acc_1={}", b0, b1);

        let write0_result = txn.write("acc_0", (b0 - 10).to_le_bytes().to_vec());
        eprintln!("[T1] Write acc_0 result: {:?}", write0_result);

        if write0_result.is_ok() {
            let write1_result = txn.write("acc_1", (b1 + 10).to_le_bytes().to_vec());
            eprintln!("[T1] Write acc_1 result: {:?}", write1_result);

            if write1_result.is_ok() {
                let commit_result = txn.commit();
                eprintln!("[T1] Commit result: {:?}", commit_result);
                commit_result
            } else {
                Err(MvccError::StorageError("write1 failed".to_string()))
            }
        } else {
            Err(MvccError::StorageError("write0 failed".to_string()))
        }
    });

    // T2: Transfer 10 from acc_0 to acc_1 (same transfer)
    let h2 = thread::spawn(move || {
        barrier2.wait(); // Wait for both threads to be ready
        let mut txn = db2.begin();
        let b0 = u64::from_le_bytes(txn.read("acc_0").unwrap().try_into().unwrap());
        let b1 = u64::from_le_bytes(txn.read("acc_1").unwrap().try_into().unwrap());
        eprintln!("[T2] Read: acc_0={}, acc_1={}", b0, b1);

        let write0_result = txn.write("acc_0", (b0 - 10).to_le_bytes().to_vec());
        eprintln!("[T2] Write acc_0 result: {:?}", write0_result);

        if write0_result.is_ok() {
            let write1_result = txn.write("acc_1", (b1 + 10).to_le_bytes().to_vec());
            eprintln!("[T2] Write acc_1 result: {:?}", write1_result);

            if write1_result.is_ok() {
                let commit_result = txn.commit();
                eprintln!("[T2] Commit result: {:?}", commit_result);
                commit_result
            } else {
                Err(MvccError::StorageError("write1 failed".to_string()))
            }
        } else {
            Err(MvccError::StorageError("write0 failed".to_string()))
        }
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    eprintln!("[RESULT] T1: {:?}, T2: {:?}", r1, r2);

    // Check final balances
    let txn = db.begin();
    let b0 = u64::from_le_bytes(txn.read("acc_0").unwrap().try_into().unwrap());
    let b1 = u64::from_le_bytes(txn.read("acc_1").unwrap().try_into().unwrap());

    eprintln!("[FINAL] acc_0={}, acc_1={}, total={}", b0, b1, b0 + b1);

    assert_eq!(b0 + b1, 2000, "Money was created or destroyed!");
}
