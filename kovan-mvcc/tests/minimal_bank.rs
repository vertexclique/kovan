use kovan_mvcc::KovanMVCC;
use rand::Rng;
use std::sync::Arc;
use std::thread; // Add this import

#[test]
fn test_simple_concurrent_transfer() {
    let db = Arc::new(KovanMVCC::new());

    let num_accounts = 3; // Changed to 3 accounts
    // Initialize accounts
    {
        let mut txn = db.begin();
        for i in 0..num_accounts {
            txn.write(&format!("acc_{}", i), 1000u64.to_le_bytes().to_vec())
                .unwrap();
        }
        txn.commit().unwrap();
    }

    // Multiple threads: random transfers
    let mut handles = vec![];
    let num_threads = 4;
    let num_iterations = 1000;

    for _ in 0..num_threads {
        // Changed loop variable to _
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            let mut rng = rand::rng(); // Initialize random number generator
            for _ in 0..num_iterations {
                // Changed loop variable to _
                let from_idx = rng.random_range(0..num_accounts);
                let to_idx = rng.random_range(0..num_accounts);
                if from_idx == to_idx {
                    continue;
                } // Skip if source and destination are the same

                let k_from = format!("acc_{}", from_idx);
                let k_to = format!("acc_{}", to_idx);

                loop {
                    let mut txn = db_clone.begin();
                    // Read phase
                    let b_from_bytes = txn.read(&k_from).unwrap();
                    let b_to_bytes = txn.read(&k_to).unwrap();

                    let b_from = u64::from_le_bytes(b_from_bytes.try_into().unwrap());
                    let b_to = u64::from_le_bytes(b_to_bytes.try_into().unwrap());

                    if b_from < 10 {
                        // Not enough funds, just commit (read-only) or abort
                        // For simplicity, just break loop and try next transfer
                        break;
                    }

                    // Write phase
                    if txn
                        .write(&k_from, (b_from - 10).to_le_bytes().to_vec())
                        .is_err()
                    {
                        continue;
                    }
                    if txn
                        .write(&k_to, (b_to + 10).to_le_bytes().to_vec())
                        .is_err()
                    {
                        continue;
                    }

                    if txn.commit().is_ok() {
                        break;
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Check total balance
    let mut total = 0;
    let txn = db.begin();
    for i in 0..num_accounts {
        let val = txn.read(&format!("acc_{}", i)).unwrap();
        total += u64::from_le_bytes(val.try_into().unwrap());
    }

    assert_eq!(
        total,
        1000 * num_accounts as u64,
        "Money was created or destroyed!"
    );
}
