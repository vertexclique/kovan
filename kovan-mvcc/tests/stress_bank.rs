use kovan_mvcc::KovanMVCC;
use std::sync::Arc;
use std::thread;
use rand::Rng;

const ACCOUNTS: usize = 10;
const INITIAL_BALANCE: u64 = 1000;
const TRANSFERS: usize = 2000;
const THREADS: usize = 8;

#[test]
fn test_bank_consistency() {
    let db = Arc::new(KovanMVCC::new());

    // 1. Initialize Accounts
    {
        let mut t = db.begin();
        for i in 0..ACCOUNTS {
            let key = format!("acc_{}", i);
            let val = INITIAL_BALANCE.to_le_bytes().to_vec();
            t.write(&key, val).unwrap();
        }
        t.commit().unwrap();
    }

    // 2. Run Concurrent Transfers
    let mut handles = vec![];

    for _ in 0..THREADS {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for _ in 0..TRANSFERS {
                let from = rng.gen_range(0..ACCOUNTS);
                let to = rng.gen_range(0..ACCOUNTS);
                if from == to { continue; }

                let k_from = format!("acc_{}", from);
                let k_to = format!("acc_{}", to);
                let amount = 10;

                // Retry loop for conflicts
                loop {
                    let mut txn = db_clone.begin();
                    
                    // Read phase
                    let b_from_bytes = txn.read(&k_from).unwrap();
                    let b_to_bytes = txn.read(&k_to).unwrap();

                    let b_from = u64::from_le_bytes(b_from_bytes.try_into().unwrap());
                    let b_to = u64::from_le_bytes(b_to_bytes.try_into().unwrap());

                    if b_from < amount {
                        break; // Not enough funds, skip
                    }

                    // Write phase
                    let new_from = b_from - amount;
                    let new_to = b_to + amount;

                    // Note: Order of writes matters for deadlocks in locking systems,
                    // but here we use pessimistic intent writing. 
                    // If we fail to write, we abort and retry.
                    if txn.write(&k_from, new_from.to_le_bytes().to_vec()).is_ok() {
                        if txn.write(&k_to, new_to.to_le_bytes().to_vec()).is_ok() {
                            if txn.commit().is_ok() {
                                break; // Success
                            }
                        }
                    }
                    // If we get here, conflict happened. Retry.
                    // Backoff slightly
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 3. Verify Total Balance Invariant
    let txn = db.begin();
    let mut total = 0;
    for i in 0..ACCOUNTS {
        let key = format!("acc_{}", i);
        let val = txn.read(&key).unwrap();
        let balance = u64::from_le_bytes(val.try_into().unwrap());
        eprintln!("[FINAL] {} = {}", key, balance);
        total += balance;
    }

    eprintln!("[FINAL] total={} expected={}", total, (ACCOUNTS as u64) * INITIAL_BALANCE);
    assert_eq!(total, (ACCOUNTS as u64) * INITIAL_BALANCE, "Money was created or destroyed!");
}