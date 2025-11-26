use kovan_mvcc::KovanMVCC;
use rand::Rng;
use std::sync::Arc;
use std::thread;

const ACCOUNTS: usize = 10;
const INITIAL_BALANCE: u64 = 10000;
const TRANSFERS: usize = 2000;
const THREADS: usize = 10;

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
            let mut rng = rand::rng();
            for _ in 0..TRANSFERS {
                let from = rng.random_range(0..ACCOUNTS);
                let to = rng.random_range(0..ACCOUNTS);
                if from == to {
                    continue;
                }

                let k_from = format!("acc_{}", from);
                let k_to = format!("acc_{}", to);
                let amount = 10;

                // Retry loop for conflicts
                loop {
                    let mut txn = db_clone.begin();

                    // Read phase
                    // Read phase
                    let b_from_bytes = match txn.read(&k_from) {
                        Some(v) => v,
                        None => {
                            thread::yield_now();
                            continue;
                        }
                    };
                    let b_to_bytes = match txn.read(&k_to) {
                        Some(v) => v,
                        None => {
                            thread::yield_now();
                            continue;
                        }
                    };

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
                    // Backoff with random jitter to avoid livelock
                    let sleep_ms = rng.random_range(1..5);
                    thread::sleep(std::time::Duration::from_millis(sleep_ms));
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

    eprintln!(
        "[FINAL] total={} expected={}",
        total,
        (ACCOUNTS as u64) * INITIAL_BALANCE
    );
    assert_eq!(
        total,
        (ACCOUNTS as u64) * INITIAL_BALANCE,
        "Money was created or destroyed!"
    );
}
