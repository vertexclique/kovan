use kovan_stm::Stm;
use std::sync::Arc;
use std::thread;

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_counter() {
    let stm = Arc::new(Stm::new());
    let var = Arc::new(stm.tvar(0i64));

    let threads = 8;
    let increments = 100;

    let mut handles = vec![];
    for _ in 0..threads {
        let stm = stm.clone();
        let var = var.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..increments {
                stm.atomically(|tx| {
                    let v = tx.load(&var)?;
                    tx.store(&var, v + 1)?;
                    Ok(())
                });
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(val, threads * increments);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_bank_transfer() {
    let stm = Arc::new(Stm::new());
    let num_accounts = 10;
    let accounts: Vec<_> = (0..num_accounts)
        .map(|_| Arc::new(stm.tvar(1000i64)))
        .collect();

    let threads = 2;
    let transfers = 50;

    let mut handles = vec![];
    for t in 0..threads {
        let stm = stm.clone();
        let accounts = accounts.clone();
        handles.push(thread::spawn(move || {
            for i in 0..transfers {
                let from = (t * 5 + i) % num_accounts;
                let to = (t * 5 + i + 1) % num_accounts;
                let amount = 1;

                stm.atomically(|tx| {
                    let from_bal = tx.load(&accounts[from])?;
                    let to_bal = tx.load(&accounts[to])?;
                    if from_bal >= amount {
                        tx.store(&accounts[from], from_bal - amount)?;
                        tx.store(&accounts[to], to_bal + amount)?;
                    }
                    Ok(())
                });
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Total money should be conserved
    let total: i64 = stm.atomically(|tx| {
        let mut sum = 0;
        for acc in &accounts {
            sum += tx.load(acc)?;
        }
        Ok(sum)
    });

    assert_eq!(
        total,
        num_accounts as i64 * 1000,
        "money not conserved: total = {}",
        total
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_multi_var_swap() {
    let stm = Arc::new(Stm::new());
    let a = Arc::new(stm.tvar(1i64));
    let b = Arc::new(stm.tvar(2i64));

    let threads = 4;
    let swaps = 100;

    let mut handles = vec![];
    for _ in 0..threads {
        let stm = stm.clone();
        let a = a.clone();
        let b = b.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..swaps {
                stm.atomically(|tx| {
                    let va = tx.load(&a)?;
                    let vb = tx.load(&b)?;
                    tx.store(&a, vb)?;
                    tx.store(&b, va)?;
                    Ok(())
                });
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let (va, vb) = stm.atomically(|tx| Ok((tx.load(&a)?, tx.load(&b)?)));
    // After even number of swaps per thread, values should be 1,2 or 2,1
    assert!(
        (va == 1 && vb == 2) || (va == 2 && vb == 1),
        "unexpected: a={}, b={}",
        va,
        vb
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_read_only_transactions() {
    let stm = Arc::new(Stm::new());
    let var = Arc::new(stm.tvar(42i64));

    let mut handles = vec![];
    for _ in 0..8 {
        let stm = stm.clone();
        let var = var.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let v = stm.atomically(|tx| tx.load(&var));
                assert_eq!(v, 42);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_transaction_return_value() {
    let stm = Stm::new();
    let var = stm.tvar(10i64);

    let result = stm.atomically(|tx| {
        let v = tx.load(&var)?;
        tx.store(&var, v * 2)?;
        Ok(v)
    });

    assert_eq!(result, 10);

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 20);
}
