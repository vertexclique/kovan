use kovan_stm::Stm;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_basic_transaction() {
    let stm = Stm::new();
    let var = stm.tvar(10);

    let result = stm.atomically(|tx| {
        let val = tx.load(&var)?;
        tx.store(&var, val + 5)?;
        Ok(val)
    });

    assert_eq!(result, 10);

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 15);
}

#[test]
fn test_read_your_own_writes() {
    let stm = Stm::new();
    let var = stm.tvar(10);

    stm.atomically(|tx| {
        let val1 = tx.load(&var)?;
        assert_eq!(val1, 10);

        tx.store(&var, 20)?;

        let val2 = tx.load(&var)?;
        assert_eq!(val2, 20); // Should see the uncommitted write

        tx.store(&var, 30)?;
        let val3 = tx.load(&var)?;
        assert_eq!(val3, 30);

        Ok(())
    });

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 30);
}

#[test]
fn test_multiple_vars_atomic_swap() {
    let stm = Stm::new();
    let acc1 = stm.tvar(100);
    let acc2 = stm.tvar(0);

    // Transfer 50 from acc1 to acc2
    stm.atomically(|tx| {
        let v1 = tx.load(&acc1)?;
        let v2 = tx.load(&acc2)?;

        tx.store(&acc1, v1 - 50)?;
        tx.store(&acc2, v2 + 50)?;
        Ok(())
    });

    let (v1, v2) = stm.atomically(|tx| Ok((tx.load(&acc1)?, tx.load(&acc2)?)));

    assert_eq!(v1, 50);
    assert_eq!(v2, 50);
}

#[test]
fn test_isolation() {
    let stm = Arc::new(Stm::new());
    let var = Arc::new(stm.tvar(0));

    let stm_clone = stm.clone();
    let var_clone = var.clone();

    // Start a long running transaction
    let t1 = thread::spawn(move || {
        stm_clone.atomically(|tx| {
            tx.store(&var_clone, 100)?;
            // Sleep to let the other thread read
            thread::sleep(Duration::from_millis(50));
            Ok(())
        })
    });

    // This transaction should NOT see the 100 until t1 commits
    // Actually, in snapshot isolation / MVCC, it should see the old value (0) if it started before commit.
    // Or if it starts after t1 writes but before commit, it should still see 0.
    thread::sleep(Duration::from_millis(10));
    let val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(val, 0);

    t1.join().unwrap();

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 100);
}

#[test]
fn test_conflict_retry() {
    let stm = Arc::new(Stm::new());
    let var = Arc::new(stm.tvar(0));

    let stm1 = stm.clone();
    let var1 = var.clone();

    let stm2 = stm.clone();
    let var2 = var.clone();

    // Thread 1: Increment 100 times
    let t1 = thread::spawn(move || {
        for _ in 0..100 {
            stm1.atomically(|tx| {
                let v = tx.load(&var1)?;
                tx.store(&var1, v + 1)?;
                Ok(())
            });
        }
    });

    // Thread 2: Increment 100 times
    let t2 = thread::spawn(move || {
        for _ in 0..100 {
            stm2.atomically(|tx| {
                let v = tx.load(&var2)?;
                tx.store(&var2, v + 1)?;
                Ok(())
            });
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 200);
}

#[test]
fn test_side_effects() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let stm = Arc::new(Stm::new());
    let var = Arc::new(stm.tvar(0));

    let commits = Arc::new(AtomicUsize::new(0));
    let rollbacks = Arc::new(AtomicUsize::new(0));

    // 1. Successful transaction
    let c = commits.clone();
    let r = rollbacks.clone();
    let var_clone = var.clone();
    let stm_clone = stm.clone();

    stm_clone.atomically(|tx| {
        tx.store(&var_clone, 1)?;
        let c = c.clone();
        let r = r.clone();
        tx.on_commit(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        tx.on_rollback(move || {
            r.fetch_add(1, Ordering::SeqCst);
        });
        Ok(())
    });

    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(rollbacks.load(Ordering::SeqCst), 0);

    // 2. Retry transaction (Conflict)
    // We want to force a retry.
    // T1: Reads, Sleeps, Writes. (Slow)
    // T2: Writes immediately. (Fast)
    // T1 should fail to commit, retry, and trigger rollback hook for the first attempt.

    let commits = Arc::new(AtomicUsize::new(0));
    let rollbacks = Arc::new(AtomicUsize::new(0));

    let stm_t1 = stm.clone();
    let var_t1 = var.clone();
    let c_t1 = commits.clone();
    let r_t1 = rollbacks.clone();

    let stm_t2 = stm.clone();
    let var_t2 = var.clone();

    use std::sync::atomic::AtomicBool;
    let t1_ready = Arc::new(AtomicBool::new(false));
    let t1_ready_clone = t1_ready.clone();

    let t1 = thread::spawn(move || {
        stm_t1.atomically(|tx| {
            // Read to establish read version
            let _ = tx.load(&var_t1)?;

            let c = c_t1.clone();
            let r = r_t1.clone();
            tx.on_commit(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
            tx.on_rollback(move || {
                r.fetch_add(1, Ordering::SeqCst);
            });

            // Signal that we have read and are about to sleep
            t1_ready_clone.store(true, Ordering::SeqCst);

            // Sleep to allow T2 to commit first
            thread::sleep(Duration::from_millis(200));

            tx.store(&var_t1, 100)?;
            Ok(())
        })
    });

    // Wait for T1 to be ready (it has read the var)
    while !t1_ready.load(Ordering::SeqCst) {
        thread::yield_now();
        thread::sleep(Duration::from_millis(1));
    }

    // T2 runs and commits quickly
    stm_t2.atomically(|tx| {
        tx.store(&var_t2, 200)?;
        Ok(())
    });

    t1.join().unwrap();

    // T1 should have:
    // 1. Started, read version V.
    // 2. Slept.
    // 3. T2 committed version V+1.
    // 4. T1 tried to commit. Validated read set (V < V+1). Failed.
    // 5. T1 rollback hooks ran (rollbacks = 1).
    // 6. T1 retried. Succeeded.
    // 7. T1 commit hooks ran (commits = 1).

    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "Should have 1 successful commit"
    );
    assert!(
        rollbacks.load(Ordering::SeqCst) >= 1,
        "Should have at least 1 rollback due to retry"
    );
}
