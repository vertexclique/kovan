//! `try_atomically` answers the error a transaction returns: the transaction
//! rolls back (its rollback hooks run, nothing it stored is published), where
//! `atomically` panics; a retry or a conflict runs it again, as `atomically`
//! does.

use kovan_stm::{Stm, StmError};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

#[test]
fn a_user_error_rolls_back_and_is_answered() {
    let stm = Stm::new();
    let var = stm.tvar(1u64);
    let rolled_back = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&rolled_back);
    let answer: Result<(), StmError> = stm.try_atomically(|tx| {
        tx.store(&var, 99)?;
        let r = Arc::clone(&r);
        tx.on_rollback(move || {
            r.fetch_add(1, Ordering::SeqCst);
        });
        Err(StmError::User("refused".into()))
    });
    assert!(matches!(answer, Err(StmError::User(ref m)) if m == "refused"));
    assert_eq!(rolled_back.load(Ordering::SeqCst), 1);
    assert_eq!(
        stm.atomically(|tx| tx.load(&var)),
        1,
        "a rolled-back store was published"
    );
    // `atomically` turns the same error into a panic.
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        stm.atomically(|_| Err::<(), _>(StmError::User("refused".into())))
    }));
    assert!(panicked.is_err());
}

#[test]
fn a_retry_runs_the_transaction_again() {
    let stm = Stm::new();
    let var = stm.tvar(0u64);
    let mut attempts = 0;
    let answer = stm.try_atomically(|tx| {
        attempts += 1;
        if attempts < 3 {
            return Err(StmError::Retry);
        }
        tx.store(&var, 7)?;
        Ok(attempts)
    });
    assert_eq!(answer.ok(), Some(3));
    assert_eq!(stm.atomically(|tx| tx.load(&var)), 7);
}

/// Conflicting transactions on one variable all commit, each once: the
/// failed commits back off and retry.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn conflicting_transactions_all_commit() {
    let stm = Arc::new(Stm::new());
    let counter = Arc::new(stm.tvar(0u64));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (stm, counter) = (Arc::clone(&stm), Arc::clone(&counter));
            thread::spawn(move || {
                for _ in 0..2_000 {
                    stm.try_atomically(|tx| {
                        let n = tx.load(&counter)?;
                        tx.store(&counter, n + 1)
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(stm.atomically(|tx| tx.load(&counter)), 16_000);
}
