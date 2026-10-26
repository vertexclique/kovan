//! Every value that enters a transactional variable leaves it exactly once,
//! under racing transactions that conflict, retry and roll back: values
//! committed, replaced by a later store in the same transaction, dropped
//! with a rolled-back attempt, or still held when the variables drop. No
//! load reads a value already dropped. Values are counted by id outside
//! their memory (see kovan's `tests/support/thread_exit.rs`).

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_stm::{Stm, StmError};
use std::sync::Arc;
use std::thread;
use thread_exit::{Val, alive, balanced, live, lock, take_dead_reads, take_double_drops};

#[test]
#[cfg_attr(miri, ignore)] // threads
fn racing_transactions_drop_each_value_once() {
    let _l = lock();
    take_double_drops();
    take_dead_reads();
    let base = live();
    let stm = Arc::new(Stm::new());
    let vars: Arc<Vec<_>> = Arc::new((0..4).map(|_| stm.tvar(Val::new())).collect());
    let handles: Vec<_> = (0..8usize)
        .map(|t| {
            let (stm, vars) = (Arc::clone(&stm), Arc::clone(&vars));
            thread::spawn(move || {
                for i in 0..1_000usize {
                    let (a, b) = (&vars[(t + i) % 4], &vars[(t + i + 1) % 4]);
                    let mut attempt = 0;
                    stm.atomically(|tx| {
                        attempt += 1;
                        let seen = tx.load(a)?;
                        assert!(alive(seen.id), "a load gave a dropped value");
                        // Two stores to one variable: the first value drops.
                        tx.store(b, Val::new())?;
                        tx.store(b, Val::new())?;
                        tx.store(a, seen)?;
                        if i % 7 == 0 && attempt == 1 {
                            // Roll this attempt back: its values drop.
                            return Err(StmError::Retry);
                        }
                        Ok(())
                    });
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    drop(vars);
    balanced(base, "racing transactions");
    assert_eq!(take_dead_reads(), 0, "a dropped value was read");
}
