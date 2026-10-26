//! Transactions from a thread-local destructor that runs after the thread's
//! exit (see kovan's `tests/support/thread_exit.rs`), while another thread
//! commits to the same variable: no load clones a freed value, nothing
//! panics, and every value is freed in the end.

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan::flush;
use kovan_stm::Stm;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use thread_exit::{
    Val, after_exit, alive, freed_down_to, held_across_retire, live, lock, take_dead_reads,
};

/// Loads and stores in transactions after the exit, racing another thread's
/// commits to the same variable.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn transactions_after_the_exit() {
    let _l = lock();
    let base = live();
    take_dead_reads();
    let stm = Arc::new(Stm::new());
    let var = Arc::new(stm.tvar(Val::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let churner = {
        let (stm, var, stop) = (Arc::clone(&stm), Arc::clone(&var), Arc::clone(&stop));
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                stm.atomically(|tx| tx.store(&var, Val::new()));
                flush();
            }
        })
    };
    let (s, v) = (Arc::clone(&stm), Arc::clone(&var));
    let checks = after_exit(move || {
        let mut ok = Vec::new();
        for _ in 0..300 {
            let seen = s.atomically(|tx| {
                let current = tx.load(&v)?;
                tx.store(&v, Val::new())?;
                Ok(alive(current.id))
            });
            ok.push(seen);
        }
        flush();
        ok
    });
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();
    assert_eq!(
        take_dead_reads(),
        0,
        "a load after the thread's exit cloned a freed value"
    );
    assert!(
        checks.iter().all(|&ok| ok),
        "a load after the thread's exit handed out a dropped value"
    );
    drop(var);
    drop(stm);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// A variable dropped after the exit frees its value.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_variable_dropped_after_the_exit_is_freed() {
    let _l = lock();
    let base = live();
    let stm = Stm::new();
    let var = stm.tvar(Val::new());
    // `var` and `stm` are the closure's: they drop as it ends, after the
    // thread's exit.
    after_exit(move || {
        stm.atomically(|tx| tx.store(&var, Val::new()));
    });
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// A load after the exit, stopped inside its clone of the variable's value
/// while this thread commits a new value and flushes: the value stays alive.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_load_after_the_exit_holds_its_value() {
    let _l = lock();
    let base = live();
    let stm = Arc::new(Stm::new());
    let first = Val::new();
    let id = first.id;
    let var = Arc::new(stm.tvar(first));
    let (s, v) = (Arc::clone(&stm), Arc::clone(&var));
    let held = held_across_retire(
        id,
        move || s.atomically(|tx| tx.load(&v).map(drop)),
        || stm.atomically(|tx| tx.store(&var, Val::new())),
    );
    assert!(
        held,
        "a value loaded after the thread's exit was freed under its guard"
    );
    drop(var);
    drop(stm);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}
