//! Transactions and the in-flight write registry from a thread-local
//! destructor that runs after the thread's exit (see kovan's
//! `tests/support/thread_exit.rs`), while another thread commits to the same
//! keys: every read returns a value some commit wrote, conflicts come back as
//! errors, and nothing panics.

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_mvcc::KovanMVCC;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use thread_exit::{after_exit, lock};

const KEYS: usize = 8;

fn key(k: usize) -> String {
    format!("key{k}")
}

/// A value of key `k`: the key, then the writer's counter.
fn value(k: usize, n: u64) -> Vec<u8> {
    format!("{}:{n}", key(k)).into_bytes()
}

/// Reads, writes, deletes and commits after the exit, racing another thread's
/// commits to the same keys.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn transactions_after_the_exit() {
    let _l = lock();
    let db = Arc::new(KovanMVCC::new());
    {
        let mut txn = db.begin();
        for k in 0..KEYS {
            txn.write(key(k), value(k, 0)).unwrap();
        }
        txn.commit().unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let churner = {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        thread::spawn(move || {
            let mut n = 1;
            while !stop.load(Ordering::Relaxed) {
                let mut txn = db.begin();
                let k = n as usize % KEYS;
                if txn.write(key(k), value(k, n)).is_ok() {
                    let _ = txn.commit();
                }
                n += 1;
            }
        })
    };
    let d = Arc::clone(&db);
    let (well_formed, committed) = after_exit(move || {
        let mut well_formed = true;
        let mut committed = 0;
        for round in 0..200u64 {
            let mut txn = d.begin();
            for k in 0..KEYS {
                if let Ok(Some(v)) = txn.read(key(k)) {
                    well_formed &= v.starts_with(key(k).as_bytes());
                }
            }
            let k = round as usize % KEYS;
            let written = if round % 7 == 0 {
                txn.delete(key(k))
            } else {
                txn.write(key(k), value(k, 1_000_000 + round))
            };
            if written.is_ok() && txn.commit().is_ok() {
                committed += 1;
            }
            let registry = d.inflight_writes();
            registry.register(u128::from(round) + 1, round + 1);
            let _ = registry.min_ts();
            registry.unregister(u128::from(round) + 1);
        }
        (well_formed, committed)
    });
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();
    assert!(
        well_formed,
        "a read after the thread's exit returned a value no commit wrote"
    );
    assert!(
        committed > 0,
        "no transaction after the thread's exit committed"
    );
    let txn = db.begin();
    for k in 0..KEYS {
        if let Some(v) = txn.read(key(k)).unwrap() {
            assert!(v.starts_with(key(k).as_bytes()));
        }
    }
}

/// A database dropped after the exit, with committed and open transactions
/// behind it, drops without a panic.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_database_dropped_after_the_exit() {
    let _l = lock();
    let db = KovanMVCC::new();
    let mut txn = db.begin();
    txn.write("a", b"1".to_vec()).unwrap();
    txn.commit().unwrap();
    after_exit(move || {
        let mut open = db.begin();
        open.write("b", b"2".to_vec()).unwrap();
        drop(open);
        drop(db);
    });
}
