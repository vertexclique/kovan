//! A database holds no more memory after a long run than after a short one
//! once its old versions are collected (see kovan's
//! `tests/support/steady_memory.rs`): rounds of transactions from many
//! threads, and after a thread's exit, then a GC at the watermark, leave the
//! bytes held at a quiet point where they were.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;
#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_mvcc::KovanMVCC;
use std::sync::Arc;
use std::thread;
use steady_memory::assert_flat;

const THREADS: usize = 8;
const WARM: usize = 10;
/// Rounds after the warm-up; with `recycle` the run is longer, so that what
/// a round would leave behind outgrows the cache's own swing.
const ROUNDS: usize = if cfg!(feature = "recycle") { 100 } else { 30 };
/// What the run may add over the warm-up's highest mark: allocator noise
/// without `recycle`; with it, the magazine cache's working set (see
/// kovan's `tests/steady_memory.rs`).
const SLACK: isize = if cfg!(feature = "recycle") {
    4 << 20
} else {
    64 << 10
};

fn on_threads(work: impl Fn(usize) + Send + Sync + 'static) {
    let work = Arc::new(work);
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let work = Arc::clone(&work);
            thread::spawn(move || work(t))
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

const KEYS: usize = 16;

/// Collects every key's versions below the watermark.
fn collect(db: &KovanMVCC) {
    let watermark = db.gc_watermark();
    for k in 0..KEYS {
        let key = format!("key{k}");
        db.storage().gc_writes(key.as_bytes(), watermark);
        db.storage().gc_data(key.as_bytes(), watermark);
    }
}

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn memory_stays_flat_over_a_long_run() {
    let db = Arc::new(KovanMVCC::new());
    let d = Arc::clone(&db);
    assert_flat(
        "transactions across threads, then a GC",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            let d2 = Arc::clone(&d);
            on_threads(move |t| {
                for i in 0..200 {
                    let mut txn = d2.begin();
                    let k = (t + i) % KEYS;
                    if txn.write(format!("key{k}"), vec![t as u8; 48]).is_ok() {
                        let _ = txn.commit();
                    }
                    let reader = d2.begin();
                    let _ = reader.read(format!("key{}", i % KEYS));
                }
            });
            collect(&d);
        },
    );
    let d = Arc::clone(&db);
    assert_flat(
        "transactions after the thread's exit, then a GC",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            for t in 0..8 {
                let d2 = Arc::clone(&d);
                thread_exit::after_exit(move || {
                    for i in 0..32 {
                        let mut txn = d2.begin();
                        if txn
                            .write(format!("key{}", (t + i) % KEYS), vec![6u8; 48])
                            .is_ok()
                        {
                            let _ = txn.commit();
                        }
                    }
                });
            }
            collect(&d);
        },
    );
}
