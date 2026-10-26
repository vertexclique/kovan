//! The channels hold no more memory after a long run than after a short one
//! (see kovan's `tests/support/steady_memory.rs`): rounds of traffic across
//! threads, of channels made and dropped (`never` among them), and of use
//! after a thread's exit leave the bytes held at a quiet point where they
//! were.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;
#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_channel::{bounded, never, unbounded};
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

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn memory_stays_flat_over_a_long_run() {
    assert_flat(
        "unbounded traffic across threads",
        WARM,
        ROUNDS,
        SLACK,
        || {
            let (tx, rx) = unbounded::<Vec<u8>>();
            let consumers: Vec<_> = (0..4)
                .map(|_| {
                    let rx = rx.clone();
                    thread::spawn(move || while rx.recv().is_some() {})
                })
                .collect();
            on_threads(move |t| {
                let tx = tx.clone();
                for _ in 0..2_000 {
                    tx.send(vec![t as u8; 48]);
                }
            });
            drop(rx);
            for c in consumers {
                c.join().unwrap();
            }
        },
    );
    assert_flat(
        "bounded traffic across threads",
        WARM,
        ROUNDS,
        SLACK,
        || {
            let (tx, rx) = bounded::<Vec<u8>>(64);
            let consumer = thread::spawn(move || while rx.recv().is_some() {});
            on_threads(move |t| {
                let tx = tx.clone();
                for _ in 0..1_000 {
                    tx.send(vec![t as u8; 48]);
                }
            });
            consumer.join().unwrap();
        },
    );
    assert_flat(
        "never() channels made and dropped",
        WARM,
        ROUNDS,
        SLACK,
        || {
            for _ in 0..1_000 {
                let r = never::<Vec<u8>>();
                assert!(r.try_recv().is_none());
            }
        },
    );
    assert_flat(
        "channels used after the thread's exit",
        WARM,
        ROUNDS,
        SLACK,
        || {
            for _ in 0..8 {
                thread_exit::after_exit(|| {
                    let (tx, rx) = unbounded::<Vec<u8>>();
                    for _ in 0..64 {
                        tx.send(vec![4u8; 48]);
                    }
                    drop(tx);
                    while rx.recv().is_some() {}
                });
            }
        },
    );
}
