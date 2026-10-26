//! Transactions hold no more memory after a long run than after a short one
//! (see kovan's `tests/support/steady_memory.rs`): rounds of commits from
//! many threads, and after a thread's exit, leave the bytes held at a quiet
//! point where they were.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;
#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_stm::Stm;
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
    let stm = Arc::new(Stm::new());
    let var = Arc::new(stm.tvar(vec![0u8; 48]));
    let (s, v) = (Arc::clone(&stm), Arc::clone(&var));
    assert_flat("commits across threads", WARM, ROUNDS, SLACK, move || {
        let (s, v) = (Arc::clone(&s), Arc::clone(&v));
        on_threads(move |t| {
            for _ in 0..1_000 {
                s.atomically(|tx| {
                    let current = tx.load(&v)?;
                    tx.store(&v, vec![t as u8; current.len()])
                });
            }
        });
    });
    let (s, v) = (Arc::clone(&stm), Arc::clone(&var));
    assert_flat(
        "commits after the thread's exit",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            for _ in 0..8 {
                let (s, v) = (Arc::clone(&s), Arc::clone(&v));
                thread_exit::after_exit(move || {
                    for _ in 0..32 {
                        s.atomically(|tx| tx.store(&v, vec![5u8; 48]));
                    }
                });
            }
        },
    );
}
