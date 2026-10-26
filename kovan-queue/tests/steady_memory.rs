//! `SegQueue` holds no more memory after a long run than after a short one
//! (see kovan's `tests/support/steady_memory.rs`): rounds that push and pop
//! from many threads, and after a thread's exit, leave the bytes held at a
//! quiet point where they were.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;
#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_queue::seg_queue::SegQueue;
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
    let queue: Arc<SegQueue<Vec<u8>>> = Arc::new(SegQueue::new());
    let q = Arc::clone(&queue);
    assert_flat(
        "SegQueue pushed and popped across threads",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            let workers = Arc::clone(&q);
            on_threads(move |t| {
                for i in 0..4_000 {
                    workers.push(vec![t as u8; 48]);
                    if i % 2 == 1 {
                        drop(workers.pop());
                        drop(workers.pop());
                    }
                }
            });
            while q.pop().is_some() {}
        },
    );
    let q = Arc::clone(&queue);
    assert_flat(
        "SegQueue used after the thread's exit",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            for _ in 0..8 {
                let q = Arc::clone(&q);
                thread_exit::after_exit(move || {
                    for _ in 0..64 {
                        q.push(vec![3u8; 48]);
                    }
                    while q.pop().is_some() {}
                });
            }
        },
    );
}
