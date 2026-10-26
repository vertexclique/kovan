//! The channels hold no more memory after a long run than after a short one
//! (see kovan's `tests/support/steady_memory.rs`): rounds of traffic across
//! threads, of channels made and dropped (`never` among them), and of use
//! after a thread's exit leave the bytes held at a quiet point where they
//! were.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;
#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use futures::FutureExt;
use kovan_channel::{after, bounded, never, tick, unbounded};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
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
    // One quiet channel of each kind for the whole run, as a channel nobody
    // sends on for hours is: a wait on it that ends without a wakeup (a
    // receive that times out, a receive or a send dropped while it waited)
    // must not leave anything behind on it.
    let (_quiet_tx, quiet) = unbounded::<Vec<u8>>();
    let (full_tx, full_rx) = bounded::<Vec<u8>>(1);
    full_tx.send(vec![0; 48]);
    let (_idle_tx, idle_rx) = bounded::<Vec<u8>>(1);
    assert_flat(
        "waits on quiet channels that end without a wakeup",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            for _ in 0..500 {
                assert!(matches!(
                    quiet.recv_deadline(Instant::now()),
                    kovan_channel::RecvDeadline::Timeout
                ));
                assert!(quiet.recv_async().now_or_never().is_none());
                assert!(
                    idle_rx.recv_deadline(Instant::now()) == kovan_channel::RecvDeadline::Timeout
                );
                assert!(idle_rx.recv_async().now_or_never().is_none());
                assert!(full_tx.send_async(vec![1; 48]).now_or_never().is_none());
            }
        },
    );
    drop(full_rx);
    // A sender that outlives every receiver, sending on: each value is
    // dropped as it is sent.
    let (lone_tx, lone_rx) = unbounded::<Vec<u8>>();
    drop(lone_rx);
    assert_flat(
        "sends once every receiver is gone",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            for _ in 0..1_000 {
                lone_tx.send(vec![7u8; 48]);
            }
        },
    );
    assert_flat(
        "after and tick made, received and dropped",
        WARM,
        ROUNDS,
        SLACK,
        || {
            for _ in 0..200 {
                let r = after(Duration::ZERO);
                assert!(r.recv().is_some());
                let r = tick(Duration::ZERO);
                assert!(r.recv().is_some());
                assert!(r.try_recv().is_some());
                drop(after(Duration::from_secs(3600)));
                drop(tick(Duration::from_secs(3600)));
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
