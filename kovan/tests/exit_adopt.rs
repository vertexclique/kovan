//! What a thread's exit cannot place is parked on its ID, and the next exit
//! adopts it, not only a `flush`: a program whose threads come and go and that
//! never calls `flush` holds a bounded number of retired values, however many
//! threads came and went. Parked batches were adopted by `flush` alone, so
//! such a program kept every exiting thread's batch for good. Values are
//! counted by id outside their memory (see `support/thread_exit.rs`).

#[path = "support/thread_exit.rs"]
mod thread_exit;

use kovan::Atom;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use thread_exit::{Val, live, lock};

#[test]
#[cfg_attr(miri, ignore)] // threads
fn exits_adopt_what_earlier_exits_parked() {
    let _l = lock();
    let base = live();
    let shared = Arc::new(Atom::new(Val::new()));
    let watched = Arc::new(Atom::new(0u64));
    let stop = Arc::new(AtomicBool::new(false));
    // Readers keep their slots eligible (a batch needs more nodes than the
    // eligible slots to be placed) and never flush.
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (watched, stop) = (Arc::clone(&watched), Arc::clone(&stop));
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = *watched.load();
                }
            })
        })
        .collect();
    const EXITS: usize = 400;
    for _ in 0..EXITS {
        let shared = Arc::clone(&shared);
        thread::spawn(move || {
            for _ in 0..3 {
                shared.store(Val::new());
            }
        })
        .join()
        .unwrap();
    }
    // No flush anywhere. The exits placed what they retired in the readers'
    // slots, which free it as they pin again; at most the last exit's batch
    // stays parked. Each exit kept its own batch of three for good. Once the
    // exits are over, what is left does not grow with their number: each
    // reader's free-list cache, which keeps the batches of up to MAX_CACHE
    // (12) traversals until its next traversal frees them (a traversal
    // takes one exit's batch of three, as every exit that places one
    // advances the epoch), at most one parked batch, and the live value.
    let end = Instant::now() + Duration::from_secs(60);
    let bound = 4 * (12 + 1) * 3 + 3 + 1;
    while live() - base > bound && Instant::now() < end {
        thread::yield_now();
    }
    let held = live() - base;
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    assert!(
        held <= bound,
        "{held} values retired by {EXITS} exited threads are still held (at most {bound} expected)"
    );
}
