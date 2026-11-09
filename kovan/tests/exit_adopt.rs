//! What a thread's exit retired is placed, padded, whatever other threads
//! reserve, so a program whose threads come and go and that never calls
//! `flush` holds a bounded number of retired values, however many threads
//! came and went: each exit's batch waits for the readers' slots that took a
//! node of it, which the readers' drops traverse. Values are counted by id
//! outside their memory (see `support/thread_exit.rs`).

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
fn exits_place_what_they_retired_while_readers_run() {
    let _l = lock();
    let base = live();
    let shared = Arc::new(Atom::new(Val::new()));
    let watched = Arc::new(Atom::new(0u64));
    let stop = Arc::new(AtomicBool::new(false));
    // Readers pin and unpin all the time and never flush: a batch placed
    // in a reader's slot is traversed at that section's end.
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
    // No flush anywhere. Each exit placed its batch of three (padded to the
    // readers' slots) and the readers' drops free them as they go. Once the
    // exits are over, what is left does not grow with their number: the
    // batches placed in the four readers' current sections, and the live
    // value.
    let end = Instant::now() + Duration::from_secs(60);
    let bound = 4 * 3 + 1;
    while live() - base > bound && Instant::now() < end {
        thread::yield_now();
    }
    let held = live() - base;
    stop.store(true, Ordering::SeqCst);
    for r in readers {
        r.join().unwrap();
    }
    assert!(
        held <= bound,
        "{held} values held after {EXITS} exits with no flush (bound {bound})"
    );
    drop(shared);
    kovan::flush();
    assert!(
        thread_exit::freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}
