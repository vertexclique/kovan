//! Receivers parked in `recv()` hold no retired value back: a receive pins
//! for its list operations and drops its guard before it parks, and a
//! thread with no live guard is counted by no batch. Thirty-six receivers
//! park on an empty channel; one thread elsewhere retires a value born
//! before they pinned and flushes; the value is freed while they all stay
//! parked.

use kovan_channel::unbounded;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Parked receivers, as many as a machine of 36 hardware threads has.
const RECEIVERS: usize = 36;

const DEADLINE: Duration = Duration::from_secs(60);

/// Waits for `done` with a bounded backoff (a yield, then sleeps that
/// double up to 10 ms); false if `DEADLINE` passes first.
fn eventually(done: impl Fn() -> bool) -> bool {
    let end = Instant::now() + DEADLINE;
    let mut pause = Duration::from_micros(50);
    while !done() {
        if Instant::now() > end {
            return false;
        }
        thread::yield_now();
        thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_millis(10));
    }
    true
}

struct Val {
    live: Arc<AtomicUsize>,
}

impl Val {
    fn new(live: &Arc<AtomicUsize>) -> Self {
        live.fetch_add(1, Ordering::SeqCst);
        Self {
            live: Arc::clone(live),
        }
    }
}

impl Drop for Val {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

#[test]
#[cfg_attr(miri, ignore)] // threads
fn parked_receivers_hold_nothing_back() {
    let live = Arc::new(AtomicUsize::new(0));
    // The value to retire, allocated before any receiver pins: born no later
    // than their reservations, so a slot they left active would take it.
    let atom = Arc::new(kovan::Atom::new(Val::new(&live)));
    let (tx, rx) = unbounded::<u64>();
    let parked = Arc::new(Barrier::new(RECEIVERS + 1));
    let receivers: Vec<_> = (0..RECEIVERS)
        .map(|_| {
            let (rx, parked) = (rx.clone(), Arc::clone(&parked));
            thread::spawn(move || {
                // A receive that finds nothing pins for its look at the list
                // and parks; this one is the receiver's first use of kovan.
                assert!(rx.try_recv().is_none());
                parked.wait();
                rx.recv()
            })
        })
        .collect();
    parked.wait();
    // Every receiver is in recv(): the wakeups below find all of them
    // parked or about to park, and none holds a guard meanwhile.
    {
        let (atom, live) = (Arc::clone(&atom), Arc::clone(&live));
        thread::spawn(move || {
            atom.store(Val::new(&live));
            kovan::flush();
        })
        .join()
        .unwrap();
    }
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 1),
        "the retired value waited for {RECEIVERS} receivers parked in recv()"
    );
    for i in 0..RECEIVERS as u64 {
        tx.send(i);
    }
    let mut got: Vec<u64> = receivers
        .into_iter()
        .map(|r| r.join().unwrap().expect("a receiver woke with nothing"))
        .collect();
    got.sort_unstable();
    assert_eq!(got, (0..RECEIVERS as u64).collect::<Vec<_>>());
    drop(atom);
    kovan::flush();
    assert!(eventually(|| live.load(Ordering::SeqCst) == 0));
}
