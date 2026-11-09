//! Readers that looked a map up once and parked hold no table back: a lookup
//! pins for its walk and drops its guard before it returns, and a thread
//! with no live guard is counted by no batch. Sixteen readers park after one
//! lookup; a writer then resizes the map sixteen times and more, each resize
//! retiring the table it replaced (with the entries that table held); once
//! the writer has flushed, only the current table's entries are live, plus
//! at most one batch in flight.

use kovan_map::HopscotchMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const READERS: usize = 16;
/// Resizes the writer makes at least.
const RESIZES: usize = 16;
/// Entries inserted per cycle: from a table of 64 buckets this grows the
/// table three times, and removing them shrinks it back.
const PER_CYCLE: u64 = 300;
/// What may be in flight beside the current table's entries: one batch of
/// retired entries not yet submitted by the writer's last retire.
const SLACK: usize = 64;

const DEADLINE: Duration = Duration::from_secs(60);

static CREATES: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);

/// A value counted when made (cloned too, as a resize clones) and dropped.
struct Counted(#[allow(dead_code)] u64);

impl Counted {
    fn new(v: u64) -> Self {
        CREATES.fetch_add(1, Ordering::SeqCst);
        Counted(v)
    }
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        CREATES.fetch_add(1, Ordering::SeqCst);
        Counted(self.0)
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

fn live() -> usize {
    CREATES.load(Ordering::SeqCst) - DROPS.load(Ordering::SeqCst)
}

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

#[test]
#[cfg_attr(miri, ignore)] // threads
fn parked_readers_hold_no_table_back() {
    let map = Arc::new(HopscotchMap::with_capacity(64));
    map.insert(0u64, Counted::new(0));
    let parked = Arc::new(Barrier::new(READERS + 1));
    let go = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let (map, parked, go) = (Arc::clone(&map), Arc::clone(&parked), Arc::clone(&go));
            thread::spawn(move || {
                // One lookup: the reader's thread pins, walks the table and
                // drops its guard. Then it parks for good.
                assert!(map.get(&0).is_some());
                parked.wait();
                assert!(eventually(|| go.load(Ordering::SeqCst)));
            })
        })
        .collect();
    parked.wait();

    // The writer resizes the map, growing and shrinking, until it has
    // replaced the table at least RESIZES times, then flushes what it
    // retired.
    let resizes = {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            let mut resizes = 0;
            let mut capacity = map.capacity();
            let mut key = 1u64;
            while resizes < RESIZES {
                let first = key;
                for _ in 0..PER_CYCLE {
                    map.insert(key, Counted::new(key));
                    key += 1;
                    if map.capacity() != capacity {
                        capacity = map.capacity();
                        resizes += 1;
                    }
                }
                for k in first..key {
                    map.remove(&k);
                    if map.capacity() != capacity {
                        capacity = map.capacity();
                        resizes += 1;
                    }
                }
            }
            kovan::flush();
            resizes
        })
        .join()
        .unwrap()
    };
    assert!(resizes >= RESIZES, "only {resizes} resizes");

    // Every replaced table and every removed entry is freed while the
    // readers stay parked: what is live is the current table's entries
    // and at most one batch in flight.
    let bound = map.len() + SLACK;
    assert!(
        eventually(|| live() <= bound),
        "{} entries live after {resizes} resizes with {READERS} parked readers, the table holds {}",
        live(),
        map.len()
    );
    go.store(true, Ordering::SeqCst);
    for r in readers {
        r.join().unwrap();
    }
    drop(map);
    kovan::flush();
    assert!(eventually(|| live() == 0), "{} entries never freed", live());
}
