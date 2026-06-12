//! `insert_if_absent` must be a true once-only claim: under concurrent
//! same-key callers, exactly ONE caller may be told "absent" (None), and
//! every other caller must receive THAT winner's value.
//!
//! The failure mode this guards (observed in the wild as a lost counter
//! increment): phase-1 of `try_insert` scans the neighborhood for the key,
//! phase-2 claims any empty slot by CAS and only then sets the hop bit.
//! Two same-key inserters can claim DIFFERENT slots in the same window -
//! both return `Success(None)`, the map holds two "versions", and `get`
//! only ever returns one of them. Any state accumulated through the other
//! caller's value (an `Arc<AtomicI64>` counter, a channel handle, ...) is
//! silently shadowed.

use kovan_map::HopscotchMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::thread;

/// The exact consumer pattern that surfaced the bug: a counter keyed by
/// name, seeded through `insert_if_absent`, incremented through whichever
/// `Arc` the call resolved to. Every increment must be visible through a
/// final `get`.
#[test]
fn concurrent_insert_if_absent_loses_no_increments() {
    const THREADS: usize = 16;
    const INCREMENTS: usize = 250;
    const ROUNDS: usize = 400;

    for round in 0..ROUNDS {
        let map: Arc<HopscotchMap<String, Arc<AtomicI64>>> = Arc::new(HopscotchMap::new());
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let m = map.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..INCREMENTS {
                    let fresh = Arc::new(AtomicI64::new(0));
                    let counter = match m.insert_if_absent("hits".to_string(), fresh.clone()) {
                        None => fresh,
                        Some(existing) => existing,
                    };
                    counter.fetch_add(1, Ordering::AcqRel);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let total = map.get("hits").expect("key present").load(Ordering::Acquire);
        assert_eq!(
            total,
            (THREADS * INCREMENTS) as i64,
            "round {round}: increments lost - insert_if_absent admitted more than one winner"
        );
    }
}

/// The contract stated directly: across all concurrent callers for one
/// key, at most one receives `None`.
#[test]
fn concurrent_insert_if_absent_has_exactly_one_winner() {
    const THREADS: usize = 16;
    const ROUNDS: usize = 2000;

    for round in 0..ROUNDS {
        let map: Arc<HopscotchMap<String, usize>> = Arc::new(HopscotchMap::new());
        let winners = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let m = map.clone();
            let w = winners.clone();
            handles.push(thread::spawn(move || {
                if m.insert_if_absent("k".to_string(), t).is_none() {
                    w.fetch_add(1, Ordering::AcqRel);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let n = winners.load(Ordering::Acquire);
        assert_eq!(
            n, 1,
            "round {round}: {n} callers were told the key was absent"
        );
    }
}
