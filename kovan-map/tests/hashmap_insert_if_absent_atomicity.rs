//! `HashMap::insert_if_absent` under the same contract as the hopscotch
//! suite: across concurrent same-key callers, exactly ONE may be told
//! "absent", and every increment routed through the resolved value must be
//! visible through a final `get`.
//!
//! The chain map's miss path is structurally serialized (every same-key
//! inserter converges on the same tail link, and the null-expecting CAS
//! admits one winner), so the no-resize cases guard against regression.
//! The resize-pressure case targets the migration retry: an insert that
//! re-validates after a table swap must not be double-counted as a win
//! when another caller's entry landed in the new table first.

use kovan_map::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::thread;

#[test]
fn concurrent_insert_if_absent_loses_no_increments() {
    const THREADS: usize = 16;
    const INCREMENTS: usize = 250;
    const ROUNDS: usize = 400;

    for round in 0..ROUNDS {
        let map: Arc<HashMap<String, Arc<AtomicI64>>> = Arc::new(HashMap::new());
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
        let total = map
            .get("hits")
            .expect("key present")
            .load(Ordering::Acquire);
        assert_eq!(
            total,
            (THREADS * INCREMENTS) as i64,
            "round {round}: increments lost"
        );
    }
}

#[test]
fn concurrent_insert_if_absent_has_exactly_one_winner() {
    const THREADS: usize = 16;
    const ROUNDS: usize = 2000;

    for round in 0..ROUNDS {
        let map: Arc<HashMap<String, usize>> = Arc::new(HashMap::new());
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

/// The migration window: churn threads force repeated grows while counter
/// threads contend on one hot key. Every resolved counter must stay
/// reachable through `get` - a lost or double-claimed entry shows up as a
/// missing increment.
#[test]
fn insert_if_absent_survives_concurrent_resizes() {
    const COUNTER_THREADS: usize = 8;
    const CHURN_THREADS: usize = 8;
    const INCREMENTS: usize = 300;
    const ROUNDS: usize = 120;

    for round in 0..ROUNDS {
        // Small table so the churn threads drive it through many grows.
        let map: Arc<HashMap<String, Arc<AtomicI64>>> = Arc::new(HashMap::with_capacity(64));
        let mut handles = Vec::new();
        for t in 0..CHURN_THREADS {
            let m = map.clone();
            handles.push(thread::spawn(move || {
                for i in 0..INCREMENTS {
                    m.insert(format!("churn-{t}-{i}"), Arc::new(AtomicI64::new(0)));
                }
            }));
        }
        for _ in 0..COUNTER_THREADS {
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
        let total = map
            .get("hits")
            .expect("key present")
            .load(Ordering::Acquire);
        assert_eq!(
            total,
            (COUNTER_THREADS * INCREMENTS) as i64,
            "round {round}: increments lost across resizes"
        );
    }
}
