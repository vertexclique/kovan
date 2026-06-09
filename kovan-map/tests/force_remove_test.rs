//! Tests for force_remove: exhaustive eviction of all versions of a key.

use kovan_map::{HashMap, HopscotchMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[test]
fn hashmap_force_remove_basic() {
    let map: HashMap<String, u64> = HashMap::new();
    assert_eq!(map.force_remove("missing"), None);

    map.insert("k".to_string(), 1);
    map.insert("k".to_string(), 2);
    assert_eq!(map.force_remove("k"), Some(2), "returns the latest version");
    assert!(!map.contains_key("k"));
    assert_eq!(map.force_remove("k"), None, "idempotent once evicted");
}

#[test]
fn hopscotch_force_remove_basic() {
    let map: HopscotchMap<String, u64> = HopscotchMap::new();
    assert_eq!(map.force_remove("missing"), None);

    map.insert("k".to_string(), 1);
    map.insert("k".to_string(), 2);
    assert_eq!(map.force_remove("k"), Some(2));
    assert!(map.get("k").is_none());
    assert_eq!(map.force_remove("k"), None);
}

/// Hammer one key with concurrent insert/remove from many threads, then
/// force_remove after the storm: the key must be fully gone even if races
/// left multiple versions in the bucket chain (plain remove() pops only the
/// first match and can resurrect an older version).
#[test]
fn hashmap_force_remove_evicts_all_versions_after_race() {
    const THREADS: usize = 8;
    const ITERS: usize = 5_000;

    let map: Arc<HashMap<u64, u64>> = Arc::new(HashMap::new());

    for round in 0..4 {
        let stop = Arc::new(AtomicBool::new(false));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let map = Arc::clone(&map);
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    for i in 0..ITERS {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        if (t + i) % 3 == 0 {
                            map.remove(&42);
                        } else {
                            map.insert(42, (t * ITERS + i) as u64);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        stop.store(true, Ordering::SeqCst);

        // After the storm: exhaustively evict.
        map.force_remove(&42);
        assert!(
            !map.contains_key(&42),
            "round {round}: key visible after force_remove — stale version resurrected"
        );
        kovan::flush();
    }
}

#[test]
fn hopscotch_force_remove_after_race() {
    const THREADS: usize = 8;
    const ITERS: usize = 2_000;

    let map: Arc<HopscotchMap<u64, u64>> = Arc::new(HopscotchMap::new());

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                for i in 0..ITERS {
                    if (t + i) % 3 == 0 {
                        map.remove(&7);
                    } else {
                        map.insert(7, (t * ITERS + i) as u64);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    map.force_remove(&7);
    assert!(map.get(&7).is_none());
    kovan::flush();
}
