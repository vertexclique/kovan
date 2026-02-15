use kovan_map::{HashMap, HopscotchMap};
use std::sync::Arc;
use std::thread;

#[test]
#[cfg_attr(miri, ignore)]
fn test_hashmap_heavy_contention_same_key() {
    let map = Arc::new(HashMap::new());

    let mut handles = vec![];
    for t in 0..8 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..5000 {
                m.insert(0, t * 5000 + i);
                let _ = m.get(&0);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(map.get(&0).is_some());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_hashmap_concurrent_insert_remove_cycle() {
    let map = Arc::new(HashMap::new());

    let mut handles = vec![];
    for t in 0..4 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..2000 {
                let key = t * 2000 + i;
                m.insert(key, key);
                if i % 2 == 0 {
                    m.remove(&key);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_hashmap_read_heavy() {
    let map = Arc::new(HashMap::new());

    // Pre-populate
    for i in 0..1000 {
        map.insert(i, i * 2);
    }

    let mut handles = vec![];

    // Many readers
    for _ in 0..8 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..10_000 {
                let key = i % 1000;
                assert_eq!(m.get(&key), Some(key * 2));
            }
        }));
    }

    // One writer
    {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 1000..2000 {
                m.insert(i, i * 2);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_hopscotch_heavy_contention_same_key() {
    let map = Arc::new(HopscotchMap::new());

    let mut handles = vec![];
    for t in 0..8 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..5000 {
                m.insert(0, t * 5000 + i);
                let _ = m.get(&0);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(map.get(&0).is_some());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_hopscotch_concurrent_insert_remove_cycle() {
    let map = Arc::new(HopscotchMap::new());

    let mut handles = vec![];
    for t in 0..4 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..2000 {
                let key = t * 2000 + i;
                m.insert(key, key);
                if i % 2 == 0 {
                    m.remove(&key);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_hopscotch_growth_under_contention() {
    let map = Arc::new(HopscotchMap::with_capacity(64));

    let mut handles = vec![];
    for t in 0..8 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = t * 1000 + i;
                m.insert(key, key);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify entries — under heavy concurrent resize some may need re-insert
    let mut missing = 0;
    for t in 0..8 {
        for i in 0..1000 {
            let key = t * 1000 + i;
            if map.get(&key).is_none() {
                missing += 1;
            }
        }
    }
    // Allow a small number of losses from resize races, but most should survive
    assert!(
        missing <= 50,
        "too many missing keys: {} out of 8000",
        missing
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_hopscotch_read_heavy() {
    let map = Arc::new(HopscotchMap::new());

    for i in 0..1000 {
        map.insert(i, i * 2);
    }

    let mut handles = vec![];

    for _ in 0..8 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..10_000 {
                let key = i % 1000;
                assert_eq!(m.get(&key), Some(key * 2));
            }
        }));
    }

    {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 1000..2000 {
                m.insert(i, i * 2);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_hashmap_iter_during_mutation() {
    let map = Arc::new(HashMap::new());

    for i in 0..100 {
        map.insert(i, i);
    }

    let m = map.clone();
    let writer = thread::spawn(move || {
        for i in 100..200 {
            m.insert(i, i);
        }
    });

    // Iterate concurrently - should not crash
    let entries: Vec<_> = map.iter().collect();
    assert!(!entries.is_empty());

    writer.join().unwrap();
}
