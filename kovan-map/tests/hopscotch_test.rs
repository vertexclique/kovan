use kovan_map::HopscotchMap;
use std::sync::Arc;
use std::thread;

#[test]
fn test_insert_and_get() {
    let map = HopscotchMap::new();
    assert_eq!(map.insert("a", 1), None);
    assert_eq!(map.insert("b", 2), None);
    assert_eq!(map.get(&"a"), Some(1));
    assert_eq!(map.get(&"b"), Some(2));
    assert_eq!(map.get(&"c"), None);
}

#[test]
fn test_insert_replace() {
    let map = HopscotchMap::new();
    assert_eq!(map.insert(1, 10), None);
    assert_eq!(map.insert(1, 20), Some(10));
    assert_eq!(map.insert(1, 30), Some(20));
    assert_eq!(map.get(&1), Some(30));
}

#[test]
fn test_remove() {
    let map = HopscotchMap::new();
    map.insert(1, 100);
    map.insert(2, 200);

    assert_eq!(map.remove(&1), Some(100));
    assert_eq!(map.get(&1), None);
    assert_eq!(map.remove(&1), None);
    assert_eq!(map.get(&2), Some(200));
}

#[test]
fn test_len_and_is_empty() {
    let map = HopscotchMap::new();
    assert!(map.is_empty());
    assert_eq!(map.len(), 0);

    map.insert(1, 1);
    map.insert(2, 2);
    assert!(!map.is_empty());
    assert_eq!(map.len(), 2);

    map.remove(&1);
    assert_eq!(map.len(), 1);
}

#[test]
fn test_clear() {
    let map = HopscotchMap::new();
    for i in 0..100 {
        map.insert(i, i * 10);
    }
    assert_eq!(map.len(), 100);

    map.clear();
    assert!(map.is_empty());
    for i in 0..100 {
        assert_eq!(map.get(&i), None);
    }
}

#[test]
fn test_insert_if_absent() {
    let map = HopscotchMap::new();
    assert_eq!(map.insert_if_absent(1, 100), None);
    assert_eq!(map.insert_if_absent(1, 200), Some(100));
    assert_eq!(map.get(&1), Some(100));
}

#[test]
fn test_get_or_insert() {
    let map = HopscotchMap::new();
    assert_eq!(map.get_or_insert(1, 100), 100);
    assert_eq!(map.get_or_insert(1, 200), 100);
    assert_eq!(map.get(&1), Some(100));
}

#[test]
fn test_capacity_and_growth() {
    let map = HopscotchMap::with_capacity(64);
    let initial_cap = map.capacity();
    assert!(initial_cap >= 64);

    // Insert enough to trigger growth (>75% load factor)
    for i in 0..200 {
        map.insert(i, i);
    }
    assert!(map.capacity() > initial_cap);

    // Verify all entries survived growth
    for i in 0..200 {
        assert_eq!(map.get(&i), Some(i), "missing key {}", i);
    }
}

#[test]
fn test_shrink() {
    let map = HopscotchMap::with_capacity(64);

    // Fill up
    for i in 0..200 {
        map.insert(i, i);
    }
    let grown_cap = map.capacity();

    // Remove most entries to trigger shrink (<25% load factor)
    for i in 0..190 {
        map.remove(&i);
    }

    // Remaining entries should still be accessible
    for i in 190..200 {
        assert_eq!(map.get(&i), Some(i));
    }

    // Capacity should have shrunk (or at least not grown further)
    assert!(map.capacity() <= grown_cap);
}

#[test]
fn test_iter() {
    let map = HopscotchMap::new();
    map.insert(1, 10);
    map.insert(2, 20);
    map.insert(3, 30);

    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by_key(|(k, _)| *k);
    assert_eq!(entries, vec![(1, 10), (2, 20), (3, 30)]);
}

#[test]
fn test_keys() {
    let map = HopscotchMap::new();
    map.insert(1, 10);
    map.insert(2, 20);

    let mut keys: Vec<_> = map.keys().collect();
    keys.sort();
    assert_eq!(keys, vec![1, 2]);
}

#[test]
fn test_string_keys() {
    let map = HopscotchMap::new();
    map.insert("hello".to_string(), 1);
    map.insert("world".to_string(), 2);
    assert_eq!(map.get(&"hello".to_string()), Some(1));
    assert_eq!(map.get(&"world".to_string()), Some(2));
}

#[test]
fn test_many_entries() {
    let map = HopscotchMap::new();
    for i in 0..5_000 {
        map.insert(i, i * 3);
    }
    for i in 0..5_000 {
        assert_eq!(map.get(&i), Some(i * 3), "missing key {}", i);
    }
    assert_eq!(map.len(), 5_000);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_insert_read() {
    let map = Arc::new(HopscotchMap::new());
    let mut handles = vec![];

    // Writers
    for t in 0..4 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = t * 1000 + i;
                m.insert(key, key * 2);
            }
        }));
    }

    // Readers
    for _ in 0..4 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let _ = m.get(&500);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    for t in 0..4 {
        for i in 0..1000 {
            let key = t * 1000 + i;
            assert_eq!(map.get(&key), Some(key * 2));
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_remove() {
    let map = Arc::new(HopscotchMap::new());
    for i in 0..4000 {
        map.insert(i, i);
    }

    let mut handles = vec![];
    for t in 0..4 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = t * 1000 + i;
                m.remove(&key);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(map.is_empty());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_mixed_operations() {
    let map = Arc::new(HopscotchMap::new());
    let mut handles = vec![];

    for t in 0..8 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                let key = t * 500 + i;
                m.insert(key, key);
                let _ = m.get(&key);
                if i % 3 == 0 {
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
fn test_concurrent_growth() {
    let map = Arc::new(HopscotchMap::with_capacity(64));
    let mut handles = vec![];

    // Multiple threads forcing concurrent growth
    for t in 0..4 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                let key = t * 500 + i;
                m.insert(key, key);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    for t in 0..4 {
        for i in 0..500 {
            let key = t * 500 + i;
            assert_eq!(map.get(&key), Some(key), "missing key {}", key);
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_insert_replace_concurrent() {
    let map = Arc::new(HopscotchMap::new());
    map.insert(0, 0);

    let mut handles = vec![];
    for t in 0..8 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                m.insert(0, t);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let val = map.get(&0).unwrap();
    assert!(val < 8);
}

#[test]
fn test_drop_cleanup() {
    let map = HopscotchMap::new();
    for i in 0..5000 {
        map.insert(i, format!("value_{}", i));
    }
    drop(map);
}

#[test]
fn test_insert_remove_reinsert() {
    let map = HopscotchMap::new();
    for i in 0..100 {
        map.insert(i, i);
    }
    for i in 0..100 {
        map.remove(&i);
    }
    assert!(map.is_empty());

    // Reinsert with different values
    for i in 0..100 {
        map.insert(i, i + 1000);
    }
    for i in 0..100 {
        assert_eq!(map.get(&i), Some(i + 1000));
    }
}
