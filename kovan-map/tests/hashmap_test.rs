use kovan_map::HashMap;
use std::sync::Arc;
use std::thread;

#[test]
fn test_insert_and_get() {
    let map = HashMap::new();
    assert_eq!(map.insert("a", 1), None);
    assert_eq!(map.insert("b", 2), None);
    assert_eq!(map.get(&"a"), Some(1));
    assert_eq!(map.get(&"b"), Some(2));
    assert_eq!(map.get(&"c"), None);
}

#[test]
fn test_insert_replace() {
    let map = HashMap::new();
    assert_eq!(map.insert(1, 10), None);
    assert_eq!(map.insert(1, 20), Some(10));
    assert_eq!(map.insert(1, 30), Some(20));
    assert_eq!(map.get(&1), Some(30));
}

#[test]
fn test_remove() {
    let map = HashMap::new();
    map.insert(1, 100);
    map.insert(2, 200);

    assert_eq!(map.remove(&1), Some(100));
    assert_eq!(map.get(&1), None);
    assert_eq!(map.remove(&1), None);
    assert_eq!(map.get(&2), Some(200));
}

#[test]
fn test_contains_key() {
    let map = HashMap::new();
    map.insert(42, "hello");
    assert!(map.contains_key(&42));
    assert!(!map.contains_key(&99));
}

#[test]
fn test_len_and_is_empty() {
    let map = HashMap::new();
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
    let map = HashMap::new();
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
    let map = HashMap::new();
    assert_eq!(map.insert_if_absent(1, 100), None);
    assert_eq!(map.insert_if_absent(1, 200), Some(100));
    assert_eq!(map.get(&1), Some(100));
}

#[test]
fn test_iter() {
    let map = HashMap::new();
    map.insert(1, 10);
    map.insert(2, 20);
    map.insert(3, 30);

    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by_key(|(k, _)| *k);
    assert_eq!(entries, vec![(1, 10), (2, 20), (3, 30)]);
}

#[test]
fn test_keys() {
    let map = HashMap::new();
    map.insert(1, 10);
    map.insert(2, 20);

    let mut keys: Vec<_> = map.keys().collect();
    keys.sort();
    assert_eq!(keys, vec![1, 2]);
}

#[test]
fn test_many_entries() {
    let map = HashMap::new();
    for i in 0..10_000 {
        map.insert(i, i * 3);
    }
    for i in 0..10_000 {
        assert_eq!(map.get(&i), Some(i * 3));
    }
    assert_eq!(map.len(), 10_000);
}

#[test]
fn test_string_keys() {
    let map = HashMap::new();
    map.insert("hello".to_string(), 1);
    map.insert("world".to_string(), 2);
    assert_eq!(map.get(&"hello".to_string()), Some(1));
    assert_eq!(map.get(&"world".to_string()), Some(2));
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_insert_read() {
    let map = Arc::new(HashMap::new());
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

    // Readers (concurrent with writers)
    for _ in 0..4 {
        let m = map.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                // Just ensure no crashes during concurrent reads
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
    let map = Arc::new(HashMap::new());
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
    let map = Arc::new(HashMap::new());
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
fn test_insert_replace_concurrent() {
    let map = Arc::new(HashMap::new());
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

    // Value should be one of the thread IDs
    let val = map.get(&0).unwrap();
    assert!(val < 8);
}

#[test]
fn test_drop_cleanup() {
    // Ensure no leaks or crashes on drop with many entries
    let map = HashMap::new();
    for i in 0..5000 {
        map.insert(i, format!("value_{}", i));
    }
    drop(map);
}
