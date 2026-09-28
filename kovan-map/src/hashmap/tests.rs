//! Colocated coverage for `HashMap`: its single-thread contract, growth, shrinking down to (and
//! never below) its floor, concurrent inserts across a growth, and (`conditional`) the
//! conditional writes.

use super::*;

#[test]
fn test_insert_and_get() {
    let map = HashMap::new();
    assert_eq!(map.insert(1, 100), None);
    assert_eq!(map.get(&1), Some(100));
    assert_eq!(map.get(&2), None);
}

#[test]
fn test_insert_replace() {
    let map = HashMap::new();
    assert_eq!(map.insert(1, 100), None);
    assert_eq!(map.insert(1, 200), Some(100));
    assert_eq!(map.get(&1), Some(200));
}

#[test]
fn test_grow() {
    let map = HashMap::with_capacity(64);
    assert_eq!(map.capacity(), 64);
    for i in 0..1000u64 {
        map.insert(i, i * 2);
    }
    assert!(map.capacity() > 64, "map should have grown");
    for i in 0..1000u64 {
        assert_eq!(map.get(&i), Some(i * 2));
    }
    assert_eq!(map.len(), 1000);
}

#[test]
fn test_shrink() {
    let map = HashMap::with_capacity(64);
    for i in 0..1000u64 {
        map.insert(i, i);
    }
    let grown = map.capacity();
    assert!(grown > 64);
    for i in 0..1000u64 {
        map.remove(&i);
    }
    assert!(
        map.capacity() < grown,
        "map should have shrunk (capacity {} -> {})",
        grown,
        map.capacity()
    );
    assert!(map.capacity() >= 64, "never below the initial capacity");
    assert_eq!(map.len(), 0);
}

#[test]
fn test_no_shrink_below_floor() {
    let map = HashMap::with_capacity(4096);
    for i in 0..100u64 {
        map.insert(i, i);
    }
    for i in 0..100u64 {
        map.remove(&i);
    }
    assert_eq!(map.capacity(), 4096, "floor preserves sizing intent");
}

#[test]
fn test_concurrent_inserts() {
    use alloc::sync::Arc;
    extern crate std;
    use std::thread;

    let map = Arc::new(HashMap::new());
    let mut handles = alloc::vec::Vec::new();

    for thread_id in 0..4 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = thread_id * 1000 + i;
                map_clone.insert(key, key * 2);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for thread_id in 0..4 {
        for i in 0..1000 {
            let key = thread_id * 1000 + i;
            assert_eq!(map.get(&key), Some(key * 2));
        }
    }
}

#[test]
fn test_concurrent_grow() {
    use alloc::sync::Arc;
    extern crate std;
    use std::thread;

    let map = Arc::new(HashMap::with_capacity(64));
    let mut handles = alloc::vec::Vec::new();

    for thread_id in 0..8u64 {
        let map_clone = Arc::clone(&map);
        handles.push(thread::spawn(move || {
            for i in 0..2000u64 {
                let key = thread_id * 10_000 + i;
                map_clone.insert(key, key);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    for thread_id in 0..8u64 {
        for i in 0..2000u64 {
            let key = thread_id * 10_000 + i;
            assert_eq!(map.get(&key), Some(key), "lost key {key} during growth");
        }
    }
    assert!(map.capacity() >= 16_000);
}

mod conditional;
