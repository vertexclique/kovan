use kovan_map::HopscotchMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

#[test]
fn test_get_or_insert_returns_inserted_value() {
    let map = HopscotchMap::new();

    // Create an Arc with a counter
    let counter1 = Arc::new(AtomicU64::new(1));

    // Insert it
    let returned = map.get_or_insert("key", counter1.clone());

    // Modify the returned value
    returned.store(100, Ordering::Relaxed);

    // Get it again - should see the modification
    let retrieved = map.get_or_insert("key", Arc::new(AtomicU64::new(999)));

    // This should be 100, not 1 or 999
    assert_eq!(
        retrieved.load(Ordering::Relaxed),
        100,
        "get_or_insert should return the SAME Arc that was inserted, not a clone!"
    );
}

#[test]
fn test_get_or_insert_identity() {
    let map = HopscotchMap::new();

    let arc1 = Arc::new(AtomicU64::new(42));
    let returned1 = map.get_or_insert("key", arc1.clone());

    // The returned Arc should point to the SAME allocation as what was inserted
    assert!(
        Arc::ptr_eq(&returned1, &arc1) || Arc::ptr_eq(&returned1, &Arc::new(AtomicU64::new(42))),
        "First call should return value pointing to inserted data"
    );

    let arc2 = Arc::new(AtomicU64::new(99));
    let returned2 = map.get_or_insert("key", arc2);

    // Second call should return the EXISTING value, and it should be the same as first
    assert!(
        Arc::ptr_eq(&returned1, &returned2),
        "Second call should return same Arc as first call"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_get_or_insert() {
    let map = Arc::new(HopscotchMap::new());

    let mut handles = vec![];
    for i in 0..10 {
        let map_clone = map.clone();
        handles.push(thread::spawn(move || {
            let arc = Arc::new(AtomicU64::new(i));
            map_clone.get_or_insert("shared_key", arc)
        }));
    }

    let mut returned_values = vec![];
    for h in handles {
        returned_values.push(h.join().unwrap());
    }

    // All threads should get the SAME Arc instance (pointing to same allocation)
    let first = &returned_values[0];
    for val in &returned_values[1..] {
        assert!(
            Arc::ptr_eq(first, val),
            "All get_or_insert calls should return Arc pointing to same allocation! First: {:?}, This: {:?}",
            Arc::as_ptr(first),
            Arc::as_ptr(val)
        );
    }
}
