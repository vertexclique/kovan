//! Single-threaded, wasm-capable adaptation of `tests/hopscotch_test.rs`.
//!
//! 15 of the original's 21 tests port verbatim (no threading at all). 6 are
//! not ported.
//!
//! Not ported (race-only):
//! - `test_concurrent_insert_read` — 4 writer threads insert disjoint key
//!   ranges while 4 reader threads concurrently poll one key; the only
//!   thing under test is that reads racing writes from real OS threads
//!   don't crash or corrupt state. Sequentially there's no race, and the
//!   write half reduces to `test_many_entries` below.
//! - `test_concurrent_remove` — 4 threads remove disjoint key ranges; same
//!   shape, sequentially just `test_remove` run 4000 times.
//! - `test_concurrent_mixed_operations` — 8 threads doing
//!   insert/get/remove on disjoint keys with no assertion beyond "doesn't
//!   crash"; its entire value is real concurrent access to shared
//!   internals.
//! - `test_concurrent_growth` — 4 threads insert disjoint keys into a
//!   small-capacity map to force *concurrent* resize; sequentially the
//!   same total inserts trigger the same sequence of resizes with no race
//!   to survive, which is exactly `test_capacity_and_growth` below.
//! - `test_insert_replace_concurrent` — 8 threads race to `insert` the
//!   SAME key with different values; sequentially the last writer always
//!   wins deterministically, already covered by `test_insert_replace`.
//! - `test_concurrent_insert_resize_with_readers` — 4 writers forcing
//!   several resize cycles while 4 readers poll concurrently, tolerating a
//!   handful of races-losses before a re-insert pass. Sequentially there
//!   are no resize-vs-reader races to lose entries to, so the interesting
//!   property (readers survive a table swap happening under them) has no
//!   single-threaded analogue; growth itself is covered by
//!   `test_capacity_and_growth`.
//!
//! # From `tests/insert_if_absent_atomicity.rs`
//!
//! That file has no wasm counterpart: every test in it is race-only.
//! Its reasoning is recorded here rather than in a test binary that
//! registers zero tests (which would report a false green).
//!
//! Not ported (race-only), both:
//! - `concurrent_insert_if_absent_loses_no_increments` — 16 threads race
//!   `insert_if_absent` on the same key, routing 250 increments each
//!   through whichever `Arc` counter the call resolves to. The failure mode
//!   this guards against (phase-1 scan / phase-2 CAS letting two same-key
//!   inserters claim different slots) only exists under concurrent callers;
//!   run from one thread the first call always wins and the total is
//!   trivially correct.
//! - `concurrent_insert_if_absent_has_exactly_one_winner` — the contract
//!   stated directly: across concurrent same-key callers, at most one may
//!   be told "absent". With a single caller there is exactly one call, so
//!   the assertion holds by construction and exercises nothing about the
//!   CAS's atomicity under contention. Basic `insert_if_absent` semantics
//!   are already covered without threads by `tests/wasm/hopscotch_test.rs`'s
//!   `test_insert_if_absent`.

// On wasm32 there is no libtest runner; the wasm-bindgen harness supplies one.
// Aliasing its attribute to `test` lets every case below run unmodified on both
// native (libtest) and wasm32-unknown-unknown (wasm-bindgen-test).
// wasm-bindgen-test only registers its harness export under
// `target_os = "unknown"`. wasm32-wasip1 is `target_os = "wasi"`, where the
// attribute compiles but libtest never sees the function -- the binary would
// report "0 tests" and exit 0. So wasip1 keeps the plain libtest attribute and
// runs under wasmtime; only unknown-unknown swaps in the bindgen harness.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

use kovan_map::HopscotchMap;

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
