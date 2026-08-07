//! Single-threaded, wasm-capable adaptation of `tests/hashmap_test.rs`.
//!
//! 12 of the original's 16 tests port verbatim (no threading at all). 4 are
//! not ported.
//!
//! Not ported (race-only):
//! - `test_concurrent_insert_read` — 4 writer threads each insert a
//!   disjoint 1000-key range while 4 reader threads concurrently poll one
//!   key. The keys never overlap across writers, so the only thing under
//!   test is that reads happening *at the same time* as writes from real
//!   OS threads don't crash or corrupt state; sequentially there is no
//!   concurrent access to survive, and the write half reduces to
//!   `test_many_entries` below.
//! - `test_concurrent_remove` — 4 threads remove disjoint key ranges from a
//!   pre-populated map; same shape as above (disjoint keys, no actual
//!   contention to resolve). Sequentially this is just `test_remove` run
//!   4000 times.
//! - `test_concurrent_mixed_operations` — 8 threads doing
//!   insert/get/remove on disjoint keys, with no assertion beyond "doesn't
//!   crash". Its entire value is real concurrent access to shared map
//!   internals; nothing survives without real threads.
//! - `test_insert_replace_concurrent` — 8 threads race to `insert` the
//!   SAME key with different values, then check the final value is one of
//!   the 8 thread IDs. The property under test is CAS correctness under a
//!   same-key race; sequentially the last writer always wins deterministically,
//!   which is exactly what `test_insert_replace` below already checks.
//!
//! # From `tests/hashmap_insert_if_absent_atomicity.rs`
//!
//! That file has no wasm counterpart: every test in it is race-only.
//! Its reasoning is recorded here rather than in a test binary that
//! registers zero tests (which would report a false green).
//!
//! Not ported (race-only), all 3:
//! - `concurrent_insert_if_absent_loses_no_increments` — 16 threads race
//!   `insert_if_absent` on the same key, each routing 250 increments through
//!   whichever `Arc` counter the call resolves to; the assertion is that no
//!   increment is lost to two racing callers claiming *different* slots for
//!   the same key. Run from one thread there is no race: the first call
//!   always wins and every later call always resolves to the same `Arc`, so
//!   the counter total is trivially correct. The non-atomicity failure mode
//!   this test exists to catch cannot occur without concurrent callers.
//! - `concurrent_insert_if_absent_has_exactly_one_winner` — same shape,
//!   stated directly: across concurrent same-key callers, at most one may
//!   be told "absent". With one caller there is exactly one call, so
//!   "exactly one winner" is true by construction and proves nothing about
//!   the atomicity of the CAS under contention.
//! - `insert_if_absent_survives_concurrent_resizes` — pairs churn threads
//!   (forcing repeated table grows) against counter threads contending on
//!   one hot key, checking the counter isn't lost or double-claimed during
//!   a live migration. Sequentially there is no concurrent migration to
//!   race against — inserts and the `insert_if_absent` calls just happen in
//!   program order — so the property under test (the resolved entry stays
//!   reachable through a resize that is happening *at the same time* as the
//!   claim) has no single-threaded analogue. Basic `insert_if_absent`
//!   semantics and basic resize-survives-inserted-keys behavior are already
//!   covered without threads by `tests/wasm/hashmap_test.rs`'s
//!   `test_insert_if_absent` and `tests/wasm/hopscotch_test.rs`'s
//!   `test_capacity_and_growth`.

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

use kovan_map::HashMap;

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
fn test_drop_cleanup() {
    // Ensure no leaks or crashes on drop with many entries
    let map = HashMap::new();
    for i in 0..5000 {
        map.insert(i, format!("value_{}", i));
    }
    drop(map);
}
