//! Single-threaded, wasm-capable adaptation of `tests/stress_test.rs`.
//!
//! 7 of the original's 8 tests are adapted to run the same total op count
//! from one thread instead of across several real threads — none of them
//! assert anything beyond "the operation sequence completes without
//! panicking or corrupting map state" (or, for
//! `test_hopscotch_growth_under_contention`, a race-tolerant loose bound
//! that becomes an exact bound once there's no race), so the sequential
//! version still validates the same state machine, just not contention
//! itself. 1 is not ported.
//!
//! Not ported (race-only):
//! - `test_hashmap_iter_during_mutation` — spawns a writer thread that
//!   inserts new keys while the main thread concurrently calls `map.iter()`
//!   on the same map; the assertion is just that this doesn't crash. Its
//!   entire value is proving the iterator survives a table being mutated
//!   out from under it *during* iteration, from a real second thread.
//!   Sequentially there is no concurrent mutation for the iterator to
//!   survive — inserting then iterating is a no-op check with zero
//!   coverage of that property.

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

use kovan_map::{HashMap, HopscotchMap};

/// Sequential adaptation of `stress_test::test_hashmap_heavy_contention_same_key`.
/// The original ran 8 threads each doing 5000 insert+get pairs against key
/// `0`. This runs the same 40,000 insert+get pairs from one thread — it
/// validates the operation sequence doesn't corrupt state, not CAS
/// correctness under real same-key contention. The threaded original
/// remains the real coverage on native.
#[test]
fn hashmap_heavy_contention_same_key_sequential() {
    let map = HashMap::new();

    for t in 0..8u64 {
        for i in 0..5000u64 {
            map.insert(0, t * 5000 + i);
            let _ = map.get(&0);
        }
    }

    assert!(map.get(&0).is_some());
}

/// Sequential adaptation of `stress_test::test_hashmap_concurrent_insert_remove_cycle`.
/// The original split 8000 insert/remove-cycle ops across 4 threads on
/// disjoint keys. This runs the same total op count from one thread.
#[test]
fn hashmap_insert_remove_cycle_sequential() {
    let map = HashMap::new();

    for t in 0..4u64 {
        for i in 0..2000u64 {
            let key = t * 2000 + i;
            map.insert(key, key);
            if i % 2 == 0 {
                map.remove(&key);
            }
        }
    }
}

/// Sequential adaptation of `stress_test::test_hashmap_read_heavy`. The
/// original ran 8 reader threads (10,000 reads each) concurrently with 1
/// writer thread inserting 1000 more keys. This runs the writer's inserts
/// first, then the same total read count, from one thread — it validates
/// the same reads resolve to the same values, not concurrent read/write
/// safety. The threaded original remains the real coverage on native.
#[test]
fn hashmap_read_heavy_sequential() {
    let map = HashMap::new();

    for i in 0..1000u64 {
        map.insert(i, i * 2);
    }
    for i in 1000..2000u64 {
        map.insert(i, i * 2);
    }

    for _ in 0..8 {
        for i in 0..10_000u64 {
            let key = i % 1000;
            assert_eq!(map.get(&key), Some(key * 2));
        }
    }
}

/// Sequential adaptation of `stress_test::test_hopscotch_heavy_contention_same_key`.
/// See `hashmap_heavy_contention_same_key_sequential` above; same shape on
/// the other map implementation.
#[test]
fn hopscotch_heavy_contention_same_key_sequential() {
    let map = HopscotchMap::new();

    for t in 0..8u64 {
        for i in 0..5000u64 {
            map.insert(0, t * 5000 + i);
            let _ = map.get(&0);
        }
    }

    assert!(map.get(&0).is_some());
}

/// Sequential adaptation of `stress_test::test_hopscotch_concurrent_insert_remove_cycle`.
/// See `hashmap_insert_remove_cycle_sequential` above; same shape on the
/// other map implementation.
#[test]
fn hopscotch_insert_remove_cycle_sequential() {
    let map = HopscotchMap::new();

    for t in 0..4u64 {
        for i in 0..2000u64 {
            let key = t * 2000 + i;
            map.insert(key, key);
            if i % 2 == 0 {
                map.remove(&key);
            }
        }
    }
}

/// Sequential adaptation of `stress_test::test_hopscotch_growth_under_contention`.
/// The original ran 8 threads inserting disjoint keys into a small-capacity
/// map to force concurrent resize, then tolerated up to 50 losses out of
/// 8000 keys to resize races. Sequentially there is no resize race, so
/// every key must survive — the loose bound becomes an exact one. This
/// still exercises the same growth pattern (small starting capacity, 8000
/// total inserts), just not concurrent-resize-vs-writer safety.
#[test]
fn hopscotch_growth_sequential() {
    let map = HopscotchMap::with_capacity(64);

    for t in 0..8u64 {
        for i in 0..1000u64 {
            let key = t * 1000 + i;
            map.insert(key, key);
        }
    }

    for t in 0..8u64 {
        for i in 0..1000u64 {
            let key = t * 1000 + i;
            assert_eq!(map.get(&key), Some(key), "missing key {}", key);
        }
    }
}

/// Sequential adaptation of `stress_test::test_hopscotch_read_heavy`. See
/// `hashmap_read_heavy_sequential` above; same shape on the other map
/// implementation.
#[test]
fn hopscotch_read_heavy_sequential() {
    let map = HopscotchMap::new();

    for i in 0..1000u64 {
        map.insert(i, i * 2);
    }
    for i in 1000..2000u64 {
        map.insert(i, i * 2);
    }

    for _ in 0..8 {
        for i in 0..10_000u64 {
            let key = i % 1000;
            assert_eq!(map.get(&key), Some(key * 2));
        }
    }
}
