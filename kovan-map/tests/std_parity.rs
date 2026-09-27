//! Std-parity proof: `HopscotchMap` and `HashMap` lay out bounds and trait impls the way
//! `std::collections::HashMap` does.
//!
//! One test per loosened bound proves it compiles with a type that lacks the trait the branch
//! would otherwise have required (a compile failure here means a bound crept back in); one test
//! per new trait impl (`Default`, `Debug`, `Clone`, `Extend`, `FromIterator`, `PartialEq`/`Eq`)
//! checks its behaviour against `std::collections::HashMap` on the same input; a last group
//! builds and drives both maps, single- and multi-threaded, entirely through RapidHash's own
//! `BuildHasher` (`rapidhash` is a dev-dependency of this crate only, never a normal one).

use kovan_map::{HashMap as KHashMap, HopscotchMap};
use rapidhash::fast::RandomState as RapidState;
use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use std::thread;

// ---------------------------------------------------------------------------
// Loosened-bound compile proofs
// ---------------------------------------------------------------------------

/// Neither `Hash`, `Eq`, `Clone` nor `Debug`: proves the small accessors that never hash need
/// none of them.
struct NoBounds;

/// Not `BuildHasher` (or anything hash-related): proves construction of the small accessors and
/// of the iterators needs no `S: BuildHasher` either.
struct NoHasher;

/// `Default` only, not `BuildHasher`: proves `Default for Map` needs exactly `S: Default`, as
/// `std::collections::HashMap`'s does (construction never hashes).
#[derive(Default)]
struct DefaultOnlyHasher;

#[test]
fn hopscotch_default_needs_only_s_default() {
    let map: HopscotchMap<NoBounds, NoBounds, DefaultOnlyHasher> = HopscotchMap::default();
    assert!(map.is_empty());
}

#[test]
fn hashmap_default_needs_only_s_default() {
    let map: KHashMap<NoBounds, NoBounds, DefaultOnlyHasher> = KHashMap::default();
    assert!(map.is_empty());
}

#[test]
fn hopscotch_accessors_need_no_hash_eq_clone_or_buildhasher() {
    let map: HopscotchMap<NoBounds, NoBounds, NoHasher> =
        HopscotchMap::with_capacity_and_hasher(64, NoHasher);
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
    assert!(map.capacity() >= 64);
    let _: &NoHasher = map.hasher();
    // Construction only (inserting needs Hash + Eq + Clone + BuildHasher, the block these
    // accessors were split out of).
    let _ = map.iter();
    let _ = map.keys();
    let _ = map.values();

    let via_with_hasher: HopscotchMap<NoBounds, NoBounds, NoHasher> =
        HopscotchMap::with_hasher(NoHasher);
    assert_eq!(via_with_hasher.len(), 0);
}

#[test]
fn hashmap_accessors_need_no_hash_eq_clone_or_buildhasher() {
    let map: KHashMap<NoBounds, NoBounds, NoHasher> =
        KHashMap::with_capacity_and_hasher(64, NoHasher);
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
    assert!(map.capacity() >= 64);
    let _: &NoHasher = map.hasher();
    let _ = map.iter();
    let _ = map.keys();
    let _ = map.values();

    let via_with_hasher: KHashMap<NoBounds, NoBounds, NoHasher> = KHashMap::with_hasher(NoHasher);
    assert_eq!(via_with_hasher.len(), 0);
}

#[test]
fn new_and_with_capacity_need_no_hash_eq_or_clone() {
    let map: HopscotchMap<NoBounds, NoBounds> = HopscotchMap::new();
    assert!(map.is_empty());
    let map: HopscotchMap<NoBounds, NoBounds> = HopscotchMap::with_capacity(128);
    assert!(map.capacity() >= 128);

    let map: KHashMap<NoBounds, NoBounds> = KHashMap::new();
    assert!(map.is_empty());
    let map: KHashMap<NoBounds, NoBounds> = KHashMap::with_capacity(128);
    assert!(map.capacity() >= 128);
}

/// `Clone` and `Debug` only: no `Hash`, no `Eq`.
#[derive(Clone, Debug)]
struct CloneDebugOnly;

#[test]
fn debug_needs_no_hash_or_buildhasher() {
    // HopscotchMap's walk compares keys (K: Eq), HashMap's does not; neither hashes.
    let map: HopscotchMap<i32, CloneDebugOnly, NoHasher> = HopscotchMap::with_hasher(NoHasher);
    assert_eq!(format!("{map:?}"), "{}");
    let map: KHashMap<CloneDebugOnly, CloneDebugOnly, NoHasher> = KHashMap::with_hasher(NoHasher);
    assert_eq!(format!("{map:?}"), "{}");
}

#[test]
fn hopscotch_into_iterator_for_ref_needs_no_hash_or_buildhasher() {
    // K: Eq + Clone (the walk yields owned clones - see the impl's doc comment), but no Hash and
    // no S: BuildHasher: `NoHasher` implements neither.
    let map: HopscotchMap<i32, String, NoHasher> = HopscotchMap::with_hasher(NoHasher);
    let mut n = 0;
    for _ in &map {
        n += 1;
    }
    assert_eq!(n, 0);
}

#[test]
fn hashmap_into_iterator_for_ref_needs_no_hash_eq_or_buildhasher() {
    // K: Clone only (no Eq, no Hash), and no S: BuildHasher.
    let map: KHashMap<i32, String, NoHasher> = KHashMap::with_hasher(NoHasher);
    let mut n = 0;
    for _ in &map {
        n += 1;
    }
    assert_eq!(n, 0);
}

// ---------------------------------------------------------------------------
// New trait impls, checked against std::collections::HashMap on the same input
// ---------------------------------------------------------------------------

/// `FromIterator`, `Clone` and `Extend` ask `Send` of what they insert (a replaced entry's
/// destructor may run on another thread), never `Sync`: `Cell` is `Send` and not `Sync`.
#[test]
fn inserting_impls_need_send_not_sync() {
    use std::cell::Cell;
    let map: HopscotchMap<i32, Cell<i32>> = (0..4).map(|i| (i, Cell::new(i))).collect();
    let mut cloned = map.clone();
    Extend::extend(&mut cloned, [(9, Cell::new(9))]);
    assert_eq!((map.len(), cloned.len()), (4, 5));
    assert_eq!(cloned.get(&9).map(Cell::into_inner), Some(9));

    let map: KHashMap<i32, Cell<i32>> = (0..4).map(|i| (i, Cell::new(i))).collect();
    let mut cloned = map.clone();
    Extend::extend(&mut cloned, [(9, Cell::new(9))]);
    assert_eq!((map.len(), cloned.len()), (4, 5));
    assert_eq!(cloned.get(&9).map(Cell::into_inner), Some(9));
}

#[test]
fn hopscotch_from_iter_and_eq_match_std() {
    let pairs: Vec<(i32, i32)> = (0..200).map(|i| (i, i * 2)).collect();
    let map: HopscotchMap<i32, i32> = pairs.iter().copied().collect();
    let std_map: StdHashMap<i32, i32> = pairs.iter().copied().collect();

    assert_eq!(map.len(), std_map.len());
    for (k, v) in &std_map {
        assert_eq!(map.get(k), Some(*v));
    }

    // Two concurrent maps built from the same input compare equal (PartialEq/Eq, V: i32: Eq).
    let map2: HopscotchMap<i32, i32> = pairs.into_iter().collect();
    assert_eq!(map, map2);
}

#[test]
fn hashmap_from_iter_and_eq_match_std() {
    let pairs: Vec<(i32, i32)> = (0..200).map(|i| (i, i * 2)).collect();
    let map: KHashMap<i32, i32> = pairs.iter().copied().collect();
    let std_map: StdHashMap<i32, i32> = pairs.iter().copied().collect();

    assert_eq!(map.len(), std_map.len());
    for (k, v) in &std_map {
        assert_eq!(map.get(k), Some(*v));
    }

    let map2: KHashMap<i32, i32> = pairs.into_iter().collect();
    assert_eq!(map, map2);
}

#[test]
fn partial_eq_holds_without_value_eq() {
    // f64 has PartialEq but not Eq: PartialEq must still work (Eq is a separate, stricter impl -
    // HopscotchMap<i32, f64>: Eq does not hold, and does not need to).
    let a: HopscotchMap<i32, f64> = HopscotchMap::new();
    a.insert(1, 1.5);
    let b: HopscotchMap<i32, f64> = HopscotchMap::new();
    b.insert(1, 1.5);
    assert!(a == b);
    b.insert(2, 2.5);
    assert!(a != b);
}

#[test]
fn hopscotch_debug_is_a_std_shaped_map() {
    let map: HopscotchMap<i32, i32> = HopscotchMap::new();
    map.insert(1, 10);
    let debug = format!("{map:?}");
    assert!(debug.starts_with('{') && debug.ends_with('}'));
    assert!(debug.contains('1') && debug.contains("10"));
}

#[test]
fn hashmap_debug_is_a_std_shaped_map() {
    let map: KHashMap<i32, i32> = KHashMap::new();
    map.insert(1, 10);
    let debug = format!("{map:?}");
    assert!(debug.starts_with('{') && debug.ends_with('}'));
    assert!(debug.contains('1') && debug.contains("10"));
}

#[test]
fn hopscotch_default_works_for_any_buildhasher_default() {
    let map: HopscotchMap<i32, i32, RapidState> = HopscotchMap::default();
    assert!(map.is_empty());
    map.insert(5, 50);
    assert_eq!(map.get(&5), Some(50));
}

#[test]
fn hashmap_default_works_for_any_buildhasher_default() {
    let map: KHashMap<i32, i32, RapidState> = KHashMap::default();
    assert!(map.is_empty());
    map.insert(5, 50);
    assert_eq!(map.get(&5), Some(50));
}

#[test]
fn hopscotch_clone_is_an_independent_snapshot() {
    let map: HopscotchMap<i32, i32> = HopscotchMap::new();
    for i in 0..50 {
        map.insert(i, i * 3);
    }
    let cloned = map.clone();
    assert_eq!(map, cloned);

    cloned.insert(999, 999);
    assert_eq!(map.get(&999), None);
    assert_ne!(map.len(), cloned.len());
}

#[test]
fn hashmap_clone_is_an_independent_snapshot() {
    let map: KHashMap<i32, i32> = KHashMap::new();
    for i in 0..50 {
        map.insert(i, i * 3);
    }
    let cloned = map.clone();
    assert_eq!(map, cloned);

    cloned.insert(999, 999);
    assert_eq!(map.get(&999), None);
    assert_ne!(map.len(), cloned.len());
}

/// A clone of a grown map is sized like its source and, once emptied, shrinks back exactly as
/// far as the source does, never holding the grown table forever.
#[test]
fn hopscotch_clone_of_a_grown_map_shrinks_like_its_source() {
    let map: HopscotchMap<u64, u64> = HopscotchMap::with_capacity(64);
    for i in 0..10_000 {
        map.insert(i, i);
    }
    let cloned = map.clone();
    assert_eq!(cloned.capacity(), map.capacity());
    for i in 0..10_000 {
        assert_eq!(map.remove(&i), Some(i));
        assert_eq!(cloned.remove(&i), Some(i));
    }
    assert_eq!(cloned.capacity(), map.capacity());
}

#[test]
fn hashmap_clone_of_a_grown_map_shrinks_like_its_source() {
    let map: KHashMap<u64, u64> = (0..10_000).map(|i| (i, i)).collect();
    let grown = map.capacity();
    let cloned = map.clone();
    assert_eq!(cloned.capacity(), grown);
    for i in 0..10_000 {
        assert_eq!(map.remove(&i), Some(i));
        assert_eq!(cloned.remove(&i), Some(i));
    }
    assert!(map.capacity() < grown, "the source shrinks back");
    assert_eq!(cloned.capacity(), map.capacity());

    // A sized map's floor survives the clone too.
    let sized: KHashMap<u64, u64> = KHashMap::with_capacity(4096);
    for i in 0..100 {
        sized.insert(i, i);
    }
    let cloned = sized.clone();
    for i in 0..100 {
        assert_eq!(cloned.remove(&i), Some(i));
    }
    assert_eq!(cloned.capacity(), 4096);
}

#[test]
fn hopscotch_extend_matches_std_owned_and_borrowed() {
    let mut map: HopscotchMap<i32, i32> = HopscotchMap::new();
    Extend::extend(&mut map, (0..20).map(|i| (i, i)));
    let borrowed: Vec<(i32, i32)> = (20..40).map(|i| (i, i)).collect();
    Extend::extend(&mut map, borrowed.iter().map(|(k, v)| (k, v)));

    let mut std_map: StdHashMap<i32, i32> = StdHashMap::new();
    std_map.extend((0..20).map(|i| (i, i)));
    std_map.extend(borrowed.iter().map(|(k, v)| (k, v)));

    assert_eq!(map.len(), std_map.len());
    for (k, v) in &std_map {
        assert_eq!(map.get(k), Some(*v));
    }
}

#[test]
fn hashmap_extend_matches_std_owned_and_borrowed() {
    let mut map: KHashMap<i32, i32> = KHashMap::new();
    Extend::extend(&mut map, (0..20).map(|i| (i, i)));
    let borrowed: Vec<(i32, i32)> = (20..40).map(|i| (i, i)).collect();
    Extend::extend(&mut map, borrowed.iter().map(|(k, v)| (k, v)));

    let mut std_map: StdHashMap<i32, i32> = StdHashMap::new();
    std_map.extend((0..20).map(|i| (i, i)));
    std_map.extend(borrowed.iter().map(|(k, v)| (k, v)));

    assert_eq!(map.len(), std_map.len());
    for (k, v) in &std_map {
        assert_eq!(map.get(k), Some(*v));
    }
}

// ---------------------------------------------------------------------------
// RapidHash: both maps proven with a BuildHasher other than the built-in one, including through
// a resize and a displacement (the write paths that must hash only with the map's own `S`).
// ---------------------------------------------------------------------------

#[test]
fn hopscotch_correct_under_rapidhash_single_threaded() {
    let map: HopscotchMap<i64, i64, RapidState> =
        HopscotchMap::with_capacity_and_hasher(64, RapidState::default());
    for i in 0..5000 {
        assert_eq!(map.insert(i, i * 7), None);
    }
    for i in 0..5000 {
        assert_eq!(map.get(&i), Some(i * 7));
    }
    assert_eq!(map.len(), 5000);
    for i in (0..5000).step_by(2) {
        assert_eq!(map.remove(&i), Some(i * 7));
    }
    assert_eq!(map.len(), 2500);
}

#[test]
fn hashmap_correct_under_rapidhash_single_threaded() {
    let map: KHashMap<i64, i64, RapidState> =
        KHashMap::with_capacity_and_hasher(64, RapidState::default());
    for i in 0..5000 {
        assert_eq!(map.insert(i, i * 7), None);
    }
    for i in 0..5000 {
        assert_eq!(map.get(&i), Some(i * 7));
    }
    assert_eq!(map.len(), 5000);
    for i in (0..5000).step_by(2) {
        assert_eq!(map.remove(&i), Some(i * 7));
    }
    assert_eq!(map.len(), 2500);
}

#[test]
fn hopscotch_correct_under_rapidhash_concurrent_resize() {
    let map: Arc<HopscotchMap<i64, i64, RapidState>> = Arc::new(
        HopscotchMap::with_capacity_and_hasher(64, RapidState::default()),
    );
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                for i in 0..2000 {
                    let k = t * 2000 + i;
                    map.insert(k, k * 2);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(map.len(), 16000);
    for k in 0..16000 {
        assert_eq!(map.get(&k), Some(k * 2));
    }
}

#[test]
fn hashmap_correct_under_rapidhash_concurrent_resize() {
    let map: Arc<KHashMap<i64, i64, RapidState>> = Arc::new(KHashMap::with_capacity_and_hasher(
        64,
        RapidState::default(),
    ));
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                for i in 0..2000 {
                    let k = t * 2000 + i;
                    map.insert(k, k * 2);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(map.len(), 16000);
    for k in 0..16000 {
        assert_eq!(map.get(&k), Some(k * 2));
    }
}

// ---------------------------------------------------------------------------
// The snapshot impls under concurrent writers: a clone taken while writers grow and shrink the
// map through several resizes holds every key present throughout with its value, nothing torn,
// no key twice, and equals its source once the writers stop.
// ---------------------------------------------------------------------------

macro_rules! snapshot_under_churn {
    ($name:ident, $map:ident) => {
        #[test]
        fn $name() {
            use std::collections::HashSet;
            use std::sync::atomic::{AtomicUsize, Ordering};

            const STABLE: u64 = 2_000;
            const CYCLE: u64 = 20_000;
            let map: Arc<$map<u64, u64, RapidState>> =
                Arc::new($map::with_capacity_and_hasher(64, RapidState::default()));
            for k in 0..STABLE {
                map.insert(k, k * 3);
            }
            // A fixed amount of writing (three grow-and-shrink cycles per writer) bounds the
            // garbage the run retires; the snapshots are taken for as long as the writers run.
            let writers_left = Arc::new(AtomicUsize::new(4));
            let writers: Vec<_> = (0..4u64)
                .map(|t| {
                    let (map, left) = (Arc::clone(&map), Arc::clone(&writers_left));
                    thread::spawn(move || {
                        let base = STABLE + t * CYCLE;
                        for i in 0..6 * CYCLE {
                            let k = base + i % CYCLE;
                            if (i / CYCLE) % 2 == 0 {
                                map.insert(k, k * 3);
                            } else {
                                map.remove(&k);
                            }
                        }
                        left.fetch_sub(1, Ordering::Release);
                    })
                })
                .collect();
            let mut snapshots = 0;
            while snapshots == 0 || writers_left.load(Ordering::Acquire) > 0 {
                snapshots += 1;
                let cloned = (*map).clone();
                let mut seen = HashSet::new();
                for (k, v) in cloned.iter() {
                    assert!(seen.insert(k), "key {k} yielded twice by a quiescent clone");
                    assert_eq!(v, k * 3, "key {k} cloned with a value it never held");
                }
                assert_eq!(cloned.len(), seen.len());
                for k in 0..STABLE {
                    assert_eq!(cloned.get(&k), Some(k * 3), "stable key {k} missing");
                }
                assert!(cloned == cloned.clone());
                assert!(format!("{cloned:?}").starts_with('{'));
            }
            for w in writers {
                w.join().unwrap();
            }
            let quiescent = (*map).clone();
            assert!(*map == quiescent);
            assert_eq!(quiescent.len(), map.len());
        }
    };
}

snapshot_under_churn!(hopscotch_snapshot_impls_under_churn, HopscotchMap);
snapshot_under_churn!(hashmap_snapshot_impls_under_churn, KHashMap);
