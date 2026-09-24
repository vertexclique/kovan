//! Shuttle-searched interleavings of `HopscotchMap` against a resize its own writes trigger: a
//! once-only claim (`insert_if_absent`) racing the grow, and a walk racing it. The exact
//! interleavings each fix closes are replayed step by step by the unit tests in
//! `src/hopscotch_tests.rs`; this searches the schedules around them.
//!
//! What each property pins:
//! - A claim that landed while a grow copied the table is final: the claimer is told the key
//!   was absent (it put it there), the key is in the new table once, and the count is exact.
//!   Before the fix the claim retried into the new table, met its own copied entry and was told
//!   the key already existed.
//! - A walk whose table a grow replaces keeps walking the table it started on: every key
//!   present for the whole walk is yielded exactly once. Before the fix the walk carried its
//!   slot position into the new table, whose layout differs, and repeated keys.
//!
//! Both use an identity hasher and keys whose neighborhoods never fill, so no insert displaces
//! an entry (displacement is the one way a walk may yield an entry twice, documented on
//! `HopscotchMap::iter`), and in the walk keys 100..128 sit in different slots before and after
//! the grow.
//!
//! Like `shuttle_resize.rs`, this is a sampled search, not an exhaustive one: a clean run means
//! the sampled schedules held, a red run is a real finding (replay it with the printed
//! schedule).

#![cfg(feature = "shuttle")]

use core::hash::{BuildHasher, Hasher};
use kovan_map::HopscotchMap;
use std::sync::Arc;

/// Hashes a `u64` key to itself, so the test places every key in the bucket it names.
#[derive(Clone, Copy, Default)]
struct Identity;

struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 << 8) | u64::from(*byte);
        }
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

impl BuildHasher for Identity {
    type Hasher = IdentityHasher;

    fn build_hasher(&self) -> IdentityHasher {
        IdentityHasher(0)
    }
}

fn claim_races_a_grow() {
    let map = HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(64, Identity);
    let map = Arc::new(map);
    // 48 entries in 64 buckets: the next entry crosses three quarters and grows the table. The
    // claimed key's home is bucket 40 in 64 buckets (its first free slot is within reach) and
    // bucket 104 in 128.
    for k in 0..48 {
        map.insert(k, k);
    }
    let claimer = {
        let map = Arc::clone(&map);
        shuttle::thread::spawn(move || map.insert_if_absent(1_000, 7))
    };
    let grower = {
        let map = Arc::clone(&map);
        shuttle::thread::spawn(move || {
            for k in 48..60 {
                map.insert(k, k);
            }
        })
    };
    grower.join().unwrap();
    assert_eq!(
        claimer.join().unwrap(),
        None,
        "the only claim of the key was told it already existed"
    );
    assert_eq!(map.get(&1_000), Some(7));
    assert_eq!(map.len(), 61);
    assert_eq!(map.iter().filter(|(k, _)| *k == 1_000).count(), 1);
}

fn walk_races_a_grow() {
    let map = HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(64, Identity);
    let map = Arc::new(map);
    // Keys 100..128 have homes 36..64 in 64 buckets and 100..128 in 128; keys 128..140 have
    // homes 0..12 in both. 40 entries, then the grower's 9 keys (homes 12..21) make 49 and
    // grow the table.
    for k in 100..140 {
        map.insert(k, k);
    }
    let walker = {
        let map = Arc::clone(&map);
        shuttle::thread::spawn(move || {
            let mut seen: Vec<u64> = map.iter().map(|(k, _)| k).collect();
            seen.sort_unstable();
            seen
        })
    };
    let grower = {
        let map = Arc::clone(&map);
        shuttle::thread::spawn(move || {
            for k in 12..21 {
                map.insert(k, k);
            }
        })
    };
    grower.join().unwrap();
    let seen = walker.join().unwrap();
    assert_eq!(map.capacity(), 128, "the grower's keys grew the table");
    let mut once = seen.clone();
    once.dedup();
    assert_eq!(once, seen, "a walk yielded a key twice");
    for k in 100..140 {
        assert!(
            seen.binary_search(&k).is_ok(),
            "key {k}, present for the whole walk, was skipped"
        );
    }
}

#[test]
fn shuttle_hopscotch_claim_races_a_grow() {
    shuttle::check_random(claim_races_a_grow, 2_000);
}

#[test]
fn shuttle_hopscotch_walk_races_a_grow() {
    shuttle::check_random(walk_races_a_grow, 2_000);
}
