//! The conditional writes of `HashMap`: every method and every outcome, the closure run exactly
//! once for a present key (and, but for `compute`, never for an absent one), a closure or a
//! clone that panics leaving the map as it was with its held link released, and the writers of
//! a held link (the key's writers, an insert into the chain the hold ends, a resize) landing
//! only after the holder wrote, while readers pass the hold.

extern crate std;

use super::*;
use crate::test_support::{Fragile, Runs, panics, stalled};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::hash::Hasher;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;

/// How long a test waits for a stopped closure to arrive: far past any healthy run.
const MEET: Duration = Duration::from_secs(30);

/// Every key hashes to 7: every key of the map is in one chain, whatever its capacity.
#[derive(Clone, Copy, Default)]
struct OneChain;

struct SevenHasher;

impl Hasher for SevenHasher {
    fn finish(&self) -> u64 {
        7
    }

    fn write(&mut self, _: &[u8]) {}
}

impl BuildHasher for OneChain {
    type Hasher = SevenHasher;

    fn build_hasher(&self) -> SevenHasher {
        SevenHasher
    }
}

/// A one-chain map holding `(k, 10 * k)` for each of `keys`, in that order.
fn chain_map(keys: &[u64]) -> Arc<HashMap<u64, u64, OneChain>> {
    let map = Arc::new(HashMap::with_capacity_and_hasher(64, OneChain));
    for &k in keys {
        assert_eq!(map.insert(k, 10 * k), None);
    }
    map
}

fn sorted_keys<V: Clone, S>(map: &HashMap<u64, V, S>) -> Vec<u64> {
    let mut keys: Vec<u64> = map.iter().map(|(k, _)| k).collect();
    keys.sort_unstable();
    keys
}

#[test]
fn remove_if_answers_each_outcome() {
    let map = chain_map(&[1, 2]);
    let runs = Runs::default();
    let absent = map.remove_if(&3, |_| {
        runs.tick();
        true
    });
    assert_eq!(absent, None);
    assert_eq!(runs.take(), 0, "pred ran for an absent key");
    let refused = map.remove_if(&1, |v| {
        runs.tick();
        *v > 10
    });
    assert_eq!(refused, None);
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&1), Some(10), "a refused remove keeps the entry");
    assert_eq!(map.len(), 2);
    let removed = map.remove_if(&1, |v| {
        runs.tick();
        *v == 10
    });
    assert_eq!(removed, Some(10));
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&1), None);
    assert_eq!(map.len(), 1);
    assert_eq!(sorted_keys(&map), [2]);
}

#[test]
fn compare_and_remove_removes_only_the_expected_value() {
    let map = chain_map(&[1, 2]);
    assert_eq!(map.compare_and_remove(&1, &11), None);
    assert_eq!(map.compare_and_remove(&3, &30), None);
    assert_eq!(map.get(&1), Some(10));
    assert_eq!(map.compare_and_remove(&1, &10), Some(10));
    assert_eq!(map.compare_and_remove(&1, &10), None, "removed once");
    assert_eq!(map.len(), 1);
}

#[test]
fn replace_if_answers_each_outcome() {
    let map = chain_map(&[1, 2]);
    let runs = Runs::default();
    let absent = map.replace_if(3, 7, |_| {
        runs.tick();
        true
    });
    assert_eq!(absent, Err(None));
    assert_eq!(runs.take(), 0, "pred ran for an absent key");
    assert_eq!(map.get(&3), None, "an absent key is not inserted");
    let refused = map.replace_if(1, 7, |v| {
        runs.tick();
        *v > 10
    });
    assert_eq!(refused, Err(Some(10)));
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&1), Some(10));
    let replaced = map.replace_if(1, 7, |v| {
        runs.tick();
        *v == 10
    });
    assert_eq!(replaced, Ok(10));
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&1), Some(7));
    assert_eq!(map.len(), 2);
    assert_eq!(sorted_keys(&map), [1, 2]);
}

#[test]
fn compare_and_swap_answers_each_outcome() {
    let map = chain_map(&[1]);
    assert_eq!(map.compare_and_swap(1, &11, 5), Err(Some(10)));
    assert_eq!(map.compare_and_swap(2, &10, 5), Err(None));
    assert_eq!(map.compare_and_swap(1, &10, 5), Ok(10));
    assert_eq!(map.compare_and_swap(1, &10, 6), Err(Some(5)));
    assert_eq!(map.get(&1), Some(5));
    assert_eq!(map.get(&2), None);
    assert_eq!(map.len(), 1);
}

#[test]
fn compute_answers_each_outcome() {
    let map = chain_map(&[1]);
    let runs = Runs::default();
    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, None);
        None
    });
    assert_eq!(answer, None);
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), None);
    assert_eq!(map.len(), 1);
    assert_eq!(map.insert(4, 40), None, "the chain's end is not left held");

    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, None);
        Some(30)
    });
    assert_eq!(answer, Some(30));
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), Some(30));
    assert_eq!(map.len(), 3);

    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, Some(&30));
        seen.map(|v| v + 1)
    });
    assert_eq!(answer, Some(31));
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), Some(31));
    assert_eq!(map.len(), 3);

    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, Some(&31));
        None
    });
    assert_eq!(answer, None);
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), None);
    assert_eq!(map.len(), 2);
    assert_eq!(sorted_keys(&map), [1, 4]);
}

/// A compute that inserts counts its node and grows the table past three quarters, and a
/// remove_if or a compute that removes shrinks it under a quarter, as insert and remove do.
#[test]
fn conditional_writes_grow_and_shrink_the_table() {
    let map = HashMap::<u64, u64>::with_capacity(64);
    for k in 0..48 {
        assert_eq!(map.compute(k, |_| Some(k)), Some(k));
    }
    assert_eq!(map.capacity(), 64);
    assert_eq!(map.compute(48, |_| Some(48)), Some(48));
    assert_eq!(map.capacity(), 128, "the 49th entry grows the table");
    assert_eq!(map.len(), 49);
    for k in 0..17 {
        assert_eq!(map.remove_if(&k, |_| true), Some(k));
    }
    assert_eq!(map.capacity(), 128);
    assert_eq!(map.compute(17, |_| None), None);
    assert_eq!(map.len(), 31);
    assert_eq!(map.capacity(), 64, "31 entries shrink 128 buckets");
    for k in 18..49 {
        assert_eq!(map.get(&k), Some(k), "key {k}");
    }
}

/// The map as `fragile_map` made it (keys 1 and 2 in one chain, 3 absent), with every link a
/// hold could have taken released: the key's writes, and an insert at the chain's end, land.
fn assert_as_made(map: &HashMap<u64, Fragile, OneChain>, what: &str) {
    assert_eq!(map.len(), 2, "{what}");
    assert_eq!(map.get(&1), Some(Fragile(10)), "{what}");
    assert_eq!(map.get(&2), Some(Fragile(20)), "{what}");
    assert_eq!(map.get(&3), None, "{what}");
    assert_eq!(
        map.insert(2, Fragile(20)),
        Some(Fragile(20)),
        "{what}: key 2 not held"
    );
    assert_eq!(
        map.insert(3, Fragile(0)),
        None,
        "{what}: the chain's end not held"
    );
    assert_eq!(map.remove(&3), Some(Fragile(0)), "{what}");
}

fn fragile_map() -> HashMap<u64, Fragile, OneChain> {
    let map = HashMap::with_capacity_and_hasher(64, OneChain);
    map.insert(1, Fragile(10));
    map.insert(2, Fragile(20));
    map
}

/// A predicate that panics, and a clone of the value it would answer that panics, leave the map
/// as it was, the held node's link back as it was.
#[test]
fn a_panicking_remove_if_leaves_the_map_unchanged() {
    let map = fragile_map();
    panics(false, || {
        map.remove_if(&2, |_| panic!("pred"));
    });
    assert_as_made(&map, "remove_if, pred");
    panics(true, || {
        map.remove_if(&2, |_| true);
    });
    assert_as_made(&map, "remove_if, clone");
}

/// A predicate that panics, and a clone of the value it would answer that panics, leave the map
/// as it was, the held node's link back as it was.
#[test]
fn a_panicking_replace_if_leaves_the_map_unchanged() {
    let map = fragile_map();
    panics(false, || {
        let _ = map.replace_if(2, Fragile(1), |_| panic!("pred"));
    });
    assert_as_made(&map, "replace_if, pred");
    panics(true, || {
        let _ = map.replace_if(2, Fragile(1), |_| true);
    });
    assert_as_made(&map, "replace_if, clone");
}

/// A closure that panics, and a clone of the value a compute answers that panics, leave the map
/// as it was, for a present key and for an absent one: nothing linked twice or lost, the held
/// link (the node's, or the chain's end) back as it was.
#[test]
fn a_panicking_compute_leaves_the_map_unchanged() {
    let map = fragile_map();
    panics(false, || {
        map.compute(2, |_| panic!("f"));
    });
    assert_as_made(&map, "compute present, f");
    panics(true, || {
        map.compute(2, |_| Some(Fragile(1)));
    });
    assert_as_made(&map, "compute present, clone");
    panics(false, || {
        map.compute(3, |_| panic!("f"));
    });
    assert_as_made(&map, "compute absent, f");
    panics(true, || {
        map.compute(3, |_| Some(Fragile(1)));
    });
    assert_as_made(&map, "compute absent, clone");
}

/// While a remove_if's predicate runs, the key's node is held: a lookup and a walk pass the
/// hold, and a remove of the key lands only after the remove_if removed it (so it answers
/// `None`); without the hold it could remove the key first.
#[test]
fn a_write_of_the_key_waits_for_the_closure() {
    let map = chain_map(&[1, 2]);
    let (arrived, arrival) = sync_channel(1);
    let (release, go) = sync_channel(1);
    let holder = {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            let pause = stalled(arrived, go, true);
            map.remove_if(&1, move |_| pause())
        })
    };
    arrival.recv_timeout(MEET).expect("the predicate runs");
    assert_eq!(map.get(&1), Some(10), "a lookup passes the hold");
    assert_eq!(sorted_keys(&map), [1, 2], "a walk passes the hold");
    let remover = {
        let map = Arc::clone(&map);
        thread::spawn(move || map.remove(&1))
    };
    release.send(()).expect("let the predicate answer");
    assert_eq!(holder.join().expect("the remove_if"), Some(10));
    assert_eq!(remover.join().expect("the remove"), None, "it landed after");
    assert_eq!(map.len(), 1);
    assert_eq!(sorted_keys(&map), [2]);
}

/// A compute of an absent key holds the chain's end while its closure runs: an insert of the
/// key (and of any key of the chain) lands after the compute linked its node.
#[test]
fn an_insert_into_the_chain_waits_for_a_compute_of_an_absent_key() {
    for makes in [Some(1), None] {
        let map = chain_map(&[1, 2]);
        let (arrived, arrival) = sync_channel(1);
        let (release, go) = sync_channel(1);
        let computer = {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                let pause = stalled(arrived, go, makes);
                map.compute(5, move |seen| {
                    assert_eq!(seen, None);
                    pause()
                })
            })
        };
        arrival.recv_timeout(MEET).expect("the closure runs");
        assert_eq!(map.get(&5), None);
        let writers: Vec<_> = [(5, 50), (6, 60)]
            .into_iter()
            .map(|(k, v)| {
                let map = Arc::clone(&map);
                thread::spawn(move || map.insert(k, v))
            })
            .collect();
        release.send(()).expect("let the closure answer");
        assert_eq!(computer.join().expect("the compute"), makes);
        let answers: Vec<Option<u64>> = writers
            .into_iter()
            .map(|w| w.join().expect("an insert"))
            .collect();
        assert_eq!(
            answers,
            [makes, None],
            "{makes:?}: the insert of key 5 landed after"
        );
        assert_eq!(map.get(&5), Some(50));
        assert_eq!(sorted_keys(&map), [1, 2, 5, 6]);
        assert_eq!(map.len(), 4);
    }
}

/// A resize freezing a held link waits for its holder, so the conditional write lands in the
/// old table before the copy reads it.
#[test]
fn a_resize_waits_for_the_closure() {
    let map = chain_map(&[1, 2, 3]);
    let (arrived, arrival) = sync_channel(1);
    let (release, go) = sync_channel(1);
    let computer = {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            let pause = stalled(arrived, go, ());
            map.compute(2, move |seen| {
                pause();
                seen.map(|v| v + 1)
            })
        })
    };
    arrival.recv_timeout(MEET).expect("the closure runs");
    let resizer = {
        let map = Arc::clone(&map);
        thread::spawn(move || map.try_resize(128))
    };
    release.send(()).expect("let the closure answer");
    assert_eq!(computer.join().expect("the compute"), Some(21));
    resizer.join().expect("the resize");
    assert_eq!(map.capacity(), 128);
    assert_eq!(map.get(&2), Some(21), "the write was copied");
    assert_eq!(sorted_keys(&map), [1, 2, 3]);
    assert_eq!(map.len(), 3);
}
