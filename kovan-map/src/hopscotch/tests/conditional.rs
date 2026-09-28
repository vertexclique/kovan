//! The conditional writes of `HopscotchMap`: every method and every outcome, the closure run
//! exactly once for a present key (and, but for `compute`, never for an absent one), a closure
//! or a clone that panics leaving the map as it was with its home guard released, the slot a
//! `compute` of an absent key reserves as other threads meet it, and a write of the key and a
//! resize waiting for a closure that runs under the key's home guard.

extern crate std;

use super::*;
use crate::test_support::{Fragile, Runs, panics, stalled};

/// A 64-bucket map under the identity hash holding `(k, 10 * k)` for each of `keys`.
fn identity_map(keys: &[u64]) -> Arc<HopscotchMap<u64, u64, Identity>> {
    let map = Arc::new(HopscotchMap::with_capacity_and_hasher(64, Identity));
    for &k in keys {
        assert_eq!(map.insert(k, 10 * k), None);
    }
    map
}

/// The control word of bucket `idx` of the map's current table.
fn control_of<V>(map: &HopscotchMap<u64, V, Identity>, idx: usize) -> u64 {
    let guard = pin();
    let table = unsafe { &*map.table.load(Ordering::Acquire, &guard).as_raw() };
    table.get_bucket(idx).control.load(Ordering::Acquire)
}

/// Whether slot `idx` of the map's current table holds any word (an entry or a reservation).
fn slot_taken<V>(map: &HopscotchMap<u64, V, Identity>, idx: usize) -> bool {
    let guard = pin();
    let table = unsafe { &*map.table.load(Ordering::Acquire, &guard).as_raw() };
    !table.looks_free(idx, &guard)
}

#[test]
fn remove_if_answers_each_outcome() {
    let map = identity_map(&[1, 2]);
    let runs = Runs::default();
    // Key 3's home is empty; key 65's home (1) holds key 1: absent either way, `pred` not run.
    for absent in [3, 65] {
        let answer = map.remove_if(&absent, |_| {
            runs.tick();
            true
        });
        assert_eq!(answer, None, "absent key {absent}");
        assert_eq!(runs.take(), 0, "absent key {absent}: pred ran");
    }
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
    assert_eq!(walked_keys(&map), [2]);
}

#[test]
fn compare_and_remove_removes_only_the_expected_value() {
    let map = identity_map(&[1, 2]);
    assert_eq!(map.compare_and_remove(&1, &11), None);
    assert_eq!(map.compare_and_remove(&3, &30), None);
    assert_eq!(map.get(&1), Some(10));
    assert_eq!(map.compare_and_remove(&1, &10), Some(10));
    assert_eq!(map.compare_and_remove(&1, &10), None, "removed once");
    assert_eq!(map.len(), 1);
}

/// While a remove_if's predicate runs, the key's home is held: a remove of the key meets the held
/// guard and lands only after the remove_if removed it (so it answers `None`), and a lookup
/// answers the value the predicate is looking at, without waiting.
#[test]
fn a_write_of_the_key_waits_for_the_predicate() {
    let map = identity_map(&[5]);
    let (arrived, arrival) = sync_channel(1);
    let (release, go) = sync_channel(1);
    let holder = {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            let pause = stalled(arrived, go, true);
            map.remove_if(&5, move |_| pause())
        })
    };
    arrival.recv_timeout(MEET).expect("the predicate runs");
    assert_eq!(map.get(&5), Some(50), "a lookup does not wait");
    let (remover, remover_arrival, remover_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::WriterMetHeldGuard, move || map.remove(&5))
    };
    remover_arrival
        .recv_timeout(MEET)
        .expect("the remove meets the home the remove_if holds");
    remover_release.send(()).expect("let the remove spin on");
    release.send(()).expect("let the predicate answer");
    assert_eq!(holder.join().expect("the remove_if"), Some(50));
    assert_eq!(remover.join().expect("the remove"), None, "it landed after");
    assert!(map.is_empty());
}

#[test]
fn replace_if_answers_each_outcome() {
    let map = identity_map(&[1, 2]);
    let runs = Runs::default();
    for absent in [3, 65] {
        let answer = map.replace_if(absent, 7, |_| {
            runs.tick();
            true
        });
        assert_eq!(answer, Err(None), "absent key {absent}");
        assert_eq!(runs.take(), 0, "absent key {absent}: pred ran");
        assert_eq!(map.get(&absent), None, "an absent key is not inserted");
    }
    assert_eq!(map.len(), 2);
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
    assert_eq!(walked_keys(&map), [1, 2]);
}

#[test]
fn compare_and_swap_answers_each_outcome() {
    let map = identity_map(&[1]);
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
    let map = identity_map(&[1]);
    let runs = Runs::default();
    let home_3 = control_of(&map, 3);

    // Absent, left absent: the reserved slot is given back.
    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, None);
        None
    });
    assert_eq!(answer, None);
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), None);
    assert_eq!(
        control_of(&map, 3),
        home_3,
        "the reserved slot's bit is taken back"
    );
    assert!(!slot_taken(&map, 3), "the reserved slot is free again");
    assert_eq!(map.len(), 1);

    // Absent, inserted.
    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, None);
        Some(30)
    });
    assert_eq!(answer, Some(30));
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), Some(30));
    assert_eq!(map.len(), 2);

    // Present, replaced.
    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, Some(&30));
        seen.map(|v| v + 1)
    });
    assert_eq!(answer, Some(31));
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), Some(31));
    assert_eq!(map.len(), 2);

    // Present, removed.
    let answer = map.compute(3, |seen| {
        runs.tick();
        assert_eq!(seen, Some(&31));
        None
    });
    assert_eq!(answer, None);
    assert_eq!(runs.take(), 1);
    assert_eq!(map.get(&3), None);
    assert_eq!(map.len(), 1);
    assert_eq!(walked_keys(&map), [1]);
    assert_eq!(control_of(&map, 3), home_3);
}

/// Key 64's home (0) has its whole neighborhood taken in `displacing_layout`: a compute that may
/// insert it moves key 2 to slot 33 first, as an insert would, and reserves slot 2. A compute
/// that then makes no entry leaves the content as it was (the move stays: it changed no key).
#[test]
fn a_compute_of_an_absent_key_in_a_full_neighborhood_displaces_first() {
    let map = displacing_layout();
    assert_eq!(map.compute(128, |_| None), None);
    assert_eq!(key_at(&map, 33), Some(2), "the reservation moved key 2");
    assert_eq!(key_at(&map, 2), None, "and gave slot 2 back");
    assert_eq!(map.get(&128), None);
    assert_eq!(map.len(), 33);
    assert_eq!(walked_keys(&map), keys_and(&[]));

    let map = displacing_layout();
    assert_eq!(map.compute(64, |_| Some(64)), Some(64));
    assert_displaced(&map);
    assert_eq!(map.get(&64), Some(64));
    assert_eq!(map.get(&2), Some(2));
    assert_eq!(map.len(), 34);
    assert_eq!(walked_keys(&map), keys_and(&[64]));
}

/// A compute that inserts counts its entry and grows the table past three quarters, and a
/// remove_if or a compute that removes shrinks it under a quarter, as insert and remove do.
#[test]
fn conditional_writes_grow_and_shrink_the_table() {
    let map = HopscotchMap::<u64, u64>::with_capacity(64);
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

/// The map as `fragile_map` made it: keys 1 and 65 (home 1, slots 1 and 2) with values 10 and
/// 650, key 3 absent, home 1's and home 3's words as they were, and every write of those homes
/// going through (so no guard was left held).
fn assert_as_made(map: &HopscotchMap<u64, Fragile, Identity>, words: (u64, u64), what: &str) {
    assert_eq!(map.len(), 2, "{what}");
    assert_eq!(map.get(&1), Some(Fragile(10)), "{what}");
    assert_eq!(map.get(&65), Some(Fragile(650)), "{what}");
    assert_eq!(map.get(&3), None, "{what}");
    assert_eq!((control_of(map, 1), control_of(map, 3)), words, "{what}");
    assert!(!slot_taken(map, 3), "{what}: no slot left reserved");
    assert_eq!(
        map.insert(129, Fragile(0)),
        None,
        "{what}: home 1 is not held"
    );
    assert_eq!(map.remove(&129), Some(Fragile(0)), "{what}");
    assert_eq!(
        map.insert(3, Fragile(0)),
        None,
        "{what}: home 3 is not held"
    );
    assert_eq!(map.remove(&3), Some(Fragile(0)), "{what}");
}

fn fragile_map() -> (HopscotchMap<u64, Fragile, Identity>, (u64, u64)) {
    let map = HopscotchMap::with_capacity_and_hasher(64, Identity);
    map.insert(1, Fragile(10));
    map.insert(65, Fragile(650));
    let words = (control_of(&map, 1), control_of(&map, 3));
    (map, words)
}

/// A predicate that panics, and a clone of the value it would answer that panics, leave the map
/// as it was: nothing unlinked, the home guard released.
#[test]
fn a_panicking_remove_if_leaves_the_map_unchanged() {
    let (map, words) = fragile_map();
    panics(false, || {
        map.remove_if(&65, |_| panic!("pred"));
    });
    assert_as_made(&map, words, "remove_if, pred");
    panics(true, || {
        map.remove_if(&65, |_| true);
    });
    assert_as_made(&map, words, "remove_if, clone");
}

/// A predicate that panics, and a clone of the value it would answer that panics, leave the map
/// as it was: nothing linked, the home guard released.
#[test]
fn a_panicking_replace_if_leaves_the_map_unchanged() {
    let (map, words) = fragile_map();
    panics(false, || {
        let _ = map.replace_if(65, Fragile(1), |_| panic!("pred"));
    });
    assert_as_made(&map, words, "replace_if, pred");
    panics(true, || {
        let _ = map.replace_if(65, Fragile(1), |_| true);
    });
    assert_as_made(&map, words, "replace_if, clone");
}

/// A closure that panics, and a clone of the value a compute answers that panics, leave the map
/// as it was, for a present key and for an absent one: nothing linked twice or lost, no slot left
/// reserved, the home guard released.
#[test]
fn a_panicking_compute_leaves_the_map_unchanged() {
    let (map, words) = fragile_map();
    panics(false, || {
        map.compute(65, |_| panic!("f"));
    });
    assert_as_made(&map, words, "compute present, f");
    panics(true, || {
        map.compute(65, |_| Some(Fragile(1)));
    });
    assert_as_made(&map, words, "compute present, clone");
    panics(false, || {
        map.compute(3, |_| panic!("f"));
    });
    assert_as_made(&map, words, "compute absent, f");
    panics(true, || {
        map.compute(3, |_| Some(Fragile(1)));
    });
    assert_as_made(&map, words, "compute absent, clone");
}

/// While a compute's closure runs, the key's home is held: an insert of the key meets the held
/// guard and lands only after the compute wrote, and a lookup answers the value the closure is
/// looking at, without waiting.
#[test]
fn a_write_of_the_key_waits_for_the_closure() {
    let map = identity_map(&[5]);
    let (arrived, arrival) = sync_channel(1);
    let (release, go) = sync_channel(1);
    let computer = {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            let pause = stalled(arrived, go, ());
            map.compute(5, move |seen| {
                pause();
                seen.map(|v| v + 1)
            })
        })
    };
    arrival.recv_timeout(MEET).expect("the closure runs");
    assert_eq!(map.get(&5), Some(50), "a lookup does not wait");
    let (writer, writer_arrival, writer_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::WriterMetHeldGuard, move || map.insert(5, 7))
    };
    writer_arrival
        .recv_timeout(MEET)
        .expect("the insert meets the home the compute holds");
    writer_release.send(()).expect("let the insert spin on");
    release.send(()).expect("let the closure finish");
    assert_eq!(computer.join().expect("the compute"), Some(51));
    assert_eq!(
        writer.join().expect("the insert"),
        Some(51),
        "the insert lands after the compute"
    );
    assert_eq!(map.get(&5), Some(7));
}

/// A resize waits for the home a closure runs under, like any writer's: the compute's entry
/// lands in the old table and is copied.
#[test]
fn a_resize_waits_for_the_closure() {
    let map = thirty_entries();
    let (arrived, arrival) = sync_channel(1);
    let (release, go) = sync_channel(1);
    let computer = {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            let pause = stalled(arrived, go, 7);
            map.compute(1_000, move |seen| {
                assert_eq!(seen, None);
                Some(pause())
            })
        })
    };
    arrival.recv_timeout(MEET).expect("the closure runs");
    let (resizer, resizer_arrival, resizer_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::ResizerMetHeldGuard, move || map.try_resize(128))
    };
    resizer_arrival
        .recv_timeout(MEET)
        .expect("the resize waits for the home the closure runs under");
    assert_eq!(map.capacity(), 64);
    resizer_release.send(()).expect("let the resize wait on");
    release.send(()).expect("let the closure finish");
    assert_eq!(computer.join().expect("the compute"), Some(7));
    resizer.join().expect("the resize");
    assert_eq!(map.capacity(), 128);
    assert_eq!(map.get(&1_000), Some(7), "the entry was copied");
    assert_eq!(map.len(), 31);
}

/// A compute of an absent key holding its reserved slot (keys 0 and 1 in slots 0 and 1, key 64
/// reserving slot 2): the reservation is no entry to a lookup or a walk, and taken to a writer,
/// which puts key 2 (home 2) in slot 3 instead; filled, it links key 64 there. A compute that
/// makes no entry gives the slot back.
#[test]
fn a_reserved_slot_is_no_entry_to_readers_and_taken_to_writers() {
    for makes in [Some(640), None] {
        let map = identity_map(&[0, 1]);
        let before = control_of(&map, 0);
        let (computer, arrival, release) = {
            let map = Arc::clone(&map);
            stopped_at(Point::ComputeReserved, move || {
                map.compute(64, move |seen| {
                    assert_eq!(seen, None);
                    makes
                })
            })
        };
        arrival
            .recv_timeout(MEET)
            .expect("the compute reserved its slot");
        assert!(slot_taken(&map, 2), "{makes:?}: slot 2 reserved");
        assert_eq!(
            key_at(&map, 2),
            None,
            "{makes:?}: a reservation names no entry"
        );
        assert_eq!(map.get(&64), None, "{makes:?}");
        assert_eq!(walked_keys(&map), [0, 1], "{makes:?}");
        assert_eq!(map.insert(2, 20), None);
        assert_eq!(
            key_at(&map, 3),
            Some(2),
            "{makes:?}: an insert passes the reserved slot"
        );
        assert_eq!(map.remove(&2), Some(20), "{makes:?}: home 2 is not held");
        release.send(()).expect("let the closure run");
        assert_eq!(computer.join().expect("the compute"), makes, "{makes:?}");
        assert_eq!(map.get(&64), makes, "{makes:?}");
        match makes {
            Some(_) => {
                assert_eq!(key_at(&map, 2), Some(64));
                assert_eq!(walked_keys(&map), [0, 1, 64]);
            }
            None => {
                assert!(!slot_taken(&map, 2), "the slot is given back");
                assert_eq!(control_of(&map, 0), before, "and its bit");
                assert_eq!(walked_keys(&map), [0, 1]);
            }
        }
    }
}
