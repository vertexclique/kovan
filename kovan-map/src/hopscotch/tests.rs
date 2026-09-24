//! Colocated coverage for the hopscotch module: the map's single-thread contract, its concurrent
//! stress cases, and deterministic replays of the interleavings a resize turns on (an insert
//! that lands just before the table it landed in is replaced, a resize meeting a writer that
//! holds a home bucket, a walk whose table is replaced under it) and those a displacement turns
//! on (a lookup, a claim, a remove and a walk racing the move of an entry), each stopped step by
//! step through `pause`.

use super::*;

#[test]
fn test_insert_and_get() {
    let map = HopscotchMap::new();
    assert_eq!(map.insert(1, 100), None);
    assert_eq!(map.get(&1), Some(100));
    assert_eq!(map.get(&2), None);
}

#[test]
fn test_growing() {
    let map = HopscotchMap::with_capacity(32);
    for i in 0..100 {
        map.insert(i, i * 2);
    }
    for i in 0..100 {
        assert_eq!(map.get(&i), Some(i * 2));
    }
}

#[test]
fn test_concurrent() {
    use alloc::sync::Arc;
    extern crate std;
    use std::thread;

    let map = Arc::new(HopscotchMap::with_capacity(64));
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
fn test_concurrent_insert_and_remove() {
    use alloc::sync::Arc;
    extern crate std;
    use std::thread;

    let map = Arc::new(HopscotchMap::with_capacity(64));

    // Phase 1: Pre-populate so removers have something to work with
    for thread_id in 0..4u64 {
        for i in 0..500u64 {
            let key = thread_id * 1000 + i;
            map.insert(key, key * 3);
        }
    }

    let mut insert_handles = alloc::vec::Vec::new();
    let mut remove_handles = alloc::vec::Vec::new();

    // Spawn inserter threads: each inserts keys in its own range
    for thread_id in 0..4u64 {
        let map_clone = Arc::clone(&map);
        insert_handles.push(thread::spawn(move || {
            for i in 0..500u64 {
                let key = thread_id * 1000 + i;
                map_clone.insert(key, key * 3);
            }
        }));
    }

    // Spawn remover threads: each removes keys from the same ranges,
    // racing with inserters
    for thread_id in 0..4u64 {
        let map_clone = Arc::clone(&map);
        remove_handles.push(thread::spawn(move || {
            for i in 0..500u64 {
                let key = thread_id * 1000 + i;
                if let Some(val) = map_clone.remove(&key) {
                    // Value must be correct if present
                    assert_eq!(val, key * 3);
                }
            }
        }));
    }

    for handle in insert_handles {
        handle.join().unwrap();
    }
    for handle in remove_handles {
        handle.join().unwrap();
    }

    // Verify: every remaining key has the correct value
    for thread_id in 0..4u64 {
        for i in 0..500u64 {
            let key = thread_id * 1000 + i;
            if let Some(val) = map.get(&key) {
                assert_eq!(val, key * 3);
            }
        }
    }
}

/// Regression: get_or_insert must not panic when a concurrent remove
/// deletes the key between the internal insert and the return.
#[test]
fn test_hopscotch_get_or_insert_concurrent_remove() {
    use alloc::sync::Arc;
    extern crate std;
    use std::sync::Barrier;
    use std::thread;

    let map = Arc::new(HopscotchMap::<u64, u64>::with_capacity(64));
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8u64)
        .map(|tid| {
            let map = map.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for i in 0..5000u64 {
                    let key = i % 32; // Small key space forces heavy contention
                    if tid % 2 == 0 {
                        // Half the threads do get_or_insert
                        let _ = map.get_or_insert(key, tid * 1000 + i);
                    } else {
                        // Other half remove
                        let _ = map.remove(&key);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join()
            .expect("Thread panicked during get_or_insert/remove race");
    }
}

extern crate std;

use super::pause::{self, Point, Stop};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::hash::Hasher;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// How long a test waits for a stopped thread to arrive: far past any healthy run, so a thread
/// that never arrives is a failure, never a slow machine.
const MEET: Duration = Duration::from_secs(30);

/// Run `op` on a thread of its own that stops at `point`. The caller waits on the returned
/// receiver for the arrival and lets the thread go through the returned sender.
fn stopped_at<T: Send + 'static>(
    point: Point,
    op: impl FnOnce() -> T + Send + 'static,
) -> (JoinHandle<T>, Receiver<()>, SyncSender<()>) {
    let (arrived, arrival) = sync_channel(1);
    let (release, go) = sync_channel(1);
    let handle = thread::spawn(move || {
        pause::arm(Stop {
            point,
            arrived,
            go,
        });
        op()
    });
    (handle, arrival, release)
}

/// A map of 64 buckets holding keys `0..30`.
fn thirty_entries() -> Arc<HopscotchMap<u64, u64>> {
    let map = Arc::new(HopscotchMap::with_capacity(64));
    for k in 0..30 {
        map.insert(k, k);
    }
    map
}

/// An insert that landed, whose table a resize replaces before the call returns, reports its
/// own insert: `insert_if_absent` answers `None` (it did not find the key, it put it there),
/// and `insert` answers `None` (no earlier value). Before the fix the call retried into the new
/// table, met its own migrated entry and answered `Some(its own value)`.
#[test]
fn an_insert_that_lands_before_a_resize_reports_its_own_insert() {
    for only_if_absent in [true, false] {
        let map = thirty_entries();
        let (writer, arrival, release) = {
            let map = Arc::clone(&map);
            stopped_at(Point::AfterLanding, move || {
                if only_if_absent {
                    map.insert_if_absent(1_000, 7)
                } else {
                    map.insert(1_000, 7)
                }
            })
        };
        arrival.recv_timeout(MEET).expect("the insert landed");
        map.try_resize(128);
        assert_eq!(
            map.capacity(),
            128,
            "the table the insert landed in is replaced"
        );
        release.send(()).expect("let the insert return");
        assert_eq!(
            writer.join().expect("the insert"),
            None,
            "only_if_absent {only_if_absent}: an insert that happened reports the key absent"
        );
        assert_eq!(map.get(&1_000), Some(7));
        assert_eq!(map.len(), 31);
        assert_eq!(map.iter().filter(|(k, _)| *k == 1_000).count(), 1);
    }
}

/// A resize waits for an insert that holds its home bucket before it copies a slot, so the
/// insert lands in the old table and is copied, never lost and never retried.
#[test]
fn a_resize_waits_for_the_insert_holding_its_home_bucket() {
    let map = thirty_entries();
    let (writer, writer_arrival, writer_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::InGuardBeforeClaim, move || {
            map.insert_if_absent(1_000, 7)
        })
    };
    writer_arrival
        .recv_timeout(MEET)
        .expect("the insert holds its home bucket and found its key absent");
    let (resizer, resizer_arrival, resizer_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::ResizerMetHeldGuard, move || map.try_resize(128))
    };
    resizer_arrival
        .recv_timeout(MEET)
        .expect("the resize waits for the writer holding a home bucket before it copies a slot");
    assert_eq!(
        map.capacity(),
        64,
        "no table is swapped in while a writer is between its scan and its claim"
    );
    resizer_release.send(()).expect("let the resize wait on");
    writer_release
        .send(())
        .expect("let the insert claim its slot");
    assert_eq!(writer.join().expect("the insert"), None);
    resizer.join().expect("the resize");
    assert_eq!(map.capacity(), 128);
    assert_eq!(
        map.get(&1_000),
        Some(7),
        "the insert is in the table that replaced its own"
    );
    assert_eq!(map.len(), 31);
}

/// A clear waits for an insert that holds its home bucket the same way, so the insert is either
/// cleared with the rest or lands after the clear with its hop bit, never left half there.
#[test]
fn a_clear_waits_for_the_insert_holding_its_home_bucket() {
    let map = thirty_entries();
    let (writer, writer_arrival, writer_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::InGuardBeforeClaim, move || {
            map.insert_if_absent(1_000, 7)
        })
    };
    writer_arrival
        .recv_timeout(MEET)
        .expect("the insert holds its home bucket");
    let (clearer, clearer_arrival, clearer_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::ResizerMetHeldGuard, move || map.clear())
    };
    clearer_arrival
        .recv_timeout(MEET)
        .expect("the clear waits for the writer holding a home bucket");
    clearer_release.send(()).expect("let the clear wait on");
    writer_release
        .send(())
        .expect("let the insert claim its slot");
    assert_eq!(writer.join().expect("the insert"), None);
    clearer.join().expect("the clear");
    assert_eq!(map.len(), 0);
    assert_eq!(
        map.get(&1_000),
        None,
        "the insert landed before the clear and was cleared"
    );
    assert_eq!(map.iter().count(), 0);
    // The cleared map takes the key again, once.
    assert_eq!(map.insert_if_absent(1_000, 8), None);
    assert_eq!(map.get(&1_000), Some(8));
}

/// Hashes a `u64` key to itself, so a test places every key in the bucket it names.
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

/// A walk whose table grows under it keeps walking the table it started on. Key 70's home is
/// bucket 6 in 64 buckets (it sits in slot 10, the first free slot of bucket 6's neighborhood)
/// and bucket 70 in 128: a walk that moved to the new table at its position would meet key 70
/// a second time.
#[test]
fn a_walk_keeps_its_table_across_a_growth() {
    let map = HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(64, Identity);
    for k in (0..10).chain([70]) {
        map.insert(k, k);
    }
    let mut walk = map.iter();
    let mut seen: Vec<u64> = walk.by_ref().take(11).map(|(k, _)| k).collect();
    assert_eq!(seen, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 70]);
    // Slots 20..58; the insert that makes 49 entries grows the table to 128 buckets.
    for k in 20..58 {
        map.insert(k, k);
    }
    assert_eq!(map.capacity(), 128);
    seen.extend(walk.map(|(k, _)| k));
    seen.sort_unstable();
    let want: Vec<u64> = (0..10).chain(20..58).chain([70]).collect();
    assert_eq!(
        seen, want,
        "every entry once: none repeated from the new table's layout"
    );
}

/// A walk whose table shrinks under it keeps walking the table it started on. Key 70 sits in
/// slot 70 of 128 buckets, ahead of the walk, and in slot 7 of 64 (home 6, first free slot),
/// behind it: a walk that moved to the new table at its position would never meet it.
#[test]
fn a_walk_keeps_its_table_across_a_shrink() {
    let map = HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(128, Identity);
    for k in (0..7).chain(8..40).chain([70]) {
        map.insert(k, k);
    }
    let mut walk = map.iter();
    let mut seen: Vec<u64> = walk.by_ref().take(20).map(|(k, _)| k).collect();
    assert_eq!(seen, (0..7).chain(8..21).collect::<Vec<u64>>());
    // The removal that leaves 31 entries, under a quarter of 128 buckets, shrinks the table.
    for k in 21..30 {
        assert_eq!(map.remove(&k), Some(k));
    }
    assert_eq!(map.capacity(), 64);
    seen.extend(walk.map(|(k, _)| k));
    seen.sort_unstable();
    let want: Vec<u64> = (0..7).chain(8..21).chain(30..40).chain([70]).collect();
    assert_eq!(
        seen, want,
        "every entry present throughout, once: none skipped"
    );
}

/// A 64-bucket map with the identity hash whose keys `0..=32` sit each in its home slot. Key
/// 64's home is bucket 0, whose whole neighborhood (slots 0..32) is taken, so inserting key 64
/// moves key 2 from slot 2 to slot 33 (the first free slot, and in key 2's neighborhood) and
/// takes slot 2.
fn displacing_layout() -> Arc<HopscotchMap<u64, u64, Identity>> {
    let map = Arc::new(HopscotchMap::with_capacity_and_hasher(64, Identity));
    for k in 0..=32 {
        map.insert(k, k);
    }
    map
}

/// The key in slot `idx` of the map's current table.
fn key_at(map: &HopscotchMap<u64, u64, Identity>, idx: usize) -> Option<u64> {
    let guard = pin();
    let table = unsafe { &*map.table.load(Ordering::Acquire, &guard).as_raw() };
    let slot = &table.get_bucket(idx).slot;
    let entry_ptr = slot.load(Ordering::Acquire, &guard);
    // SAFETY: null for a free slot, else loaded under `guard`, which keeps it from being freed.
    unsafe { entry_ptr.as_raw().as_ref() }.map(|entry| entry.key)
}

/// Every key a walk of the map yields, sorted.
fn walked_keys(map: &HopscotchMap<u64, u64, Identity>) -> Vec<u64> {
    let mut keys: Vec<u64> = map.iter().map(|(k, _)| k).collect();
    keys.sort_unstable();
    keys
}

/// The keys of `displacing_layout` and then `more`, sorted as `walked_keys` sorts them.
fn keys_and(more: &[u64]) -> Vec<u64> {
    (0..=32).chain(more.iter().copied()).collect()
}

/// The layout `displacing_layout` has once key 64 is in: key 2 moved from slot 2 to slot 33,
/// key 64 took slot 2, and the table did not grow.
fn assert_displaced(map: &HopscotchMap<u64, u64, Identity>) {
    assert_eq!(key_at(map, 2), Some(64), "key 64 took the slot key 2 left");
    assert_eq!(key_at(map, 33), Some(2), "key 2 moved to slot 33");
    assert_eq!(map.capacity(), 64, "the insert did not grow the table");
}

/// A lookup that read its home's hop bits before a displacement moved its key, and scans after
/// the move, finds the key's old slot taken by another key. The move stamp it then re-reads has
/// advanced, so it scans again and finds the key in its new slot. Before the fix it answered
/// `None` for a key present throughout.
#[test]
fn a_lookup_that_read_the_hop_bits_before_a_move_finds_the_moved_key() {
    let map = displacing_layout();
    let (reader, arrival, release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::LookupReadHops, move || map.get(&2))
    };
    arrival
        .recv_timeout(MEET)
        .expect("the lookup read key 2's hop bits");
    assert_eq!(map.insert(64, 64), None);
    assert_displaced(&map);
    release.send(()).expect("let the lookup scan");
    assert_eq!(reader.join().expect("the lookup"), Some(2));
}

/// Stopped at each step of a move (the entry linked in both slots; then unlinked from its old
/// slot, whose hop bit is still set), a lookup finds the entry and a walk yields every key once.
#[test]
fn a_move_keeps_its_entry_visible_to_lookups_and_walks_at_every_step() {
    for point in [Point::MoveLinkedTwice, Point::MoveUnlinked] {
        let map = displacing_layout();
        let (mover, arrival, release) = {
            let map = Arc::clone(&map);
            stopped_at(point, move || map.insert(64, 64))
        };
        arrival.recv_timeout(MEET).expect("the insert moves key 2");
        let in_old_slot = (point == Point::MoveLinkedTwice).then_some(2);
        assert_eq!(key_at(&map, 2), in_old_slot, "{point:?}");
        assert_eq!(key_at(&map, 33), Some(2), "{point:?}");
        assert_eq!(map.get(&2), Some(2), "{point:?}: a lookup missed the key");
        assert_eq!(walked_keys(&map), keys_and(&[]), "{point:?}: a walk");
        release.send(()).expect("let the move finish");
        assert_eq!(mover.join().expect("the insert"), None);
        assert_displaced(&map);
        assert_eq!(map.get(&2), Some(2));
        assert_eq!(walked_keys(&map), keys_and(&[64]));
    }
}

/// A claim of a key whose entry a displacement is moving waits for the move (which holds the
/// key's home guard) and then finds the key. Before the fix the claim scanned mid-move, missed
/// the entry and linked a second one.
#[test]
fn a_claim_of_a_key_being_moved_finds_it() {
    for point in [Point::MoveLinkedTwice, Point::MoveUnlinked] {
        let map = displacing_layout();
        let (mover, mover_arrival, mover_release) = {
            let map = Arc::clone(&map);
            stopped_at(point, move || map.insert(64, 64))
        };
        mover_arrival
            .recv_timeout(MEET)
            .expect("the insert moves key 2");
        let (claimer, claimer_arrival, claimer_release) = {
            let map = Arc::clone(&map);
            stopped_at(Point::WriterMetHeldGuard, move || {
                map.insert_if_absent(2, 99)
            })
        };
        claimer_arrival
            .recv_timeout(MEET)
            .expect("the claim waits for the move, which holds key 2's home guard");
        mover_release.send(()).expect("let the move finish");
        assert_eq!(mover.join().expect("the insert"), None);
        claimer_release.send(()).expect("let the claim go on");
        assert_eq!(claimer.join().expect("the claim"), Some(2), "{point:?}");
        assert_eq!(map.get(&2), Some(2));
        assert_eq!(map.len(), 34);
        assert_eq!(walked_keys(&map), keys_and(&[64]));
    }
}

/// Two claims of a key whose insert has to displace an entry have one winner: the second waits
/// for the first, which holds the key's home guard through the whole move.
#[test]
fn two_claims_of_a_key_that_needs_a_displacement_have_one_winner() {
    let map = displacing_layout();
    let (first, first_arrival, first_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::MoveLinkedTwice, move || map.insert_if_absent(64, 1))
    };
    first_arrival
        .recv_timeout(MEET)
        .expect("the first claim moves key 2");
    let (second, second_arrival, second_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::WriterMetHeldGuard, move || {
            map.insert_if_absent(64, 2)
        })
    };
    second_arrival
        .recv_timeout(MEET)
        .expect("the second claim waits for bucket 0's guard");
    first_release.send(()).expect("let the first claim finish");
    assert_eq!(first.join().expect("the first claim"), None);
    second_release.send(()).expect("let the second claim run");
    assert_eq!(second.join().expect("the second claim"), Some(1));
    assert_displaced(&map);
    assert_eq!(map.get(&64), Some(1));
    assert_eq!(map.len(), 34);
    assert_eq!(walked_keys(&map), keys_and(&[64]));
}

/// A remove of a key whose entry a displacement is moving waits for the move and removes the
/// key. Before the fix the remove could find the entry mid-move, lose its unlink to the move and
/// answer `None` for a present key.
#[test]
fn a_remove_racing_the_move_of_its_key_removes_it() {
    for point in [Point::MoveLinkedTwice, Point::MoveUnlinked] {
        let map = displacing_layout();
        let (mover, mover_arrival, mover_release) = {
            let map = Arc::clone(&map);
            stopped_at(point, move || map.insert(64, 64))
        };
        mover_arrival
            .recv_timeout(MEET)
            .expect("the insert moves key 2");
        let (remover, remover_arrival, remover_release) = {
            let map = Arc::clone(&map);
            stopped_at(Point::WriterMetHeldGuard, move || map.remove(&2))
        };
        remover_arrival
            .recv_timeout(MEET)
            .expect("the remove waits for the move");
        mover_release.send(()).expect("let the move finish");
        assert_eq!(mover.join().expect("the insert"), None);
        remover_release.send(()).expect("let the remove go on");
        assert_eq!(remover.join().expect("the remove"), Some(2), "{point:?}");
        assert_eq!(map.get(&2), None);
        assert_eq!(map.len(), 33);
        let want: Vec<u64> = (0..=32).filter(|k| *k != 2).chain([64]).collect();
        assert_eq!(walked_keys(&map), want);
    }
}

/// A walk that met key 2 in slot 2 before a displacement moved it to slot 33, ahead of the walk,
/// does not yield it again there: the move links the same entry, which the walk recognizes.
/// Before the fix the move linked a copy and the walk yielded key 2 twice.
#[test]
fn a_walk_meets_a_key_a_displacement_moved_ahead_of_it_once() {
    let map = displacing_layout();
    let mut walk = map.iter();
    let mut seen: Vec<u64> = walk.by_ref().take(3).map(|(k, _)| k).collect();
    assert_eq!(seen, [0, 1, 2]);
    assert_eq!(map.insert(64, 64), None);
    assert_displaced(&map);
    seen.extend(walk.map(|(k, _)| k));
    seen.sort_unstable();
    assert_eq!(seen, keys_and(&[]), "key 2 once: not met again in slot 33");
}

/// A walk meets a key once even when a displacement moves it ahead of the walk and an update
/// then replaces its entry there with a new one: the walk recognizes the key.
#[test]
fn a_walk_meets_a_key_once_when_it_is_moved_ahead_and_updated() {
    let map = displacing_layout();
    let mut walk = map.iter();
    let mut seen: Vec<(u64, u64)> = walk.by_ref().take(3).collect();
    assert_eq!(seen, [(0, 0), (1, 1), (2, 2)]);
    assert_eq!(map.insert(64, 64), None);
    assert_displaced(&map);
    assert_eq!(map.insert(2, 200), Some(2), "key 2 is replaced in slot 33");
    seen.extend(walk);
    seen.sort_unstable();
    let want: Vec<(u64, u64)> = (0..=32).map(|k| (k, k)).collect();
    assert_eq!(seen, want, "key 2 once, as the walk first met it");
}

/// A displacement whose first candidate's home guard is held by another writer moves the next
/// candidate instead of growing the table, and never waits for the held guard. Key 66's home is
/// bucket 2, key 2's home.
#[test]
fn a_displacement_moves_another_entry_when_one_home_is_held() {
    let map = displacing_layout();
    let (holder, holder_arrival, holder_release) = {
        let map = Arc::clone(&map);
        stopped_at(Point::InGuardBeforeClaim, move || {
            map.insert_if_absent(66, 66)
        })
    };
    holder_arrival
        .recv_timeout(MEET)
        .expect("the insert of key 66 holds bucket 2's guard");
    // Key 2 cannot move while its home is held, so key 3 moves from slot 3 to slot 33.
    assert_eq!(map.insert(64, 64), None);
    assert_eq!(key_at(&map, 3), Some(64));
    assert_eq!(key_at(&map, 33), Some(3));
    assert_eq!(map.capacity(), 64, "a held home is not a full table");
    holder_release.send(()).expect("let key 66 claim a slot");
    assert_eq!(holder.join().expect("the insert of key 66"), None);
    // Key 66's neighborhood (slots 2..34) is full now, so its insert moves key 4 to slot 34.
    assert_eq!(key_at(&map, 4), Some(66));
    assert_eq!(key_at(&map, 34), Some(4));
    assert_eq!(map.capacity(), 64);
    assert_eq!(map.len(), 35);
    for k in keys_and(&[64, 66]) {
        assert_eq!(map.get(&k), Some(k), "key {k}");
    }
    assert_eq!(walked_keys(&map), keys_and(&[64, 66]));
}
