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
use super::table::GUARD;
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
        pause::arm(Stop { point, arrived, go });
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

/// Hashes a `u64` key below 128 to itself with its seven bits repeated at the top of the hash:
/// the key's bucket is the one it names, as under [`Identity`], and keys of different buckets
/// differ in their top bits too, which a walk compares first.
#[derive(Clone, Copy, Default)]
struct Tagged;

struct TaggedHasher(u64);

impl Hasher for TaggedHasher {
    fn finish(&self) -> u64 {
        self.0 | self.0 << 57
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

impl BuildHasher for Tagged {
    type Hasher = TaggedHasher;

    fn build_hasher(&self) -> TaggedHasher {
        TaggedHasher(0)
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
fn key_at<S>(map: &HopscotchMap<u64, u64, S>, idx: usize) -> Option<u64> {
    let guard = pin();
    let table = unsafe { &*map.table.load(Ordering::Acquire, &guard).as_raw() };
    let word = table.get_bucket(idx).load(Ordering::Acquire, &guard);
    word.entry().map(|entry| entry.key)
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

/// A test that gives up on a thread it paused inside a move (it drops the channel that would
/// release it) leaves the map whole: the thread finishes the move, so no entry stays linked in
/// two slots and dropping the map frees every entry once.
#[test]
fn an_abandoned_pause_inside_a_move_leaves_the_map_whole() {
    for point in [Point::MoveLinkedTwice, Point::MoveUnlinked] {
        let map = displacing_layout();
        let (mover, mover_arrival, mover_release) = {
            let map = Arc::clone(&map);
            stopped_at(point, move || map.insert(64, 64))
        };
        mover_arrival
            .recv_timeout(MEET)
            .expect("the insert moves key 2");
        drop(mover_release);
        assert_eq!(
            mover.join().expect("the insert finishes"),
            None,
            "{point:?}"
        );
        assert_eq!(map.get(&2), Some(2), "{point:?}");
        assert_eq!(map.get(&64), Some(64), "{point:?}");
        assert_eq!(map.len(), 34, "{point:?}");
        assert_eq!(walked_keys(&map), keys_and(&[64]), "{point:?}");
        drop(map);
    }
}

/// A claim of a key whose entry a displacement is moving finds the key, at either point of the
/// move, without waiting for the move's home guard: its lookup rescans when the move stamp
/// changed. Before the fix the claim scanned mid-move, missed the entry and linked a second one.
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
        // Mid-move, with the move holding key 2's home guard: both claims find key 2 by the
        // lookup `get` makes and answer without waiting for the guard.
        assert_eq!(map.insert_if_absent(2, 99), Some(2), "{point:?}");
        assert_eq!(map.get_or_insert(2, 98), 2, "{point:?}");
        mover_release.send(()).expect("let the move finish");
        assert_eq!(mover.join().expect("the insert"), None);
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

/// A walk that met key 2 in slot 2 through `next`, and folds the rest after a displacement moved
/// key 2 to slot 33, does not count it again there: the fold recognizes what `next` met.
#[test]
fn a_fold_after_next_meets_a_key_a_displacement_moved_ahead_of_it_once() {
    let map = displacing_layout();
    let mut walk = map.iter();
    let seen: Vec<u64> = walk.by_ref().take(3).map(|(k, _)| k).collect();
    assert_eq!(seen, [0, 1, 2]);
    assert_eq!(map.insert(64, 64), None);
    assert_displaced(&map);
    // Keys 3..=32 in slots 3..=32, and key 2 in slot 33 once more.
    assert_eq!(walk.count(), 30, "key 2 once: not met again in slot 33");
}

/// A 64-bucket map whose keys `32..64` sit each in its home slot (under `Identity` or `Tagged`),
/// so the neighborhood of bucket 32 (slots 32..64) is full and slots 64.. are free.
fn upper_half_layout<S: BuildHasher + Default>() -> HopscotchMap<u64, u64, S> {
    let map = HopscotchMap::with_capacity_and_hasher(64, S::default());
    for k in 32..64 {
        map.insert(k, k);
    }
    map
}

/// How many times each key comes out of a fold over `map` whose closure runs `write` once, at
/// the first entry it gets.
fn fold_counts<S: BuildHasher>(
    map: &HopscotchMap<u64, u64, S>,
    write: impl FnOnce(),
) -> std::collections::BTreeMap<u64, usize> {
    let mut write = Some(write);
    map.iter()
        .fold(std::collections::BTreeMap::new(), |mut counts, (k, _)| {
            if let Some(write) = write.take() {
                write();
            }
            *counts.entry(k).or_insert(0) += 1;
            counts
        })
}

/// A fold meets a key once when a displacement, made from the fold's own closure, moves the key
/// from a slot the fold may already have read to one it reads later: inserting key 96 (home
/// bucket 32) moves key 33 from slot 33 to slot 64, the first slot of the table's second group
/// of `u64::BITS` slots, which the fold reads after it yields the first.
fn a_fold_meets_a_key_a_displacement_moved_ahead_of_it_once<S: BuildHasher + Default>() {
    let map = upper_half_layout::<S>();
    let counts = fold_counts(&map, || {
        assert_eq!(map.insert(96, 96), None);
        assert_eq!(
            key_at(&map, 33),
            Some(96),
            "key 96 took the slot key 33 left"
        );
        assert_eq!(key_at(&map, 64), Some(33), "key 33 moved to slot 64");
    });
    for k in 32..64 {
        assert_eq!(
            counts.get(&k),
            Some(&1),
            "key {k}, present throughout, once"
        );
    }
    assert!(counts.values().all(|&n| n == 1), "no key twice: {counts:?}");
}

#[test]
fn a_fold_meets_a_key_a_displacement_moved_ahead_of_it_once_identity() {
    a_fold_meets_a_key_a_displacement_moved_ahead_of_it_once::<Identity>();
}

#[test]
fn a_fold_meets_a_key_a_displacement_moved_ahead_of_it_once_tagged() {
    a_fold_meets_a_key_a_displacement_moved_ahead_of_it_once::<Tagged>();
}

/// A fold meets a key once when the fold's own closure removes it from a slot the fold may
/// already have read and inserts it again into one the fold reads later: key 127 (home bucket
/// 63) takes slot 63 first, so key 63 lands in slot 64. The new entry is another allocation of
/// the same key, which the fold recognizes by comparing keys.
fn a_fold_meets_a_key_reinserted_ahead_of_it_once<S: BuildHasher + Default>() {
    let map = upper_half_layout::<S>();
    let counts = fold_counts(&map, || {
        assert_eq!(map.remove(&63), Some(63));
        assert_eq!(map.insert(127, 127), None);
        assert_eq!(map.insert(63, 630), None);
        assert_eq!(key_at(&map, 63), Some(127));
        assert_eq!(key_at(&map, 64), Some(63), "key 63 again, in slot 64");
    });
    for k in 32..64 {
        assert_eq!(counts.get(&k), Some(&1), "key {k} once");
    }
    assert!(counts.values().all(|&n| n == 1), "no key twice: {counts:?}");
}

#[test]
fn a_fold_meets_a_key_reinserted_ahead_of_it_once_identity() {
    a_fold_meets_a_key_reinserted_ahead_of_it_once::<Identity>();
}

#[test]
fn a_fold_meets_a_key_reinserted_ahead_of_it_once_tagged() {
    a_fold_meets_a_key_reinserted_ahead_of_it_once::<Tagged>();
}

std::thread_local! {
    /// The clones of [`Clones`] values made on this thread.
    static CLONED: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}

/// A key or value that counts its clones on the thread that makes them.
#[derive(PartialEq, Eq, Hash)]
struct Clones(u64);

impl Clone for Clones {
    fn clone(&self) -> Self {
        CLONED.with(|cloned| cloned.set(cloned.get() + 1));
        Self(self.0)
    }
}

/// The clones of [`Clones`] values `walk` makes.
fn clones_in(walk: impl FnOnce()) -> usize {
    let before = CLONED.with(core::cell::Cell::get);
    walk();
    CLONED.with(core::cell::Cell::get) - before
}

/// A walk of the keys clones no value and a walk of the values no key, through `next` and
/// through `fold` alike; a walk of the entries clones both.
#[test]
fn keys_and_values_clone_only_what_they_yield() {
    let by_key = HopscotchMap::<Clones, u64>::new();
    let by_value = HopscotchMap::<u64, Clones>::new();
    for k in 0..100 {
        by_key.insert(Clones(k), k);
        by_value.insert(k, Clones(k));
    }
    assert_eq!(clones_in(|| assert_eq!(by_value.keys().count(), 100)), 0);
    assert_eq!(
        clones_in(|| assert_eq!(by_value.keys().collect::<Vec<_>>().len(), 100)),
        0
    );
    assert_eq!(clones_in(|| assert_eq!(by_key.values().count(), 100)), 0);
    assert_eq!(
        clones_in(|| assert_eq!(by_key.values().collect::<Vec<_>>().len(), 100)),
        0
    );
    assert_eq!(clones_in(|| assert_eq!(by_key.keys().count(), 100)), 100);
    assert_eq!(clones_in(|| assert_eq!(by_value.iter().count(), 100)), 100);
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

/// The identity hash of a key with the tag `tag` (the hash's top bits) and the low bits `low`.
fn tagged(tag: u64, low: u64) -> u64 {
    (tag << 60) | low
}

/// The scan a writer makes under its home's guard (`find_held`, which protects no entry it reads)
/// answers what a reader's scan (`find`) answers: each key of the home at its slot, past entries
/// of the home with another tag or with the same tag and another key, and nothing for an absent
/// key, whether its tag matches an entry's or none. Home 0's keys sit in slots 0..4 and key 1 of
/// home 1 in slot 4, which home 0's bits do not name.
#[test]
fn a_scan_under_the_home_guard_answers_as_a_reader_scan() {
    let map = HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(64, Identity);
    let home_0 = [tagged(0, 0), tagged(1, 64), tagged(1, 128), tagged(2, 0)];
    for k in home_0.into_iter().chain([1]) {
        assert_eq!(map.insert(k, k), None);
    }
    let guard = pin();
    let table = unsafe { &*map.table.load(Ordering::Acquire, &guard).as_raw() };
    let home = table.home_guard(0).expect("no writer holds home 0");
    assert_eq!(home.hops(), 0b1111, "home 0's keys in slots 0..4");
    let key_of = |found: Option<(usize, Word<'_, u64, u64>)>| {
        found.map(|(offset, word)| (offset, word.entry().map(|entry| entry.key)))
    };
    for (offset, k) in home_0.into_iter().enumerate() {
        let held = key_of(table.find_held(&home, k, &k, &guard));
        assert_eq!(held, Some((offset, Some(k))), "key {k:#x}");
        assert_eq!(held, key_of(table.find(0, home.hops(), k, &k, &guard)));
    }
    for k in [tagged(1, 192), tagged(3, 0), 64] {
        assert_eq!(
            key_of(table.find_held(&home, k, &k, &guard)),
            None,
            "absent key {k:#x}"
        );
        assert_eq!(key_of(table.find(0, home.hops(), k, &k, &guard)), None);
    }
    drop(home);
    for k in home_0.into_iter().chain([1]) {
        assert_eq!(map.remove(&k), Some(k), "key {k:#x}");
    }
    assert!(map.is_empty());
}

/// A writer takes a home's guard from one read of its control word: the guard it gets names the
/// home's bits as they were when it took it, a refused or held home is left as it was, and a
/// remove of a key whose home has no bits answers `None` even while another writer holds that
/// home, without waiting for it.
#[test]
fn a_home_guard_is_taken_from_one_read_of_the_control_word() {
    let map = Arc::new(HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(64, Identity));
    // Home 3 holds keys 3 and 67 in slots 3 and 4; home 5 has no bits.
    for k in [3, 67] {
        assert_eq!(map.insert(k, k), None);
    }
    let guard = pin();
    let table = unsafe { &*map.table.load(Ordering::Acquire, &guard).as_raw() };
    let control = |idx: usize| table.get_bucket(idx).control.load(Ordering::Relaxed);
    let before = control(3);
    assert_eq!(hop_bits(before), 0b11);

    let refused = table.home_guard_unless(3, |word| hop_bits(word) != 0);
    assert_eq!(refused.err(), Some(before), "refused with the word it read");
    assert_eq!(control(3), before, "a refused home is left as it was");

    let held = table
        .home_guard_unless(3, |_| false)
        .expect("no writer holds home 3");
    assert_eq!(held.hops(), 0b11);
    assert_eq!(control(3), before | GUARD);
    let met = table.home_guard_unless(3, |_| false);
    assert_eq!(
        met.err(),
        Some(before | GUARD),
        "a held home answers the word it read"
    );
    assert_eq!(control(3), before | GUARD, "a held home is left as it was");
    drop(held);
    assert_eq!(control(3), before, "the release clears the guard bit alone");

    let empty = table.home_guard(5).expect("no writer holds home 5");
    let (done, answer) = sync_channel(1);
    let remover = {
        let map = Arc::clone(&map);
        thread::spawn(move || done.send(map.remove(&5)).expect("the test waits"))
    };
    assert_eq!(
        answer
            .recv_timeout(MEET)
            .expect("the remove answers while home 5 is held"),
        None
    );
    remover.join().expect("the remove");
    drop(empty);
    assert_eq!(map.remove(&67), Some(67));
    assert_eq!(map.remove(&3), Some(3));
    assert!(map.is_empty());
}

/// A writer's scan under its home guard reads the home's entries without protecting them
/// (`find_held`), so no other thread may unlink, retire or move one of them, or replace the
/// table holding them, until the holder is done with them. Each writer that scans that way
/// (a remove, a replace, a claim of an absent key, all of home 2 in `displacing_layout`) is
/// stopped right after its scan loaded key 2's word and before it read key 2's entry; then a
/// remove and a replace of key 2 wait for the guard, a resize and a clear wait for it before
/// they copy or clear a slot (the table is not replaced), and a displacement that would move key
/// 2 moves key 3 instead, without waiting. Every answer and the final contents are a sequential
/// map's, the holder's write first.
#[test]
fn a_held_scan_holds_off_every_writer_that_could_retire_an_entry_it_read() {
    use std::collections::BTreeMap;

    #[derive(Clone, Copy, Debug)]
    enum Op {
        Remove(u64),
        Insert(u64, u64),
        Claim(u64, u64),
        Resize,
        Clear,
    }
    fn apply(map: &HopscotchMap<u64, u64, Identity>, op: Op) -> Option<u64> {
        match op {
            Op::Remove(k) => map.remove(&k),
            Op::Insert(k, v) => map.insert(k, v),
            Op::Claim(k, v) => map.insert_if_absent(k, v),
            Op::Resize => {
                map.try_resize(128);
                None
            }
            Op::Clear => {
                map.clear();
                None
            }
        }
    }
    fn model(want: &mut BTreeMap<u64, u64>, op: Op) -> Option<u64> {
        match op {
            Op::Remove(k) => want.remove(&k),
            Op::Insert(k, v) => want.insert(k, v),
            Op::Claim(k, v) => match want.get(&k) {
                Some(&present) => Some(present),
                None => want.insert(k, v),
            },
            Op::Resize => None,
            Op::Clear => {
                want.clear();
                None
            }
        }
    }

    let holders = [Op::Remove(2), Op::Insert(2, 200), Op::Claim(66, 66)];
    let rivals = [
        Op::Remove(2),
        Op::Insert(2, 7),
        Op::Resize,
        Op::Clear,
        Op::Insert(64, 64),
    ];
    for holder_op in holders {
        for rival_op in rivals {
            let case = std::format!("holder {holder_op:?}, rival {rival_op:?}");
            let map = displacing_layout();
            let mut want: BTreeMap<u64, u64> = (0..=32).map(|k| (k, k)).collect();
            let (holder, arrival, release) = {
                let map = Arc::clone(&map);
                stopped_at(Point::HeldScanLoaded, move || apply(&map, holder_op))
            };
            arrival
                .recv_timeout(MEET)
                .unwrap_or_else(|_| panic!("{case}: the holder's scan loaded key 2's word"));
            let holder_want = model(&mut want, holder_op);

            let rival = if let Op::Insert(64, _) = rival_op {
                // Key 64's home, bucket 0, has its neighborhood full: its insert moves the entry
                // of the farthest slot it can into slot 33. Key 2's home is held, so key 3 moves.
                assert_eq!(apply(&map, rival_op), None, "{case}");
                assert_eq!(
                    key_at(&map, 3),
                    Some(64),
                    "{case}: key 64 took key 3's slot"
                );
                assert_eq!(key_at(&map, 33), Some(3), "{case}: key 3 moved, not key 2");
                None
            } else {
                let point = match rival_op {
                    Op::Resize | Op::Clear => Point::ResizerMetHeldGuard,
                    _ => Point::WriterMetHeldGuard,
                };
                let map = Arc::clone(&map);
                let (rival, rival_arrival, rival_release) =
                    stopped_at(point, move || apply(&map, rival_op));
                rival_arrival
                    .recv_timeout(MEET)
                    .unwrap_or_else(|_| panic!("{case}: the rival waits for home 2's guard"));
                Some((rival, rival_release))
            };
            assert_eq!(key_at(&map, 2), Some(2), "{case}: key 2's entry stays put");
            assert_eq!(map.capacity(), 64, "{case}: the table is not replaced");

            release.send(()).expect("let the holder read key 2's entry");
            assert_eq!(holder.join().expect("the holder"), holder_want, "{case}");
            let rival_want = model(&mut want, rival_op);
            if let Some((rival, rival_release)) = rival {
                rival_release.send(()).expect("let the rival go on");
                assert_eq!(rival.join().expect("the rival"), rival_want, "{case}");
            }

            let mut got: Vec<(u64, u64)> = map.iter().collect();
            got.sort_unstable();
            assert_eq!(got, want.into_iter().collect::<Vec<_>>(), "{case}");
            assert_eq!(map.len(), got.len(), "{case}");
            for (k, v) in got {
                assert_eq!(map.get(&k), Some(v), "{case}: key {k}");
            }
            let grown = matches!(rival_op, Op::Resize);
            assert_eq!(map.capacity(), if grown { 128 } else { 64 }, "{case}");
        }
    }
}

mod conditional;
