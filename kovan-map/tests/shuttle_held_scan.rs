//! Shuttle-searched interleavings of `HopscotchMap` writers whose scans under a home guard read
//! the home's entries without protecting them: a remove and an insert scan the slots the home's
//! hop bits name while holding the home's writer guard, and rely on no other thread retiring an
//! entry of the home before they are done with it (every remove, replace, move, resize and clear
//! of the home's entries holds that guard).
//!
//! Three threads replace, remove and claim five keys that all share one home, so every scan
//! under the guard reads every entry, and each thread retires past kovan's `EPOCH_FREQ` (128
//! retires per thread move the global epoch), so an entry a scan reads can be younger than the
//! epoch its thread last published, and kovan would free it without waiting for that thread if
//! another thread retired it. Each value checks on every clone and drop that it was not dropped
//! before, and every value made must be dropped exactly once once the map is dropped and kovan
//! flushed: a retire done twice or never fails the count, a read of a freed value fails its
//! clone. That read is caught only when nothing reused the memory first, and the window it needs
//! (a whole batch of entries younger than the reader's epoch, retired and freed between its load
//! and its use) is narrow: a variant whose lookups read unprotected passed this search at the
//! default count. The exclusion itself is pinned by the unit test that stops a scan under the
//! guard (`a_held_scan_holds_off_every_writer_that_could_retire_an_entry_it_read`) and by
//! `tla/hopscotch` (`HeldUse`, `HS_mut_held_scan`).
//!
//! The scenario has a binary of its own: an exact count of the values dropped needs every kovan
//! thread of the process to have ended, and the tests of one binary run side by side.
//!
//! Like `shuttle_resize.rs`, this is a sampled search, not an exhaustive one: a clean run means
//! the sampled schedules held, a red run is a real finding (replay it with the printed seed).
//! It runs under random scheduling and under PCT (depth 3), `KOVAN_SHUTTLE_ITERS` schedules each
//! (default 2,000).

#![cfg(feature = "shuttle")]

use core::hash::{BuildHasher, Hasher};
use kovan_map::HopscotchMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Every key hashes to 7: the keys share home 7, and a tag, so a scan of the home reads every
/// entry.
#[derive(Clone, Copy, Default)]
struct OneHome;

struct SevenHasher;

impl Hasher for SevenHasher {
    fn finish(&self) -> u64 {
        7
    }

    fn write(&mut self, _bytes: &[u8]) {}
}

impl BuildHasher for OneHome {
    type Hasher = SevenHasher;

    fn build_hasher(&self) -> SevenHasher {
        SevenHasher
    }
}

/// A value that counts every value made and dropped, and fails a clone or a drop of a value
/// already dropped (a read of a freed entry's value, unless its memory was reused meanwhile).
struct Counted {
    live: bool,
    made: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Counted {
    fn new(made: &Arc<AtomicUsize>, dropped: &Arc<AtomicUsize>) -> Self {
        made.fetch_add(1, Ordering::Relaxed);
        Self {
            live: true,
            made: Arc::clone(made),
            dropped: Arc::clone(dropped),
        }
    }
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        assert!(self.live, "a value read after it was dropped");
        Self::new(&self.made, &self.dropped)
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        assert!(self.live, "a value dropped twice");
        self.live = false;
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Writes per thread: past `EPOCH_FREQ` retires, as two of every three writes retire an entry
/// when the key is present.
const WRITES: u64 = 400;

const KEYS: u64 = 5;

fn held_scans_race_retires() {
    let made = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let map =
        Arc::new(HopscotchMap::<u64, Counted, OneHome>::with_capacity_and_hasher(64, OneHome));
    let writers: Vec<_> = (0..3u64)
        .map(|t| {
            let (map, made, dropped) = (Arc::clone(&map), Arc::clone(&made), Arc::clone(&dropped));
            shuttle::thread::spawn(move || {
                for i in 0..WRITES {
                    let k = (t + i) % KEYS;
                    // A replace and a remove scan the home under its guard and retire the entry
                    // they find; a claim of a present key clones its value.
                    match (t + i) % 3 {
                        0 => drop(map.insert(k, Counted::new(&made, &dropped))),
                        1 => drop(map.remove(&k)),
                        _ => drop(map.get_or_insert(k, Counted::new(&made, &dropped))),
                    }
                }
                kovan::flush();
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    assert!(map.len() as u64 <= KEYS);
    assert_eq!(map.capacity(), 64);
    drop(map);
    // Drains any batch still parked in a slot, as `shuttle_resize.rs` does.
    for _ in 0..16 {
        kovan::flush();
    }
    assert_eq!(
        made.load(Ordering::Relaxed),
        dropped.load(Ordering::Relaxed),
        "every value made is dropped once"
    );
}

/// Schedules per strategy: `KOVAN_SHUTTLE_ITERS`, as for `shuttle_maps.rs`, or 2,000.
fn iterations() -> usize {
    std::env::var("KOVAN_SHUTTLE_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000)
}

#[test]
fn shuttle_hopscotch_held_scans_race_retires() {
    let n = iterations();
    shuttle::check_random(held_scans_race_retires, n);
    shuttle::check_pct(held_scans_race_retires, n, 3);
}
