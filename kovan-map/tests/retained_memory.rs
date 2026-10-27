//! What the maps hold while they work (see kovan's
//! `tests/support/retained.rs`): `HashMap` and `HopscotchMap` with values
//! from 16 bytes to 64 KiB, replaced on a fixed set of keys, inserted and
//! removed, and grown and emptied again (each growth retires the old
//! table), and a run four times longer. One test, so no other test
//! allocates beside it.

#[path = "../../kovan/tests/support/retained.rs"]
mod retained;

use std::sync::Arc;

use kovan_map::{HashMap, HopscotchMap};
use retained::{Held, report, run};

const THREADS: [usize; 2] = [2, 8];
/// Keys each thread replaces values on.
const KEYS: u64 = 64;

fn rounds_for(size: usize) -> usize {
    if size >= 1 << 16 { 4_000 } else { 20_000 }
}

macro_rules! map_workloads {
    ($map:ident, $replace:ident, $churn:ident, $grow:ident) => {
        /// Each thread replaces the value of one of its own `KEYS` keys a
        /// round.
        fn $replace<const S: usize>(threads: usize, rounds: usize) -> Held {
            let map = Arc::new($map::<u64, [u8; S]>::new());
            run(threads, rounds, move |t, rounds| {
                for i in 0..rounds {
                    let key = t as u64 * KEYS + i as u64 % KEYS;
                    std::hint::black_box(map.insert(key, [i as u8; S]).is_some());
                }
            })
        }

        /// Each thread inserts a key of its own and removes it a round.
        fn $churn<const S: usize>(threads: usize, rounds: usize) -> Held {
            let map = Arc::new($map::<u64, [u8; S]>::new());
            run(threads, rounds, move |t, rounds| {
                for i in 0..rounds {
                    let key = t as u64 * KEYS + i as u64 % KEYS;
                    map.insert(key, [i as u8; S]);
                    std::hint::black_box(map.remove(&key).is_some());
                }
            })
        }

        /// Each thread fills 1024 keys of its own and empties them again,
        /// `rounds / 1024` times: the table grows (and retires the old one).
        fn $grow<const S: usize>(threads: usize, rounds: usize) -> Held {
            let map = Arc::new($map::<u64, [u8; S]>::new());
            run(threads, rounds, move |t, rounds| {
                for pass in 0..(rounds / 1024).max(1) {
                    let base = (t as u64) << 32;
                    for k in 0..1024 {
                        map.insert(base + k, [pass as u8; S]);
                    }
                    for k in 0..1024 {
                        std::hint::black_box(map.remove(&(base + k)).is_some());
                    }
                }
            })
        }
    };
}

map_workloads!(HashMap, hash_replace, hash_churn, hash_grow);
map_workloads!(HopscotchMap, hop_replace, hop_churn, hop_grow);

macro_rules! sizes {
    ($f:ident, $threads:expr, $name:expr) => {{
        report(
            $name,
            16,
            $threads,
            rounds_for(16),
            $f::<16>($threads, rounds_for(16)),
        );
        report(
            $name,
            256,
            $threads,
            rounds_for(256),
            $f::<256>($threads, rounds_for(256)),
        );
        report(
            $name,
            4096,
            $threads,
            rounds_for(4096),
            $f::<4096>($threads, rounds_for(4096)),
        );
        report(
            $name,
            65536,
            $threads,
            rounds_for(65536),
            $f::<65536>($threads, rounds_for(65536)),
        );
    }};
}

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn what_the_maps_hold_while_they_work() {
    for threads in THREADS {
        sizes!(hash_replace, threads, "hashmap replace");
        sizes!(hash_churn, threads, "hashmap insert/remove");
        sizes!(hash_grow, threads, "hashmap grow/empty");
        sizes!(hop_replace, threads, "hopscotch replace");
        sizes!(hop_churn, threads, "hopscotch insert/remove");
        sizes!(hop_grow, threads, "hopscotch grow/empty");
    }
    for rounds in [20_000, 80_000] {
        report(
            "hashmap replace (run length)",
            256,
            8,
            rounds,
            hash_replace::<256>(8, rounds),
        );
    }
}
