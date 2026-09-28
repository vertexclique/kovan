//! The contract both maps keep under concurrency, each property checked against `HashMap` and
//! `HopscotchMap` (a module per property, a test per map): conditional writes answer exactly,
//! every write that reports a previous value reports the one it replaced, no entry is lost,
//! duplicated or resurrected, a walk yields every entry present for its whole length once, the
//! count is exact at rest, every value is dropped exactly once, and recorded concurrent
//! histories linearize. Resizes run in the middle of all of it (maps start at 64 buckets and
//! grow and shrink under the writes). `KOVAN_STRESS_SCALE` multiplies the rounds.

#[path = "support/lin.rs"]
mod lin;
#[path = "support/maps.rs"]
mod maps;
#[path = "support/stress.rs"]
mod stress;

#[path = "concurrent/conditional.rs"]
mod conditional;

use kovan_map::{HashMap, HopscotchMap};
use lin::{Call, Event, REFUSED, linearizable, perform};
use maps::{Clustered, Constant, Grouped, Identity, Map};
use std::collections::HashMap as StdMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use stress::{Counts, Rng, Tracked, scaled, serial, settle};

type Fold = foldhash::fast::FixedState;

/// Run `f(thread)` on `n` threads started together; their results in thread order.
fn together<T: Send + 'static>(n: usize, f: impl Fn(usize) -> T + Send + Sync + 'static) -> Vec<T> {
    let f = Arc::new(f);
    let start = Arc::new(Barrier::new(n));
    let handles: Vec<_> = (0..n)
        .map(|t| {
            let f = Arc::clone(&f);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                f(t)
            })
        })
        .collect();
    handles
        .into_iter()
        .map(|h| h.join().expect("a worker panicked"))
        .collect()
}

/// `insert_if_absent` answers `None` exactly when its call inserted. Each thread claims keys no
/// other thread claims, so every call inserts, and must say so, while the map grows under them
/// (a claim that landed before a grow's copy used to retry in the new table, meet its own entry
/// there and answer `Some(its own value)`).
mod insert_if_absent_answers_none_for_every_insert_across_grows {
    use super::*;

    fn run<M: Map<u64, u64>>() {
        let _serial = serial();
        const THREADS: usize = 16;
        let per = scaled(2_000) as u64;
        for round in 0..4 {
            let map = Arc::new(M::with_capacity(64));
            let m = Arc::clone(&map);
            let wrong: Vec<u64> = together(THREADS, move |t| {
                let base = t as u64 * per;
                (base..base + per)
                    .filter(|&k| m.insert_if_absent(k, k + 1).is_some())
                    .count() as u64
            });
            let total: u64 = wrong.iter().sum();
            assert_eq!(
                total,
                0,
                "{} round {round}: {total} inserts answered as present",
                M::NAME
            );
            assert_eq!(
                map.len(),
                THREADS * per as usize,
                "{} round {round}: count",
                M::NAME
            );
            for k in 0..THREADS as u64 * per {
                assert_eq!(
                    map.get(&k),
                    Some(k + 1),
                    "{} round {round}: key {k}",
                    M::NAME
                );
            }
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>();
    }
}

/// Threads race `insert_if_absent` and `get_or_insert` for the same keys while the map grows:
/// every key has exactly one winner (the one call answered `None`, or the `get_or_insert` that
/// answered its own value), every other call answers the winner's value, `get` returns it, and
/// every value made (the winners', the losers' and every clone) is dropped exactly once.
mod racing_claims_have_one_winner_across_grows {
    use super::*;

    fn run<M: Map<u64, Tracked>>() {
        let _serial = serial();
        const THREADS: usize = 8;
        const KEYS: u64 = 512;
        for round in 0..scaled(20) {
            let counts = Counts::new();
            {
                let map = Arc::new(M::with_capacity(64));
                let (m, c) = (Arc::clone(&map), Arc::clone(&counts));
                // Per thread, per key: (answer, whether this call won).
                let answers: Vec<Vec<(u64, bool)>> = together(THREADS, move |t| {
                    (0..KEYS)
                        .map(|k| {
                            let own = (t as u64 + 1) * 1_000_000 + k;
                            if (t + k as usize).is_multiple_of(2) {
                                match m.insert_if_absent(k, c.value(own)) {
                                    None => (own, true),
                                    Some(v) => (v.id, false),
                                }
                            } else {
                                let v = m.get_or_insert(k, c.value(own));
                                (v.id, v.id == own)
                            }
                        })
                        .collect()
                });
                for k in 0..KEYS as usize {
                    let winners: Vec<u64> =
                        answers.iter().filter(|a| a[k].1).map(|a| a[k].0).collect();
                    assert_eq!(
                        winners.len(),
                        1,
                        "{} round {round} key {k}: winners {winners:?}",
                        M::NAME
                    );
                    for (t, a) in answers.iter().enumerate() {
                        assert_eq!(
                            a[k].0,
                            winners[0],
                            "{} round {round} key {k} thread {t}",
                            M::NAME
                        );
                    }
                    assert_eq!(map.get(&(k as u64)).map(|v| v.id), Some(winners[0]));
                }
                assert_eq!(map.len(), KEYS as usize);
            }
            settle(&counts);
            assert!(counts.balanced(), "{} round {round}: {counts:?}", M::NAME);
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, Tracked, Fold>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, Tracked, Fold>>();
    }
}

/// `get_or_insert` that inserts answers its own value, even when a concurrent `insert`
/// replaced it before the call returned: the replace reporting this call's value as the one it
/// replaced proves this call inserted, and then its answer must be its own value.
mod get_or_insert_answers_its_own_value_when_it_inserts {
    use super::*;

    fn run<M: Map<u64, u64>>() {
        let _serial = serial();
        const KEYS: u64 = 4_000;
        let mut seen_race = 0usize;
        for round in 0..scaled(20) {
            let map = Arc::new(M::with_capacity(8_192));
            let m = Arc::clone(&map);
            // Both threads take the keys in the same order: each key is claimed and replaced at
            // about the same moment. Per key: the claim's answer, or the value the replace saw.
            let out = together(2, move |t| {
                (0..KEYS)
                    .map(|k| {
                        if t == 0 {
                            m.get_or_insert(k, 1)
                        } else {
                            m.insert(k, 2).unwrap_or(0)
                        }
                    })
                    .collect::<Vec<u64>>()
            });
            for (k, (claimed, replaced)) in out[0].iter().zip(&out[1]).enumerate() {
                if *replaced == 1 {
                    seen_race += 1;
                    assert_eq!(
                        *claimed,
                        1,
                        "{} round {round} key {k}: inserted 1, answered {claimed}",
                        M::NAME
                    );
                }
            }
        }
        assert!(
            seen_race > 0,
            "{}: the replace never saw the claim's value",
            M::NAME
        );
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>();
    }
}

/// Inserts, replaces and removes race while the map grows and shrinks; at rest the count is the
/// number of entries a walk yields and `get` finds (a remove that landed before a grow's copy
/// used to retry and remove the copy too, counting the key twice).
mod count_is_exact_at_rest_after_writes_race_resizes {
    use super::*;

    fn run<M: Map<u64, u64>>() {
        let _serial = serial();
        const THREADS: usize = 8;
        for round in 0..scaled(10) {
            let map = Arc::new(M::with_capacity(64));
            let m = Arc::clone(&map);
            together(THREADS, move |t| {
                let mut rng = Rng::new((round * THREADS + t) as u64);
                for _ in 0..4_000 {
                    let k = rng.below(1_024);
                    match rng.below(4) {
                        0 | 1 => {
                            m.insert(k, k);
                        }
                        2 => {
                            m.remove(&k);
                        }
                        _ => {
                            m.insert_if_absent(k, k);
                        }
                    }
                }
            });
            let walked = map.entries();
            let present = (0..1_024u64).filter(|k| map.get(k).is_some()).count();
            assert_eq!(
                walked.len(),
                present,
                "{} round {round}: walk vs lookups",
                M::NAME
            );
            assert_eq!(map.len(), present, "{} round {round}: count", M::NAME);
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>();
    }
}

/// A key a thread removed is gone for that thread: `get` and `contains_key` right after the
/// remove answer absent (no thread inserts it again). With every key of a `HashMap` in one
/// chain, adjacent removes race their unlinks (a lookup used to answer from a deleted node whose
/// unlink failed).
mod a_removed_key_stays_removed {
    use super::*;

    fn run<M: Map<u64, u64>>(per: u64) {
        let _serial = serial();
        const THREADS: usize = 8;
        for round in 0..scaled(20) {
            let map = Arc::new(M::with_capacity(64));
            let m = Arc::clone(&map);
            together(THREADS, move |t| {
                for i in 0..per {
                    let k = i * THREADS as u64 + t as u64;
                    m.insert(k, k);
                    if k.is_multiple_of(3) {
                        continue;
                    }
                    assert_eq!(
                        m.remove(&k),
                        Some(k),
                        "{} round {round}: key {k} present",
                        M::NAME
                    );
                    assert_eq!(
                        m.get(&k),
                        None,
                        "{} round {round}: removed key {k} found",
                        M::NAME
                    );
                    assert!(
                        !m.contains_key(&k),
                        "{} round {round}: removed key {k}",
                        M::NAME
                    );
                }
            });
            for k in 0..per * THREADS as u64 {
                let want = k.is_multiple_of(3).then_some(k);
                assert_eq!(map.get(&k), want, "{} round {round}: key {k}", M::NAME);
            }
        }
    }

    #[test]
    fn hashmap_one_chain() {
        run::<HashMap<u64, u64, Constant>>(40);
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>(400);
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>(400);
    }
}

/// A walk yields every key present for its whole length exactly once while other threads
/// replace those keys' values, insert and remove other keys, and grow and shrink the table (a
/// walk used to carry its slot index into the new table, and a replace in two steps used to hide
/// the key from a walk passing between them). HopscotchMap yields no key twice at all.
mod a_walk_yields_every_steady_key_once {
    use super::*;

    fn run<M: Map<u64, u64>>(steady: u64, churn: u64) {
        let _serial = serial();
        for round in 0..scaled(6) {
            let map = Arc::new(M::with_capacity(64));
            for k in 0..steady {
                map.insert(k, 0);
            }
            let stop = Arc::new(AtomicBool::new(false));
            let (m, s) = (Arc::clone(&map), Arc::clone(&stop));
            let writers = thread::spawn(move || {
                together(4, move |t| {
                    let mut rng = Rng::new((round * 4 + t) as u64 + 99);
                    let mut i = 0u64;
                    while !s.load(Ordering::Relaxed) {
                        i += 1;
                        let k = rng.below(steady);
                        m.insert(k, i);
                        let other = steady + rng.below(churn);
                        if rng.below(2) == 0 {
                            m.insert(other, i);
                        } else {
                            m.remove(&other);
                        }
                    }
                })
            });
            for walk in 0..40 {
                let mut seen: StdMap<u64, usize> = StdMap::new();
                for (k, _) in map.entries() {
                    *seen.entry(k).or_default() += 1;
                }
                for k in 0..steady {
                    assert_eq!(
                        seen.get(&k),
                        Some(&1),
                        "{} round {round} walk {walk}: key {k}",
                        M::NAME
                    );
                }
                if M::NAME == "HopscotchMap" {
                    assert!(
                        seen.values().all(|&n| n == 1),
                        "{} round {round} walk {walk}: a key twice",
                        M::NAME
                    );
                }
            }
            stop.store(true, Ordering::Relaxed);
            writers.join().expect("writers");
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>(2_000, 4_000);
    }

    #[test]
    fn hashmap_one_chain() {
        run::<HashMap<u64, u64, Constant>>(24, 24);
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>(2_000, 4_000);
    }
}

/// HopscotchMap: an insert into a home a remove is emptying keeps its entry. Keys 5 and 1029
/// share home 5 of 1024 buckets, and of every smaller table the map shrinks to; one thread inserts and removes 5, the other inserts 1029 and
/// must find it (a remove used to clear its bit with no guard after the insert set the same bit
/// for the slot it had just freed, leaving the new entry invisible).
#[test]
fn hopscotch_an_insert_into_a_home_being_emptied_keeps_its_entry() {
    let _serial = serial();
    for round in 0..scaled(40) {
        let map =
            Arc::new(HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(1024, Identity));
        let m = Arc::clone(&map);
        together(2, move |t| {
            for i in 0..20_000u64 {
                if t == 0 {
                    m.insert(5, i);
                    m.remove(&5);
                } else {
                    assert_eq!(m.insert(1029, i), None, "round {round}");
                    assert_eq!(m.get(&1029), Some(i), "round {round}: the entry was lost");
                    assert_eq!(
                        m.remove(&1029),
                        Some(i),
                        "round {round}: the entry was lost"
                    );
                }
            }
        });
        assert_eq!(map.len(), 0);
    }
}

/// HopscotchMap: lookups and removes of keys a displacement is moving. Keys 0..=32 sit each in
/// its home slot of 64 buckets; the mover inserts a key of home 0 (whose neighborhood is full,
/// so another key moves out to make room), removes it, and puts a filler in the slot the move
/// freed, so every round moves a key again. A lookup of a key that stays present must find it
/// (a move used to clear the old bit before setting the new one, with no stamp for a lookup to
/// notice), and a remove of a present key must remove it (a remove whose unlink met the entry
/// moved used to answer `None`).
#[test]
fn hopscotch_a_key_being_moved_stays_findable_and_removable() {
    let _serial = serial();
    for round in 0..scaled(20) {
        let map =
            Arc::new(HopscotchMap::<u64, u64, Identity>::with_capacity_and_hasher(64, Identity));
        for k in 0..=32 {
            map.insert(k, k);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let (m, s) = (Arc::clone(&map), Arc::clone(&stop));
        let mover = thread::spawn(move || {
            // Fillers of homes 2, 3, ...: 66, 67, ... (home k & 63).
            for filler in 66..96u64 {
                if m.capacity() != 64 {
                    break;
                }
                m.insert(64, 64);
                m.remove(&64);
                m.insert(filler, filler);
            }
            s.store(true, Ordering::Relaxed);
        });
        let (m, s) = (Arc::clone(&map), Arc::clone(&stop));
        let checker = thread::spawn(move || {
            let mut rng = Rng::new(round as u64);
            while !s.load(Ordering::Relaxed) {
                let k = 2 + rng.below(31);
                if rng.below(4) == 0 {
                    assert_eq!(
                        m.remove(&k),
                        Some(k),
                        "round {round}: remove of present key {k}"
                    );
                    m.insert(k, k);
                } else {
                    assert_eq!(m.get(&k), Some(k), "round {round}: present key {k} missed");
                }
            }
        });
        mover.join().expect("mover");
        checker.join().expect("checker");
        for k in 0..=32 {
            assert_eq!(map.get(&k), Some(k), "round {round}: key {k}");
        }
    }
}

/// Recorded concurrent histories of every writing and reading call (clear and the conditional
/// writes included) on two keys, with a thread churning other keys so the table grows and
/// shrinks under them, are linearizable per key. A compare-and-remove or a compare-and-swap
/// expects the value its thread last saw of the key.
mod histories_linearize_across_resizes {
    use super::*;

    const CALLERS: usize = 4;
    const CALLS: usize = 6;
    const KEYS: u64 = 2;
    /// The key a clear is recorded under: it belongs to every key's history.
    const ALL: u64 = u64::MAX;

    fn run<M: Map<u64, u64>>() {
        let _serial = serial();
        for round in 0..scaled(300) {
            let map = Arc::new(M::with_capacity(64));
            let clock = Arc::new(AtomicU64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let (m, s) = (Arc::clone(&map), Arc::clone(&stop));
            let churn = thread::spawn(move || {
                let mut n = 0u64;
                while !s.load(Ordering::Relaxed) {
                    for k in 100..180 {
                        m.insert(k, n);
                    }
                    for k in 100..180 {
                        m.remove(&k);
                    }
                    n += 1;
                }
            });
            let (m, c) = (Arc::clone(&map), Arc::clone(&clock));
            let logs: Vec<Vec<(u64, Event)>> = together(CALLERS, move |t| {
                let mut rng = Rng::new(round as u64 * 31 + t as u64);
                let mut last_seen = [0u64; KEYS as usize];
                (0..CALLS)
                    .map(|i| {
                        let k = rng.below(KEYS);
                        let v = (t * CALLS + i) as u64 + 1;
                        let seen = last_seen[k as usize];
                        let call = match rng.below(33) {
                            0..=3 => Call::Insert(v),
                            4..=6 => Call::InsertIfAbsent(v),
                            7..=9 => Call::GetOrInsert(v),
                            10..=12 => Call::Remove,
                            13..=16 => Call::Get,
                            17..=18 => Call::RemoveIfEven,
                            19..=20 => Call::CompareAndRemove(seen),
                            21..=22 => Call::ReplaceIfEven(v),
                            23..=25 => Call::CompareAndSwap(seen, v),
                            26..=28 => Call::ComputeAdd(100),
                            29..=31 => Call::ComputeToggle(v),
                            _ => Call::Clear,
                        };
                        let invoked = c.fetch_add(1, Ordering::SeqCst);
                        let answer = perform(&*m, k, call);
                        let answered = c.fetch_add(1, Ordering::SeqCst);
                        if let Some(a) = answer {
                            last_seen[k as usize] = a & !REFUSED;
                        }
                        (
                            if call == Call::Clear { ALL } else { k },
                            Event {
                                call,
                                answer,
                                invoked,
                                answered,
                            },
                        )
                    })
                    .collect()
            });
            stop.store(true, Ordering::Relaxed);
            churn.join().expect("churn");
            for k in 0..KEYS {
                let history: Vec<Event> = logs
                    .iter()
                    .flatten()
                    .filter(|(key, _)| *key == k || *key == ALL)
                    .map(|(_, e)| *e)
                    .collect();
                assert!(
                    linearizable(&history, None),
                    "{} round {round} key {k}: no linearization of {history:#?}",
                    M::NAME
                );
            }
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>();
    }

    #[test]
    fn hashmap_one_chain() {
        run::<HashMap<u64, u64, Constant>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>();
    }
}

/// Every value a map is given is dropped exactly once, whatever happens to it: kept, replaced,
/// removed, refused by a claim or a conditional write, made by a compute, copied by a resize,
/// cleared, or dropped with the map.
mod every_value_is_dropped_once {
    use super::*;

    fn run<M: Map<u64, Tracked>>() {
        let _serial = serial();
        for round in 0..scaled(6) {
            let counts = Counts::new();
            {
                let map = Arc::new(M::with_capacity(64));
                let (m, c) = (Arc::clone(&map), Arc::clone(&counts));
                together(8, move |t| {
                    let mut rng = Rng::new((round * 8 + t) as u64 + 7);
                    for i in 0..3_000u64 {
                        let k = rng.below(600);
                        let parity = rng.below(2);
                        match rng.below(12) {
                            0 => drop(m.insert(k, c.value(i))),
                            1 => drop(m.insert_if_absent(k, c.value(i))),
                            2 => drop(m.get_or_insert(k, c.value(i))),
                            3 => drop(m.remove(&k)),
                            4 => drop(m.force_remove(&k)),
                            5 => drop(m.get(&k)),
                            6 => drop(m.remove_if(&k, |v| v.id % 2 == parity)),
                            7 => drop(m.compare_and_remove(&k, &c.value(i % 5))),
                            8 => drop(m.replace_if(k, c.value(i), |v| v.id % 2 == parity)),
                            9 => drop(m.compare_and_swap(k, &c.value(i % 5), c.value(i))),
                            10 => drop(m.compute(k, |v| match v {
                                Some(v) if v.id % 2 == parity => None,
                                Some(v) => Some(c.value(v.id + 1)),
                                None => Some(c.value(i)),
                            })),
                            _ if t == 0 && i % 1_000 == 999 => m.clear(),
                            _ => drop(m.entries()),
                        }
                    }
                });
            }
            settle(&counts);
            assert!(counts.balanced(), "{} round {round}: {counts:?}", M::NAME);
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, Tracked, Fold>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, Tracked, Fold>>();
    }
}

/// A clear racing writers leaves a consistent map: every key a walk yields is one `get` finds,
/// and the count at rest is the walk's.
mod a_clear_racing_writers_leaves_a_consistent_map {
    use super::*;

    fn run<M: Map<u64, u64>>() {
        let _serial = serial();
        for round in 0..scaled(10) {
            let map = Arc::new(M::with_capacity(64));
            let m = Arc::clone(&map);
            together(6, move |t| {
                let mut rng = Rng::new((round * 6 + t) as u64 + 3);
                for i in 0..2_000u64 {
                    if t == 0 && i % 200 == 0 {
                        m.clear();
                    }
                    let k = rng.below(300);
                    if rng.below(3) == 0 {
                        m.remove(&k);
                    } else {
                        m.insert(k, i);
                    }
                }
            });
            let walked = map.entries();
            for (k, v) in &walked {
                assert_eq!(map.get(k), Some(*v), "{} round {round}: key {k}", M::NAME);
            }
            assert_eq!(map.len(), walked.len(), "{} round {round}: count", M::NAME);
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>();
    }
}

#[path = "concurrent/matches_std.rs"]
mod matches_std_on_any_sequence;

/// Sixteen threads each own every sixteenth key (so the keys of one hash group, or of one
/// neighborhood, belong to sixteen different threads) and run insert, replace, claim,
/// get_or_insert, remove, force_remove, the conditional writes and lookups on them, in bursts
/// that grow and then shrink the table, checking every answer against the thread's own `std`
/// model: with no other thread on its keys, every answer is determined. At the end the map holds
/// exactly the union of the models. `KOVAN_STRESS_SCALE` multiplies the calls (millions in the
/// release runs).
mod owned_keys_answer_like_a_sequential_model {
    use super::*;

    const THREADS: usize = 16;
    const KEYS_PER_THREAD: u64 = 256;

    fn run<M: Map<u64, u64>>(calls: usize) {
        let _serial = serial();
        let map = Arc::new(M::with_capacity(64));
        let m = Arc::clone(&map);
        let models: Vec<StdMap<u64, u64>> = together(THREADS, move |t| {
            let mut rng = Rng::new(t as u64 + 1_000);
            let mut model: StdMap<u64, u64> = StdMap::new();
            for i in 0..calls {
                let k = t as u64 + THREADS as u64 * rng.below(KEYS_PER_THREAD);
                let v = i as u64;
                // Bursts of 4096 calls lean to inserting, then to removing.
                let grow = (i / 4_096).is_multiple_of(2);
                let roll = rng.below(10);
                // Every sixteenth call is a conditional write, on the call number's parity.
                let parity = (i / 16) as u64 % 2;
                let (got, want) = match (grow, roll) {
                    _ if i % 16 == 5 => (
                        m.remove_if(&k, |v| v % 2 == parity),
                        match model.get(&k) {
                            Some(p) if p % 2 == parity => model.remove(&k),
                            _ => None,
                        },
                    ),
                    _ if i % 16 == 11 => (
                        lin::replaced(m.replace_if(k, v, |p| p % 2 == parity)),
                        lin::replaced(match model.get_mut(&k) {
                            None => Err(None),
                            Some(p) if *p % 2 == parity => Ok(std::mem::replace(p, v)),
                            Some(p) => Err(Some(*p)),
                        }),
                    ),
                    _ if i % 16 == 13 => {
                        let next = |seen: Option<&u64>| match seen {
                            Some(p) if p % 2 == parity => None,
                            Some(p) => Some(p + 1),
                            None if grow => Some(v),
                            None => None,
                        };
                        let want = next(model.get(&k));
                        match want {
                            Some(n) => model.insert(k, n),
                            None => model.remove(&k),
                        };
                        (m.compute(k, next), want)
                    }
                    (true, 0..=3) | (false, 0) => (m.insert(k, v), model.insert(k, v)),
                    (true, 4..=5) | (false, 1) => {
                        let want = model.get(&k).copied();
                        model.entry(k).or_insert(v);
                        (m.insert_if_absent(k, v), want)
                    }
                    (true, 6) | (false, 2) => (
                        Some(m.get_or_insert(k, v)),
                        Some(*model.entry(k).or_insert(v)),
                    ),
                    (true, 7) | (false, 3..=5) => (m.remove(&k), model.remove(&k)),
                    (false, 6) => (m.force_remove(&k), model.remove(&k)),
                    (_, 8) => (
                        m.contains_key(&k).then_some(0),
                        model.contains_key(&k).then_some(0),
                    ),
                    _ => (m.get(&k), model.get(&k).copied()),
                };
                assert_eq!(
                    got,
                    want,
                    "{} thread {t} call {i} key {k} (roll {roll})",
                    M::NAME
                );
            }
            model
        });
        let mut want: Vec<(u64, u64)> = models.into_iter().flatten().collect();
        want.sort_unstable();
        let mut got = map.entries();
        got.sort_unstable();
        assert_eq!(got.len(), want.len(), "{}: entries", M::NAME);
        assert_eq!(got, want, "{}: contents", M::NAME);
        assert_eq!(map.len(), want.len(), "{}: count", M::NAME);
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>(scaled(20_000));
    }

    #[test]
    fn hashmap_grouped() {
        run::<HashMap<u64, u64, Grouped>>(scaled(20_000));
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>(scaled(20_000));
    }

    #[test]
    fn hopscotch_clustered() {
        run::<HopscotchMap<u64, u64, Clustered>>(scaled(20_000));
    }
}
