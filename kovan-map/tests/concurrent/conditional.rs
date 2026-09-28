//! The conditional writes under concurrency, on both maps, with the table growing and shrinking
//! under them: exact winners (every compare-and-swap that answered `Ok` moved its key by one,
//! every race of conditional removes or replaces of one value has one winner), exact counters
//! built on `compute`, and (on `HopscotchMap`) conditional writes of keys a displacement is
//! moving. Histories, drop counts and the owned-keys model of the other modules cover the
//! conditional writes too.

use super::*;

/// The keys the churn thread inserts and removes, far from every test's own.
const CHURN: core::ops::Range<u64> = 1 << 32..(1 << 32) + 80;

/// A thread that inserts and removes the `CHURN` keys until `stop`, so the table grows past
/// three quarters and shrinks under a quarter while the test's writes run.
fn churn<M: Map<u64, u64>>(map: &Arc<M>, stop: &Arc<AtomicBool>) -> thread::JoinHandle<()> {
    let (m, s) = (Arc::clone(map), Arc::clone(stop));
    thread::spawn(move || {
        let mut n = 0u64;
        while !s.load(Ordering::Relaxed) {
            for k in CHURN {
                m.insert(k, n);
            }
            for k in CHURN {
                m.remove(&k);
            }
            n += 1;
        }
    })
}

/// Threads increment shared counters with compare-and-swap, each retrying with the value its
/// refused swap answered: every swap that answered `Ok` moved its key by one, so each counter
/// ends at the number of such swaps, and a refused swap answered a value other than the one it
/// expected.
mod compare_and_swap_counts_every_winner {
    use super::*;

    fn run<M: Map<u64, u64>>(calls: u64) {
        let _serial = serial();
        const THREADS: usize = 8;
        const KEYS: u64 = 16;
        for round in 0..scaled(4) {
            let map = Arc::new(M::with_capacity(64));
            for k in 0..KEYS {
                map.insert(k, 0);
            }
            let stop = Arc::new(AtomicBool::new(false));
            let churner = churn(&map, &stop);
            let m = Arc::clone(&map);
            let wins: Vec<Vec<u64>> = together(THREADS, move |t| {
                let mut rng = Rng::new((round * THREADS + t) as u64 + 11);
                let mut wins = vec![0u64; KEYS as usize];
                let mut seen = vec![0u64; KEYS as usize];
                for _ in 0..calls {
                    let k = rng.below(KEYS) as usize;
                    let expected = seen[k];
                    match m.compare_and_swap(k as u64, &expected, expected + 1) {
                        Ok(old) => {
                            assert_eq!(old, expected, "{} round {round} key {k}", M::NAME);
                            wins[k] += 1;
                            seen[k] = expected + 1;
                        }
                        Err(Some(current)) => {
                            assert_ne!(current, expected, "{} round {round} key {k}", M::NAME);
                            seen[k] = current;
                        }
                        Err(None) => panic!("{} round {round}: key {k} vanished", M::NAME),
                    }
                }
                wins
            });
            stop.store(true, Ordering::Relaxed);
            churner.join().expect("churn");
            for k in 0..KEYS as usize {
                let total: u64 = wins.iter().map(|w| w[k]).sum();
                assert_eq!(
                    map.get(&(k as u64)),
                    Some(total),
                    "{} round {round} key {k}",
                    M::NAME
                );
            }
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>(4_000);
    }

    #[test]
    fn hashmap_one_chain() {
        run::<HashMap<u64, u64, Constant>>(400);
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>(4_000);
    }

    #[test]
    fn hopscotch_clustered() {
        run::<HopscotchMap<u64, u64, Clustered>>(4_000);
    }
}

/// Counters built on `compute` (absent: insert 1; present: add 1) end at the number of computes
/// of each key, whatever raced them: every compute read the value the previous one wrote.
mod compute_counters_are_exact {
    use super::*;

    fn run<M: Map<u64, u64>>(calls: u64) {
        let _serial = serial();
        const THREADS: usize = 8;
        const KEYS: u64 = 16;
        for round in 0..scaled(4) {
            let map = Arc::new(M::with_capacity(64));
            let stop = Arc::new(AtomicBool::new(false));
            let churner = churn(&map, &stop);
            let m = Arc::clone(&map);
            let counts: Vec<Vec<u64>> = together(THREADS, move |t| {
                let mut rng = Rng::new((round * THREADS + t) as u64 + 23);
                let mut counts = vec![0u64; KEYS as usize];
                for _ in 0..calls {
                    let k = rng.below(KEYS);
                    let after = m.compute(k, |v| Some(v.map_or(1, |v| v + 1)));
                    assert!(after.is_some_and(|v| v >= 1), "{} round {round}", M::NAME);
                    counts[k as usize] += 1;
                }
                counts
            });
            stop.store(true, Ordering::Relaxed);
            churner.join().expect("churn");
            for k in 0..KEYS as usize {
                let total: u64 = counts.iter().map(|c| c[k]).sum();
                let want = (total > 0).then_some(total);
                assert_eq!(
                    map.get(&(k as u64)),
                    want,
                    "{} round {round} key {k}",
                    M::NAME
                );
            }
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>(4_000);
    }

    #[test]
    fn hashmap_one_chain() {
        run::<HashMap<u64, u64, Constant>>(400);
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>(4_000);
    }

    #[test]
    fn hopscotch_clustered() {
        run::<HopscotchMap<u64, u64, Clustered>>(4_000);
    }
}

/// Every thread races for every key, three times: a compare-and-remove of the key's value 0 (one
/// thread removes it), then, the key back at 1, a replace of 1 by the thread's own value (one
/// thread replaces it, and every other answers that thread's value), then a remove_if of the
/// thread values (one thread removes the winner's value).
mod conditional_races_have_one_winner {
    use super::*;

    /// The one thread whose answer for key `k` shows it won; the test fails unless exactly one
    /// did.
    fn the_winner<T>(answers: &[Vec<T>], k: usize, won: impl Fn(&T) -> bool, what: &str) -> usize {
        let winners: Vec<usize> = (0..answers.len())
            .filter(|&t| won(&answers[t][k]))
            .collect();
        assert_eq!(winners.len(), 1, "{what} key {k}: winners {winners:?}");
        winners[0]
    }

    fn run<M: Map<u64, u64>>() {
        let _serial = serial();
        const THREADS: usize = 8;
        const KEYS: u64 = 256;
        for round in 0..scaled(10) {
            let what = format!("{} round {round}", M::NAME);
            let map = Arc::new(M::with_capacity(64));
            for k in 0..KEYS {
                map.insert(k, 0);
            }
            let stop = Arc::new(AtomicBool::new(false));
            let churner = churn(&map, &stop);

            let m = Arc::clone(&map);
            let removed: Vec<Vec<Option<u64>>> = together(THREADS, move |_| {
                (0..KEYS).map(|k| m.compare_and_remove(&k, &0)).collect()
            });
            for key in 0..KEYS {
                let k = key as usize;
                let winner = the_winner(&removed, k, Option::is_some, &what);
                assert_eq!(removed[winner][k], Some(0), "{what} key {k}");
                assert_eq!(map.get(&key), None, "{what} key {k}");
                map.insert(key, 1);
            }

            let m = Arc::clone(&map);
            let replaced: Vec<Vec<Result<u64, Option<u64>>>> = together(THREADS, move |t| {
                let own = 100 + t as u64;
                (0..KEYS)
                    .map(|k| m.replace_if(k, own, |v| *v == 1))
                    .collect()
            });
            let mut winner_of = vec![0u64; KEYS as usize];
            for key in 0..KEYS {
                let k = key as usize;
                let winner = the_winner(&replaced, k, Result::is_ok, &what);
                let own = 100 + winner as u64;
                assert_eq!(replaced[winner][k], Ok(1), "{what} key {k}");
                for (t, answer) in replaced.iter().enumerate() {
                    if t != winner {
                        assert_eq!(answer[k], Err(Some(own)), "{what} key {k} thread {t}");
                    }
                }
                assert_eq!(map.get(&key), Some(own), "{what} key {k}");
                winner_of[k] = own;
            }

            let m = Arc::clone(&map);
            let taken: Vec<Vec<Option<u64>>> = together(THREADS, move |_| {
                (0..KEYS).map(|k| m.remove_if(&k, |v| *v >= 100)).collect()
            });
            stop.store(true, Ordering::Relaxed);
            churner.join().expect("churn");
            for key in 0..KEYS {
                let k = key as usize;
                let winner = the_winner(&taken, k, Option::is_some, &what);
                assert_eq!(taken[winner][k], Some(winner_of[k]), "{what} key {k}");
                assert!(!map.contains_key(&key), "{what} key {k}");
            }
        }
    }

    #[test]
    fn hashmap() {
        run::<HashMap<u64, u64, Fold>>();
    }

    #[test]
    fn hashmap_grouped() {
        run::<HashMap<u64, u64, Grouped>>();
    }

    #[test]
    fn hopscotch() {
        run::<HopscotchMap<u64, u64, Fold>>();
    }

    #[test]
    fn hopscotch_clustered() {
        run::<HopscotchMap<u64, u64, Clustered>>();
    }
}

/// HopscotchMap: conditional writes of keys a displacement is moving find them. Keys 0..=32 sit
/// each in its home slot of 64 buckets; the mover inserts a key of home 0 (moving another key out
/// of home 0's full neighborhood), removes it, and puts a filler in the slot the move freed. Every
/// conditional write of a present key must see it: a compare-and-swap of its own value swaps, a
/// replace_if and a compute keep it, and a remove_if removes it (the checker puts it back).
#[test]
fn hopscotch_conditional_writes_find_a_key_being_moved() {
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
            let mut rng = Rng::new(round as u64 + 5);
            while !s.load(Ordering::Relaxed) {
                let k = 2 + rng.below(31);
                match rng.below(4) {
                    0 => assert_eq!(
                        m.compare_and_swap(k, &k, k),
                        Ok(k),
                        "round {round}: key {k}"
                    ),
                    1 => assert_eq!(
                        m.replace_if(k, k, |v| *v == k),
                        Ok(k),
                        "round {round}: key {k}"
                    ),
                    2 => assert_eq!(
                        m.compute(k, |v| v.copied()),
                        Some(k),
                        "round {round}: key {k}"
                    ),
                    _ => {
                        assert_eq!(
                            m.remove_if(&k, |v| *v == k),
                            Some(k),
                            "round {round}: key {k}"
                        );
                        assert_eq!(
                            m.compute(k, |v| {
                                assert_eq!(v, None, "round {round}: key {k} came back");
                                Some(k)
                            }),
                            Some(k)
                        );
                    }
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
