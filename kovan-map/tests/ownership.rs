//! Every key and value that enters either map leaves it exactly once:
//! dropped once, never leaked, never dropped twice, whichever way it went
//! (replaced, removed, refused by a conditional write, handed back, dropped
//! by an insert that found its key present, cleared, cloned, drained by an
//! owning iterator, whole or in part, or still inside when the map drops).
//! Keys and values are counted by id outside their memory (see kovan's
//! `tests/support/thread_exit.rs`), and every read is checked against that
//! count: no read of a dropped value.

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_map::{HashMap, HopscotchMap};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::thread;
use thread_exit::{Val, balanced, live, lock, take_dead_reads, take_double_drops};

/// A key or a value: compared and hashed by `n`, and tracked by its `Val`.
#[derive(Clone)]
struct T {
    n: u64,
    _tracked: Val,
}

impl PartialEq for T {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n
    }
}

impl Eq for T {}

impl Hash for T {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.n.hash(state);
    }
}

impl Borrow<u64> for T {
    fn borrow(&self) -> &u64 {
        &self.n
    }
}

fn t(n: u64) -> T {
    T {
        n,
        _tracked: Val::new(),
    }
}

fn n(v: Option<T>) -> Option<u64> {
    v.map(|v| v.n)
}

fn start() -> usize {
    take_double_drops();
    take_dead_reads();
    live()
}

fn end(base: usize, what: &str) {
    balanced(base, what);
    assert_eq!(take_dead_reads(), 0, "{what}: a dropped value was read");
}

macro_rules! ownership_tests {
    ($module:ident, $map:ident) => {
        mod $module {
            use super::*;

            fn filled(keys: u64) -> $map<T, T> {
                // Small, so the inserts resize it.
                let map = $map::with_capacity(4);
                for k in 0..keys {
                    assert!(map.insert(t(k), t(k)).is_none());
                }
                map
            }

            #[test]
            fn every_operation_drops_each_key_and_value_once() {
                let _l = lock();
                let base = start();
                let map = filled(64);
                for k in 0..64 {
                    assert_eq!(n(map.insert(t(k), t(k + 100))), Some(k));
                }
                assert_eq!(n(map.insert_if_absent(t(1), t(999))), Some(101));
                assert_eq!(n(map.insert_if_absent(t(200), t(200))), None);
                assert_eq!(map.get_or_insert(t(2), t(999)).n, 102);
                assert_eq!(map.get_or_insert(t(201), t(201)).n, 201);
                assert_eq!(n(map.get(&3)), Some(103));
                assert!(map.contains_key(&3));
                assert_eq!(n(map.remove(&4)), Some(104));
                assert_eq!(n(map.remove(&4)), None);
                assert_eq!(n(map.force_remove(&5)), Some(105));
                assert_eq!(n(map.force_remove(&5)), None);
                assert_eq!(n(map.remove_if(&6, |_| false)), None);
                assert_eq!(n(map.remove_if(&6, |v| v.n == 106)), Some(106));
                assert_eq!(n(map.compare_and_remove(&7, &t(0))), None);
                assert_eq!(n(map.compare_and_remove(&7, &t(107))), Some(107));
                assert!(
                    matches!(map.replace_if(t(8), t(999), |_| false), Err(Some(v)) if v.n == 108)
                );
                assert!(matches!(map.replace_if(t(8), t(308), |_| true), Ok(v) if v.n == 108));
                assert!(matches!(
                    map.replace_if(t(300), t(300), |_| true),
                    Err(None)
                ));
                assert!(
                    matches!(map.compare_and_swap(t(9), &t(0), t(999)), Err(Some(v)) if v.n == 109)
                );
                assert!(
                    matches!(map.compare_and_swap(t(9), &t(109), t(309)), Ok(v) if v.n == 109)
                );
                assert!(matches!(
                    map.compare_and_swap(t(301), &t(0), t(301)),
                    Err(None)
                ));
                assert_eq!(n(map.compute(t(10), |v| v.map(|v| t(v.n + 200)))), Some(310));
                assert_eq!(n(map.compute(t(11), |_| None)), None);
                assert_eq!(n(map.get(&11)), None);
                assert_eq!(n(map.compute(t(302), |_| None)), None);
                assert_eq!(n(map.compute(t(303), |_| Some(t(303)))), Some(303));
                map.extend((400..410).map(|k| (t(k), t(k))));
                assert_eq!(map.iter().count(), map.len());
                assert_eq!(map.keys().count(), map.len());
                assert_eq!(map.values().count(), map.len());
                let copy = map.clone();
                assert_eq!(copy.len(), map.len());
                drop(copy);
                let collected: $map<T, T> = (500..510).map(|k| (t(k), t(k))).collect();
                assert_eq!(collected.len(), 10);
                drop(collected);
                map.clear();
                assert!(map.is_empty());
                for k in 0..32 {
                    map.insert(t(k), t(k));
                }
                drop(map);
                end(base, stringify!($map));
            }

            #[test]
            fn an_owning_iterator_drops_each_entry_once() {
                let _l = lock();
                let base = start();
                let map = filled(256);
                for k in (0..256).step_by(3) {
                    map.remove(&k);
                }
                let drained: Vec<_> = map.into_iter().collect();
                assert_eq!(drained.len(), 256 - 86);
                drop(drained);
                end(base, "a drained owning iterator");
            }

            #[test]
            fn an_owning_iterator_dropped_part_way_drops_the_rest_once() {
                let _l = lock();
                let base = start();
                let map = filled(256);
                for k in (0..256).step_by(5) {
                    map.remove(&k);
                }
                let mut entries = map.into_iter();
                let taken: Vec<_> = entries.by_ref().take(17).collect();
                assert_eq!(taken.len(), 17);
                drop(entries);
                drop(taken);
                end(base, "an owning iterator dropped part way");
            }

            /// Many threads racing every write on a few keys of a map that
            /// starts small (so it resizes under them), then the map drained
            /// part way and dropped: the same balance.
            #[test]
            #[cfg_attr(miri, ignore)] // threads
            fn racing_writes_drop_each_key_and_value_once() {
                let _l = lock();
                let base = start();
                let map = Arc::new($map::<T, T>::with_capacity(4));
                let handles: Vec<_> = (0..8u64)
                    .map(|th| {
                        let map = Arc::clone(&map);
                        thread::spawn(move || {
                            for i in 0..3_000u64 {
                                let k = (i * 7 + th) % 96;
                                match (i + th) % 12 {
                                    0 => drop(map.insert(t(k), t(i))),
                                    1 => drop(map.remove(&k)),
                                    2 => drop(map.insert_if_absent(t(k), t(i))),
                                    3 => drop(map.get_or_insert(t(k), t(i))),
                                    4 => drop(map.compute(t(k), |v| v.map(|v| t(v.n + 1)))),
                                    5 => drop(map.replace_if(t(k), t(i), |v| v.n % 2 == 0)),
                                    6 => {
                                        if let Some(seen) = map.get(&k) {
                                            drop(map.compare_and_swap(t(k), &seen, t(i)));
                                        }
                                    }
                                    7 => drop(map.remove_if(&k, |v| v.n % 3 == 0)),
                                    8 => drop(map.force_remove(&k)),
                                    9 => map.iter().take(8).for_each(drop),
                                    10 if i % 500 == 0 => map.clear(),
                                    _ => drop(map.get(&k)),
                                }
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
                let map = Arc::into_inner(map).expect("a thread still holds the map");
                let mut entries = map.into_iter();
                entries.by_ref().take(5).for_each(drop);
                drop(entries);
                end(base, "racing writes");
            }
        }
    };
}

ownership_tests!(hashmap, HashMap);
ownership_tests!(hopscotch, HopscotchMap);
