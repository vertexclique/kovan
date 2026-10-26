//! Both maps' public operations from a thread-local destructor that runs
//! after the thread's exit (see kovan's `tests/support/thread_exit.rs`),
//! while another thread keeps replacing and removing the same keys: no
//! operation reads a freed value, nothing panics, and every value is freed in
//! the end.

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan::flush;
use kovan_map::{HashMap, HopscotchMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use thread_exit::{
    Val, after_exit, alive, freed_down_to, held_across_retire, live, lock, take_dead_reads,
};

const KEYS: u64 = 16;

macro_rules! map_after_the_exit {
    ($operations:ident, $dropped:ident, $held:ident, $map:ty) => {
        /// Every operation after the exit while another thread replaces and
        /// removes the same keys: no clone of a freed value, every value
        /// handed out alive, every value freed in the end.
        #[test]
        #[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
        fn $operations() {
            let _l = lock();
            let base = live();
            take_dead_reads();
            let map: Arc<$map> = Arc::new(<$map>::new());
            for k in 0..KEYS {
                map.insert(k, Val::new());
            }
            let stop = Arc::new(AtomicBool::new(false));
            let churner = {
                let (map, stop) = (Arc::clone(&map), Arc::clone(&stop));
                thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        for k in 0..KEYS {
                            map.insert(k, Val::new());
                            if k % 3 == 0 {
                                map.remove(&k);
                            }
                        }
                        flush();
                    }
                })
            };
            let m = Arc::clone(&map);
            let checks = after_exit(move || {
                let mut ok = Vec::new();
                for round in 0..40 {
                    for k in 0..KEYS {
                        if let Some(v) = m.get(&k) {
                            ok.push(alive(v.id));
                        }
                        let _ = m.contains_key(&k);
                        if let Some(old) = m.insert(k, Val::new()) {
                            ok.push(alive(old.id));
                        }
                        if let Some(existing) = m.insert_if_absent(k, Val::new()) {
                            ok.push(alive(existing.id));
                        }
                        let v = m.get_or_insert(k, Val::new());
                        ok.push(alive(v.id));
                        if k % 2 == 0
                            && let Some(v) = m.remove(&k)
                        {
                            ok.push(alive(v.id));
                        }
                        if k % 5 == 0
                            && let Some(v) = m.force_remove(&k)
                        {
                            ok.push(alive(v.id));
                        }
                    }
                    for (_, v) in m.iter() {
                        ok.push(alive(v.id));
                    }
                    for v in m.values() {
                        ok.push(alive(v.id));
                    }
                    let _ = m.keys().count();
                    let _ = m.len();
                    if round % 10 == 0 {
                        m.clear();
                    }
                    m.extend((0..4).map(|k| (k, Val::new())));
                }
                flush();
                ok
            });
            stop.store(true, Ordering::Relaxed);
            churner.join().unwrap();
            assert_eq!(
                take_dead_reads(),
                0,
                "an operation after the thread's exit cloned a freed value"
            );
            assert!(
                checks.iter().all(|&ok| ok),
                "an operation after the thread's exit handed out a dropped value"
            );
            drop(map);
            assert!(
                freed_down_to(base),
                "values outlived the test: {}",
                live() - base
            );
        }

        /// `get`, `get_or_insert` of a present key and an iteration after
        /// the exit, each stopped inside its clone of the stored value while
        /// this thread replaces that value and flushes: the value stays alive.
        #[test]
        #[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
        fn $held() {
            let _l = lock();
            let base = live();
            let map: Arc<$map> = Arc::new(<$map>::new());
            let reads: [fn(&$map); 3] = [
                |m| drop(m.get(&0)),
                |m| drop(m.get_or_insert(0, Val::new())),
                |m| m.values().for_each(drop),
            ];
            for (n, read) in reads.into_iter().enumerate() {
                let stored = Val::new();
                let id = stored.id;
                map.insert(0, stored);
                let m = Arc::clone(&map);
                let held =
                    held_across_retire(id, move || read(&m), || drop(map.insert(0, Val::new())));
                assert!(
                    held,
                    "read {n} after the thread's exit cloned a value freed under its guard"
                );
            }
            drop(map);
            assert!(
                freed_down_to(base),
                "values outlived the test: {}",
                live() - base
            );
        }

        /// A map dropped after the exit frees its values.
        #[test]
        #[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
        fn $dropped() {
            let _l = lock();
            let base = live();
            let map: $map = <$map>::new();
            for k in 0..KEYS {
                map.insert(k, Val::new());
            }
            after_exit(move || {
                map.insert(0, Val::new());
                map.remove(&1);
                drop(map);
            });
            assert!(
                freed_down_to(base),
                "values outlived the test: {}",
                live() - base
            );
        }
    };
}

map_after_the_exit!(
    hashmap_operations_after_the_exit,
    a_hashmap_dropped_after_the_exit_is_freed,
    a_hashmap_read_after_the_exit_holds_its_value,
    HashMap<u64, Val>
);
map_after_the_exit!(
    hopscotch_operations_after_the_exit,
    a_hopscotch_map_dropped_after_the_exit_is_freed,
    a_hopscotch_map_read_after_the_exit_holds_its_value,
    HopscotchMap<u64, Val>
);
