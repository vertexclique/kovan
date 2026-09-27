//! Shuttle-searched interleavings of every pair of operations on both maps, with a grow or a
//! shrink (or a clear, or a displacement) in flight among them: the threads record every call
//! with its answer on one clock, and each key's history must linearize (see `support/lin.rs`);
//! a walk must yield every key present for its whole length once. Under the `shuttle` feature
//! every atomic both maps use (kovan's pointer words, the latches, counts and control words) is a
//! scheduling point.
//!
//! Each scenario runs under random scheduling and under PCT (depth 3). `KOVAN_SHUTTLE_ITERS`
//! sets the schedules per scenario and strategy (default 300; the long runs recorded for a
//! release use far more). A clean run means the sampled schedules held; a red run is a finding,
//! replayable from the schedule shuttle prints.

#![cfg(feature = "shuttle")]

#[path = "support/lin.rs"]
mod lin;
#[path = "support/maps.rs"]
mod maps;

use lin::{Call, Event, linearizable};
use maps::{Clustered, Constant, Grouped, Identity, Map};
use shuttle::sync::Mutex;
use shuttle::sync::atomic::{AtomicU64, Ordering};
use shuttle::thread;
use std::sync::Arc;

type Fold = foldhash::fast::FixedState;

fn iterations() -> usize {
    std::env::var("KOVAN_SHUTTLE_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
}

/// The key a clear is recorded under: it belongs to every key's history.
const ALL: u64 = u64::MAX;

/// The calls of one execution on one clock.
#[derive(Default)]
struct Record {
    clock: AtomicU64,
    log: Mutex<Vec<(u64, Event)>>,
}

impl Record {
    fn call<M: Map<u64, u64>>(&self, m: &M, k: u64, call: Call) -> Option<u64> {
        let invoked = self.clock.fetch_add(1, Ordering::SeqCst);
        let answer = match call {
            Call::Insert(v) => m.insert(k, v),
            Call::InsertIfAbsent(v) => m.insert_if_absent(k, v),
            Call::GetOrInsert(v) => Some(m.get_or_insert(k, v)),
            Call::Remove => m.remove(&k),
            Call::Get => m.get(&k),
            Call::Clear => {
                m.clear();
                None
            }
        };
        let answered = self.clock.fetch_add(1, Ordering::SeqCst);
        let key = if call == Call::Clear { ALL } else { k };
        self.log.lock().expect("log").push((
            key,
            Event {
                call,
                answer,
                invoked,
                answered,
            },
        ));
        answer
    }

    /// Every key's history linearizes from its value before the threads started.
    fn check(&self, initial: &[(u64, Option<u64>)], name: &str) {
        let log = self.log.lock().expect("log");
        for &(k, start) in initial {
            let history: Vec<Event> = log
                .iter()
                .filter(|(key, _)| *key == k || *key == ALL)
                .map(|(_, e)| *e)
                .collect();
            assert!(
                linearizable(&history, start),
                "{name} key {k}: no linearization of {history:#?}"
            );
        }
    }
}

/// Run `bodies` on threads of their own and wait for them.
fn run_all(bodies: Vec<Box<dyn FnOnce() + Send>>) {
    let handles: Vec<_> = bodies.into_iter().map(thread::spawn).collect();
    for h in handles {
        h.join().expect("a thread of the scenario panicked");
    }
}

/// Fill `m` with `n` keys from `from` (values 0).
fn fill<M: Map<u64, u64>>(m: &M, from: u64, n: u64) {
    for k in from..from + n {
        m.insert(k, 0);
    }
}

/// Two claims and a get_or_insert of one key, a replace and a remove of another, and a thread
/// whose two inserts push the table over three quarters (a grow in flight among the claims).
fn claims_race_a_grow<M: Map<u64, u64>>() {
    let m = Arc::new(M::with_capacity(64));
    fill(&*m, 1_000, 46);
    m.insert(2, 20);
    let rec = Arc::new(Record::default());
    let (m1, r1) = (Arc::clone(&m), Arc::clone(&rec));
    let (m2, r2) = (Arc::clone(&m), Arc::clone(&rec));
    let m3 = Arc::clone(&m);
    run_all(vec![
        Box::new(move || {
            r1.call(&*m1, 1, Call::InsertIfAbsent(1));
            r1.call(&*m1, 2, Call::Insert(21));
        }),
        Box::new(move || {
            r2.call(&*m2, 1, Call::GetOrInsert(2));
            r2.call(&*m2, 2, Call::Remove);
        }),
        Box::new(move || fill(&*m3, 2_000, 3)),
    ]);
    rec.check(&[(1, None), (2, Some(20))], M::NAME);
    assert_eq!(
        m.len(),
        46 + 3 + 1 + usize::from(m.contains_key(&2)),
        "{}: count",
        M::NAME
    );
}

/// Replaces, a remove and lookups of one key against a shrink: the table falls under a quarter
/// while they run.
fn writes_race_a_shrink<M: Map<u64, u64>>() {
    let m = Arc::new(M::with_capacity(64));
    // 49 entries grow the table to 128 buckets; 32 remain, one removal short of a shrink.
    fill(&*m, 1_000, 49);
    for k in 1_000..1_018 {
        m.remove(&k);
    }
    m.insert(5, 5);
    let rec = Arc::new(Record::default());
    let (m1, r1) = (Arc::clone(&m), Arc::clone(&rec));
    let (m2, r2) = (Arc::clone(&m), Arc::clone(&rec));
    let m3 = Arc::clone(&m);
    run_all(vec![
        Box::new(move || {
            r1.call(&*m1, 1, Call::Insert(1));
            r1.call(&*m1, 1, Call::InsertIfAbsent(2));
        }),
        Box::new(move || {
            r2.call(&*m2, 1, Call::Insert(3));
            // force_remove repeats removals until one answers None, so it is no single call of
            // a key's history: it gets a key of its own.
            assert_eq!(
                m2.force_remove(&5),
                Some(5),
                "force_remove of a present key"
            );
            assert!(
                !m2.contains_key(&5),
                "a force-removed key found by its remover"
            );
            r2.call(&*m2, 1, Call::Remove);
            r2.call(&*m2, 1, Call::Get);
        }),
        Box::new(move || {
            m3.remove(&1_020);
            m3.remove(&1_021);
        }),
    ]);
    rec.check(&[(1, None)], M::NAME);
}

/// Adjacent removes (keys 3, 67 and 131 share a bucket, a chain in one map and a neighborhood in
/// the other) and lookups behind them.
fn adjacent_removes<M: Map<u64, u64>>() {
    let m = Arc::new(M::with_capacity(64));
    for k in [3, 67, 131] {
        m.insert(k, k);
    }
    let rec = Arc::new(Record::default());
    let (m1, r1) = (Arc::clone(&m), Arc::clone(&rec));
    let (m2, r2) = (Arc::clone(&m), Arc::clone(&rec));
    let (m3, r3) = (Arc::clone(&m), Arc::clone(&rec));
    run_all(vec![
        Box::new(move || {
            r1.call(&*m1, 3, Call::Remove);
        }),
        Box::new(move || {
            r2.call(&*m2, 67, Call::Remove);
            assert!(!m2.contains_key(&67), "a removed key found by its remover");
        }),
        Box::new(move || {
            r3.call(&*m3, 131, Call::Get);
            r3.call(&*m3, 67, Call::Get);
        }),
    ]);
    rec.check(&[(3, Some(3)), (67, Some(67)), (131, Some(131))], M::NAME);
    assert_eq!(m.len(), 1, "{}: count", M::NAME);
}

/// A walk racing a replace of a steady key, removes and inserts of others, and a grow: every
/// steady key once.
fn walk_races_writes_and_a_grow<M: Map<u64, u64>>() {
    let m = Arc::new(M::with_capacity(64));
    for k in 1..=4 {
        m.insert(k, k);
    }
    fill(&*m, 1_000, 42);
    let (m1, m2, m3) = (Arc::clone(&m), Arc::clone(&m), Arc::clone(&m));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s1 = Arc::clone(&seen);
    run_all(vec![
        Box::new(move || {
            let keys: Vec<u64> = m1.entries().into_iter().map(|(k, _)| k).collect();
            *s1.lock().expect("seen") = keys;
        }),
        Box::new(move || {
            m2.insert(2, 9);
            fill(&*m2, 3_000, 3);
        }),
        Box::new(move || {
            m3.remove(&1_000);
            m3.insert(5, 5);
        }),
    ]);
    let seen = seen.lock().expect("seen");
    for k in 1..=4 {
        assert_eq!(
            seen.iter().filter(|&&s| s == k).count(),
            1,
            "{}: steady key {k}",
            M::NAME
        );
    }
}

/// A clear racing writes and reads of two keys.
fn clear_races_writes<M: Map<u64, u64>>() {
    let m = Arc::new(M::with_capacity(64));
    m.insert(2, 2);
    let rec = Arc::new(Record::default());
    let (m1, r1) = (Arc::clone(&m), Arc::clone(&rec));
    let (m2, r2) = (Arc::clone(&m), Arc::clone(&rec));
    let (m3, r3) = (Arc::clone(&m), Arc::clone(&rec));
    run_all(vec![
        Box::new(move || {
            r1.call(&*m1, 0, Call::Clear);
        }),
        Box::new(move || {
            r2.call(&*m2, 1, Call::Insert(1));
            r2.call(&*m2, 1, Call::Get);
        }),
        Box::new(move || {
            r3.call(&*m3, 2, Call::InsertIfAbsent(3));
            r3.call(&*m3, 2, Call::Remove);
        }),
    ]);
    rec.check(&[(1, None), (2, Some(2))], M::NAME);
    assert_eq!(m.len(), m.entries().len(), "{}: count", M::NAME);
}

/// Keys 0..=32 each in its home bucket of 64 (identity hash): an insert of key 64 (home 0, its
/// neighborhood full in the hopscotch map) moves another key to make room, while other threads
/// read, remove and claim the keys it moves.
fn writes_race_a_move<M: Map<u64, u64>>() {
    let m = Arc::new(M::with_capacity(64));
    for k in 0..=32 {
        m.insert(k, k);
    }
    let rec = Arc::new(Record::default());
    let (m1, r1) = (Arc::clone(&m), Arc::clone(&rec));
    let (m2, r2) = (Arc::clone(&m), Arc::clone(&rec));
    let (m3, r3) = (Arc::clone(&m), Arc::clone(&rec));
    run_all(vec![
        Box::new(move || {
            r1.call(&*m1, 64, Call::Insert(64));
            r1.call(&*m1, 64, Call::Remove);
        }),
        Box::new(move || {
            r2.call(&*m2, 2, Call::Get);
            r2.call(&*m2, 3, Call::Remove);
        }),
        Box::new(move || {
            r3.call(&*m3, 2, Call::InsertIfAbsent(7));
            r3.call(&*m3, 3, Call::GetOrInsert(8));
        }),
    ]);
    rec.check(&[(2, Some(2)), (3, Some(3)), (64, None)], M::NAME);
}

/// A scenario on one map type, under both strategies.
fn search(f: fn()) {
    let n = iterations();
    shuttle::check_random(f, n);
    shuttle::check_pct(f, n, 3);
}

macro_rules! scenario {
    ($name:ident, $body:ident: $($test:ident => $map:ty),+ $(,)?) => {
        mod $name {
            use super::*;
            $(
                #[test]
                fn $test() {
                    search($body::<$map>);
                }
            )+
        }
    };
}

scenario!(claims_race_a_grow, claims_race_a_grow:
    hashmap => kovan_map::HashMap<u64, u64, Fold>,
    hashmap_grouped => kovan_map::HashMap<u64, u64, Grouped>,
    hopscotch => kovan_map::HopscotchMap<u64, u64, Fold>,
    hopscotch_clustered => kovan_map::HopscotchMap<u64, u64, Clustered>,
);
scenario!(writes_race_a_shrink, writes_race_a_shrink:
    hashmap => kovan_map::HashMap<u64, u64, Fold>,
    hopscotch => kovan_map::HopscotchMap<u64, u64, Fold>,
);
scenario!(adjacent_removes, adjacent_removes:
    hashmap_one_chain => kovan_map::HashMap<u64, u64, Constant>,
    hopscotch_one_neighborhood => kovan_map::HopscotchMap<u64, u64, Identity>,
);
scenario!(walk_races_writes_and_a_grow, walk_races_writes_and_a_grow:
    hashmap => kovan_map::HashMap<u64, u64, Fold>,
    hashmap_one_chain => kovan_map::HashMap<u64, u64, Constant>,
    hopscotch => kovan_map::HopscotchMap<u64, u64, Fold>,
);
scenario!(clear_races_writes, clear_races_writes:
    hashmap => kovan_map::HashMap<u64, u64, Fold>,
    hopscotch => kovan_map::HopscotchMap<u64, u64, Fold>,
);
scenario!(writes_race_a_move, writes_race_a_move:
    hashmap => kovan_map::HashMap<u64, u64, Identity>,
    hopscotch => kovan_map::HopscotchMap<u64, u64, Identity>,
);
