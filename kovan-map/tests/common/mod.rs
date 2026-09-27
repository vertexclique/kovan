//! Shared by the race and linearizability suite (`concurrent_maps.rs`): one trait over both maps
//! (so every property is checked against `HashMap` and `HopscotchMap` alike), the hashers that
//! place keys where a test needs them, a value that counts its drops, a seeded generator, and a
//! checker of recorded concurrent histories.

use core::hash::{BuildHasher, Hash, Hasher};
use kovan_map::{HashMap, HopscotchMap};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Both maps, as the tests use them.
pub trait Map<K, V>: Send + Sync + 'static {
    const NAME: &'static str;
    fn with_capacity(capacity: usize) -> Self;
    fn insert(&self, k: K, v: V) -> Option<V>;
    fn insert_if_absent(&self, k: K, v: V) -> Option<V>;
    fn get_or_insert(&self, k: K, v: V) -> V;
    fn get(&self, k: &K) -> Option<V>;
    fn contains_key(&self, k: &K) -> bool;
    fn remove(&self, k: &K) -> Option<V>;
    fn force_remove(&self, k: &K) -> Option<V>;
    fn len(&self) -> usize;
    fn clear(&self);
    fn entries(&self) -> Vec<(K, V)>;
}

macro_rules! impl_map {
    ($map:ident, $name:literal) => {
        impl<K, V, S> Map<K, V> for $map<K, V, S>
        where
            K: Hash + Eq + Clone + Send + Sync + 'static,
            V: Clone + Send + Sync + 'static,
            S: BuildHasher + Default + Send + Sync + 'static,
        {
            const NAME: &'static str = $name;
            fn with_capacity(capacity: usize) -> Self {
                $map::with_capacity_and_hasher(capacity, S::default())
            }
            fn insert(&self, k: K, v: V) -> Option<V> {
                $map::insert(self, k, v)
            }
            fn insert_if_absent(&self, k: K, v: V) -> Option<V> {
                $map::insert_if_absent(self, k, v)
            }
            fn get_or_insert(&self, k: K, v: V) -> V {
                $map::get_or_insert(self, k, v)
            }
            fn get(&self, k: &K) -> Option<V> {
                $map::get(self, k)
            }
            fn contains_key(&self, k: &K) -> bool {
                $map::contains_key(self, k)
            }
            fn remove(&self, k: &K) -> Option<V> {
                $map::remove(self, k)
            }
            fn force_remove(&self, k: &K) -> Option<V> {
                $map::force_remove(self, k)
            }
            fn len(&self) -> usize {
                $map::len(self)
            }
            fn clear(&self) {
                $map::clear(self)
            }
            fn entries(&self) -> Vec<(K, V)> {
                $map::iter(self).collect()
            }
        }
    };
}

impl_map!(HashMap, "HashMap");
impl_map!(HopscotchMap, "HopscotchMap");

/// Hashes a `u64` key to itself: a test places each key in the bucket it names.
#[derive(Clone, Copy, Default)]
pub struct Identity;

pub struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = (self.0 << 8) | u64::from(*b);
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

/// A hasher whose `write_u64` ignores its input: every key hashes to 7.
pub struct ConstHasher;

impl Hasher for ConstHasher {
    fn finish(&self) -> u64 {
        7
    }
    fn write(&mut self, _: &[u8]) {}
}

/// Every key hashes to one value: every key of a `HashMap` shares one chain, at any capacity.
#[derive(Clone, Copy, Default)]
pub struct Constant;

impl BuildHasher for Constant {
    type Hasher = ConstHasher;
    fn build_hasher(&self) -> ConstHasher {
        ConstHasher
    }
}

/// A value that counts how many were made and how many dropped, so a test sees a leak or a
/// double free of any value a map held, lost or dropped.
#[derive(Debug)]
pub struct Tracked {
    pub id: u64,
    live: Arc<Counts>,
}

#[derive(Default, Debug)]
pub struct Counts {
    pub made: AtomicUsize,
    pub dropped: AtomicUsize,
}

impl Counts {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn value(self: &Arc<Self>, id: u64) -> Tracked {
        self.made.fetch_add(1, Ordering::Relaxed);
        Tracked {
            id,
            live: Arc::clone(self),
        }
    }
    /// Every value made was dropped exactly once.
    pub fn balanced(&self) -> bool {
        self.made.load(Ordering::SeqCst) == self.dropped.load(Ordering::SeqCst)
    }
}

impl Clone for Tracked {
    fn clone(&self) -> Self {
        self.live.value(self.id)
    }
}

impl PartialEq for Tracked {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Wait until the reclamation freed every node retired so far, so a drop count reads final:
/// flush (each call also adopts one orphaned batch of an exited thread) until the drop count
/// stops moving. A leak converges below the count of values made and fails the caller's check;
/// nodes merely in flight converge up to it. Only meaningful while no other test's threads hold
/// kovan reservations (every test of the suite runs under `serial`).
pub fn settle(counts: &Counts) {
    let mut last = counts.dropped.load(Ordering::SeqCst);
    let mut stable = 0;
    for _ in 0..4_000 {
        kovan::flush();
        let now = counts.dropped.load(Ordering::SeqCst);
        if now == last {
            stable += 1;
            if stable >= 32 {
                return;
            }
        } else {
            stable = 0;
            last = now;
        }
    }
}

/// The suite's tests one at a time: a thread of another test holding a kovan reservation would
/// keep this test's retired values from being freed (and its drop count from settling).
pub fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// xorshift64*: a seeded generator, so a failing round names the seed that replays its choices.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The rounds a stress test runs: `KOVAN_STRESS_SCALE` multiplies the default (the long runs
/// the release report records set it; the default keeps `cargo test` short).
pub fn scaled(default: usize) -> usize {
    std::env::var("KOVAN_STRESS_SCALE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map_or(default, |m| default * m.max(1))
}

// ------------------------------------------------------------------ linearizability

/// One operation on one key, as a thread invoked it and saw it answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Call {
    Insert(u64),
    InsertIfAbsent(u64),
    GetOrInsert(u64),
    Remove,
    Get,
}

/// A call, its answer (`None` for absent, the value otherwise; `get_or_insert` answers a value
/// always), and when it was invoked and answered on one global clock.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub call: Call,
    pub answer: Option<u64>,
    pub invoked: u64,
    pub answered: u64,
}

/// Apply `call` to a one-key register holding `state`: the answer and the new state.
fn apply(call: Call, state: Option<u64>) -> (Option<u64>, Option<u64>) {
    match call {
        Call::Insert(v) => (state, Some(v)),
        Call::InsertIfAbsent(v) => match state {
            None => (None, Some(v)),
            Some(cur) => (Some(cur), state),
        },
        Call::GetOrInsert(v) => match state {
            None => (Some(v), Some(v)),
            Some(cur) => (Some(cur), state),
        },
        Call::Remove => (state, None),
        Call::Get => (state, state),
    }
}

/// Whether the history of one key has a linearization from `initial` (Wing and Gong's search,
/// memoized on the set of calls placed and the register's value): every call placed at a moment
/// after every call answered before it was invoked, each answering what the register answers.
pub fn linearizable(history: &[Event], initial: Option<u64>) -> bool {
    assert!(history.len() <= 63, "a history of at most 63 calls");
    let full: u64 = (1u64 << history.len()) - 1;
    let mut seen: HashSet<(u64, Option<u64>)> = HashSet::new();
    let mut stack = vec![(0u64, initial)];
    while let Some((done, state)) = stack.pop() {
        if done == full {
            return true;
        }
        if !seen.insert((done, state)) {
            continue;
        }
        // The earliest answer among the calls not placed: a call invoked after it cannot go
        // before it.
        let horizon = history
            .iter()
            .enumerate()
            .filter(|(i, _)| done & (1 << i) == 0)
            .map(|(_, e)| e.answered)
            .min()
            .unwrap_or(u64::MAX);
        for (i, e) in history.iter().enumerate() {
            if done & (1 << i) != 0 || e.invoked > horizon {
                continue;
            }
            let (answer, next) = apply(e.call, state);
            if answer == e.answer {
                stack.push((done | (1 << i), next));
            }
        }
    }
    false
}
