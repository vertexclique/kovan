//! Both maps behind one trait, so a test states a property once and checks it on `HashMap` and
//! `HopscotchMap`, and the hashers that place keys where a test needs them.

use core::hash::{BuildHasher, Hash, Hasher};
use kovan_map::{HashMap, HopscotchMap};

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
