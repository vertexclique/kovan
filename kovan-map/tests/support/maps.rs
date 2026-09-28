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
    fn remove_if(&self, k: &K, pred: impl FnOnce(&V) -> bool) -> Option<V>;
    fn compare_and_remove(&self, k: &K, expected: &V) -> Option<V>
    where
        V: PartialEq;
    fn replace_if(&self, k: K, v: V, pred: impl FnOnce(&V) -> bool) -> Result<V, Option<V>>;
    fn compare_and_swap(&self, k: K, expected: &V, v: V) -> Result<V, Option<V>>
    where
        V: PartialEq;
    fn compute(&self, k: K, f: impl FnOnce(Option<&V>) -> Option<V>) -> Option<V>;
    fn len(&self) -> usize;
    fn clear(&self);
    fn entries(&self) -> Vec<(K, V)>;
    /// What `entries` returns, gathered by the walk's own loop (`for_each`) instead of `next`.
    fn entries_by_fold(&self) -> Vec<(K, V)>;
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
            fn remove_if(&self, k: &K, pred: impl FnOnce(&V) -> bool) -> Option<V> {
                $map::remove_if(self, k, pred)
            }
            fn compare_and_remove(&self, k: &K, expected: &V) -> Option<V>
            where
                V: PartialEq,
            {
                $map::compare_and_remove(self, k, expected)
            }
            fn replace_if(
                &self,
                k: K,
                v: V,
                pred: impl FnOnce(&V) -> bool,
            ) -> Result<V, Option<V>> {
                $map::replace_if(self, k, v, pred)
            }
            fn compare_and_swap(&self, k: K, expected: &V, v: V) -> Result<V, Option<V>>
            where
                V: PartialEq,
            {
                $map::compare_and_swap(self, k, expected, v)
            }
            fn compute(&self, k: K, f: impl FnOnce(Option<&V>) -> Option<V>) -> Option<V> {
                $map::compute(self, k, f)
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
            fn entries_by_fold(&self) -> Vec<(K, V)> {
                let mut all = Vec::new();
                $map::iter(self).for_each(|entry| all.push(entry));
                all
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

/// A hasher that shapes a `u64` key's hash with `SHAPE` (the key given to `write_u64`).
pub struct ShapedHasher<const SHAPE: u8>(u64);

impl<const SHAPE: u8> Hasher for ShapedHasher<SHAPE> {
    fn finish(&self) -> u64 {
        match SHAPE {
            // Sixteen consecutive keys share one hash: a chain of sixteen in the chained map.
            0 => self.0 / 16,
            // Sixteen consecutive keys get consecutive hashes, groups 64 apart: neighborhoods
            // that overlap, displacements, and groups folding onto one another at small
            // capacities.
            _ => self.0 / 16 * 64 + self.0 % 16,
        }
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

/// Sixteen consecutive keys share each hash.
#[derive(Clone, Copy, Default)]
pub struct Grouped;

impl BuildHasher for Grouped {
    type Hasher = ShapedHasher<0>;
    fn build_hasher(&self) -> ShapedHasher<0> {
        ShapedHasher(0)
    }
}

/// Sixteen consecutive keys get consecutive hashes, their groups 64 apart.
#[derive(Clone, Copy, Default)]
pub struct Clustered;

impl BuildHasher for Clustered {
    type Hasher = ShapedHasher<1>;
    fn build_hasher(&self) -> ShapedHasher<1> {
        ShapedHasher(0)
    }
}
