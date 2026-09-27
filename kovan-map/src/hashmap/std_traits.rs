//! The `std::collections::HashMap`-shaped trait set: `Default`, `Debug`, `Clone`, `Extend`,
//! `PartialEq` and `Eq`, each with std's own bounds where an honest concurrent implementation can
//! give them, built entirely on `HashMap`'s public API (`iter`, `get`, `len`,
//! `with_capacity_and_hasher`, `insert`). None of it touches a write path's own body.
//!
//! `FromIterator` and both `IntoIterator` impls live in `iter.rs`, next to the iterator types
//! they return; `Send`/`Sync`/`Drop` stay in the main module, next to the field layout they
//! reason about.

use super::HashMap;
use core::fmt;
use core::hash::{BuildHasher, Hash};

/// Creates an empty map with `S`'s default hasher and the default capacity, as
/// `std::collections::HashMap`'s `Default` does for any `S: Default`. Not `#[cfg(feature =
/// "std")]`: unlike [`HashMap::new`] (which needs `foldhash::fast::FixedState`'s `std`-only
/// default), this is generic over the caller's own `S` and asks nothing of it beyond `Default`.
impl<K, V, S> Default for HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

/// A snapshot of the map's entries, as `std::collections::HashMap`'s `Debug` prints its own.
impl<K, V, S> fmt::Debug for HashMap<K, V, S>
where
    K: Hash + Eq + Clone + fmt::Debug + 'static,
    V: Clone + fmt::Debug + 'static,
    S: BuildHasher,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// A snapshot copy: a new map with the same capacity and a clone of the hasher, populated by
/// inserting a clone of every entry this map held at some point during the walk. Like `iter`, a
/// concurrent write to `self` during the clone may or may not be reflected in the result.
impl<K, V, S> Clone for HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher + Clone,
{
    fn clone(&self) -> Self {
        let cloned = Self::with_capacity_and_hasher(self.capacity(), self.hasher().clone());
        for (k, v) in self.iter() {
            cloned.insert(k, v);
        }
        cloned
    }
}

/// Inserts every pair, through the same write path [`HashMap::extend`] (`&self`) uses: a `&mut
/// self` receiver only because [`Extend`] requires one, never taken as exclusive access to the
/// map's own writers.
impl<K, V, S> Extend<(K, V)> for HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        HashMap::extend(&*self, iter);
    }
}

/// As `std::collections::HashMap`'s `Extend<(&K, &V)>`: `K` and `V` copied out of the borrowed
/// pairs, then inserted the same way as the owned-pair impl above.
impl<'a, K, V, S> Extend<(&'a K, &'a V)> for HashMap<K, V, S>
where
    K: Hash + Eq + Copy + 'static,
    V: Copy + 'static,
    S: BuildHasher,
{
    fn extend<I: IntoIterator<Item = (&'a K, &'a V)>>(&mut self, iter: I) {
        HashMap::extend(&*self, iter.into_iter().map(|(k, v)| (*k, *v)));
    }
}

/// Content equality of a snapshot of each map: the same length and, for every pair this map's
/// walk found, the other map holding that key with an equal value. Exact at quiescence and
/// best-effort under a concurrent write to either map, the same honesty [`HashMap::len`]
/// documents; a lock-free map cannot hold both maps still for the comparison the way an
/// exclusively-borrowed `std::collections::HashMap` can.
impl<K, V, S> PartialEq for HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + PartialEq + 'static,
    S: BuildHasher,
{
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(k, v)| other.get(&k).is_some_and(|ov| ov == v))
    }
}

/// `V: Eq` makes the snapshot comparison above reflexive, symmetric and transitive at
/// quiescence, as `std::collections::HashMap`'s `Eq` is.
impl<K, V, S> Eq for HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + Eq + 'static,
    S: BuildHasher,
{
}
