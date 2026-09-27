//! Lock-Free Growing/Shrinking Hopscotch Hash Map
//!
//! # Features
//!
//! - **Lock-free reads**: a lookup and a walk take no guard and never wait for a writer.
//! - **Home-bucket writer guards**: every write that links, replaces, unlinks or moves an entry
//!   holds the writer guard of the entry's home bucket, so a writer's scan of its home is stable
//!   and a key never has two entries.
//! - **Displacement without a gap**: an insert whose neighborhood is full moves entries of other
//!   homes (the same allocation, not a copy) under their home guards. A moved entry is linked at
//!   its new slot before it is unlinked from the old one, and its home's move stamp advances in
//!   between, so a lookup that raced the move rescans instead of missing the key, and a walk
//!   meets the entry and yields it once.
//! - **Safe Resizing**: A resize (and a clear) takes every home bucket's writer guard of the
//!   table before it copies a slot, so a write either lands before the copy (and is copied) or
//!   waits for the new table; a table a resize replaced never admits a writer again. An insert
//!   that landed is final: it never retries into the new table, where it would meet its own
//!   migrated entry and report it as another caller's.
//! - **Stable iteration**: An iterator walks the table that was current when it was created to
//!   the end, even when a resize replaces it meanwhile.
//! - **Memory reclamation**: Uses Kovan.
//! - **Clone Support**: Supports V: Clone (e.g., `Arc<T>`) instead of just Copy.

extern crate alloc;

use crate::sync::spin_hint;
use crate::sync::{AtomicBool, AtomicUsize};
use alloc::boxed::Box;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use displace::{InsertResult, Pending};
use foldhash::fast::FixedState;
use kovan::{Atomic, CachePadded, pin, retire};
use table::{HOP_MASK, Table, Word, hop_bits};

pub use iter::{HopscotchIntoIter, HopscotchIter, HopscotchKeys, HopscotchValues};

mod displace;
mod iter;
mod resize;
mod std_traits;
mod table;

/// Neighborhood size (H parameter in hopscotch hashing)
const NEIGHBORHOOD_SIZE: usize = 32;

/// Initial capacity
const INITIAL_CAPACITY: usize = 64;

/// Whether `count` entries overfill a table of `capacity`: more than three quarters full.
#[inline(always)]
fn overfull(count: usize, capacity: usize) -> bool {
    4 * count > 3 * capacity
}

/// Whether `count` entries underfill a table of `capacity`: less than a quarter full, and the
/// table larger than the least one.
#[inline(always)]
fn underfull(count: usize, capacity: usize) -> bool {
    4 * count < capacity && capacity > MIN_CAPACITY
}

/// Minimum capacity to prevent excessive shrinking
const MIN_CAPACITY: usize = 64;

/// Maximum probe distance before giving up and resizing
const MAX_PROBE_DISTANCE: usize = 512;

/// A concurrent, lock-free hash map based on Hopscotch Hashing.
pub struct HopscotchMap<K: 'static, V: 'static, S = FixedState> {
    table: Atomic<Table<K, V>>,
    /// The entries in the map, on a cache line of its own: every write that links or unlinks an
    /// entry bumps it, and every operation reads the table pointer and the resize flag, which
    /// would otherwise share its line.
    count: CachePadded<AtomicUsize>,
    /// Prevents concurrent writes during resize migration to avoid lost updates
    resizing: AtomicBool,
    hasher: S,
}

/// How a write of `insert_impl` ended.
enum Outcome<R, V> {
    /// The key was absent: this call linked its entry; what the caller asked of its value.
    Linked(R),
    /// The key was present and this call replaced its entry: the old value.
    Replaced(V),
    /// The key was present and this call only claims an absent key: its value.
    Present(V),
}

// Small accessors that never hash: only the struct's own `'static` bound, as std's equivalent
// block for `with_hasher`/`with_capacity_and_hasher`/`capacity`/`len`/`is_empty`/`hasher` needs
// no `Hash`, `Eq`, `Clone` or `BuildHasher`. A method that hashes or clones a value lives in the
// bound impl block below instead.
impl<K: 'static, V: 'static, S> HopscotchMap<K, V, S> {
    /// Creates a new `HopscotchMap` with the specified hasher and default capacity.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(INITIAL_CAPACITY, hasher)
    }

    /// Creates a new `HopscotchMap` with the specified capacity and hasher.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let table = Table::new(capacity);
        Self {
            table: Atomic::new(Box::into_raw(Box::new(table))),
            count: CachePadded::new(AtomicUsize::new(0)),
            resizing: AtomicBool::new(false),
            hasher,
        }
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the current capacity of the map.
    pub fn capacity(&self) -> usize {
        let guard = pin();
        let table_ptr = self.table.load(Ordering::Acquire, &guard);
        unsafe { (*table_ptr.as_raw()).capacity }
    }

    /// Get the underlying hasher.
    pub fn hasher(&self) -> &S {
        &self.hasher
    }
}

#[cfg(feature = "std")]
impl<K, V> HopscotchMap<K, V, FixedState>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    /// Creates a new `HopscotchMap` with default capacity and hasher.
    pub fn new() -> Self {
        Self::with_hasher(FixedState::default())
    }

    /// Creates a new `HopscotchMap` with the specified capacity and default hasher.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, FixedState::default())
    }
}

impl<K, V, S> HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    #[inline]
    fn wait_for_resize(&self) {
        while self.resizing.load(Ordering::Acquire) {
            spin_hint();
        }
    }

    /// Returns the value corresponding to the key.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let guard = pin();
        let table_ptr = self.table.load(Ordering::Acquire, &guard);
        let table = unsafe { &*table_ptr.as_raw() };

        table
            .lookup(hash, key, &guard)
            .map(|entry| entry.value.clone())
    }

    /// Inserts a key-value pair into the map, returning the value it replaced.
    ///
    /// Linearizable at the write under the key's home guard; the answer is
    /// exactly the value that write replaced.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        match self.insert_impl(key, value, false, |_| ()) {
            Outcome::Linked(()) => None,
            Outcome::Replaced(old) | Outcome::Present(old) => Some(old),
        }
    }

    /// The one write path of `insert`, `insert_if_absent` and `get_or_insert`: link the key's
    /// entry, or (unless `only_if_absent`) replace the present one, under the key's home guard.
    /// `on_insert` reads the value this call linked.
    fn insert_impl<R>(
        &self,
        key: K,
        value: V,
        only_if_absent: bool,
        on_insert: impl FnOnce(&V) -> R,
    ) -> Outcome<R, V> {
        let hash = self.hasher.hash_one(&key);
        let mut pending = Pending::Parts { hash, key, value };

        loop {
            self.wait_for_resize();

            let guard = pin();
            let table_ptr = self.table.load(Ordering::Acquire, &guard);
            let table = unsafe { &*table_ptr.as_raw() };

            if self.resizing.load(Ordering::Acquire) {
                continue;
            }

            // Home-bucket writer guard: serialize the writes of one home so the existence scan
            // and the slot claim inside try_insert are one atomic step, and no displacement
            // moves an entry of the home meanwhile. Contended -> spin via the outer loop, which
            // keeps re-checking `resizing` and reloads the table. A resize takes every home
            // guard of the table before it copies a slot (`hold_writers`) and keeps the guards
            // of a table it replaced, so holding this guard means the table is live and every
            // slot this call writes is copied by any resize that follows. No lock-order
            // deadlock: a guard holder never waits for anything (it takes the guards of other
            // homes it displaces entries of without waiting).
            let Some(mut home) = table.home_guard(table.bucket_index(hash)) else {
                #[cfg(test)]
                pause::at(pause::Point::WriterMetHeldGuard);
                // A call that only claims an absent key answers a present one without waiting
                // for the writer holding its home: from the lookup `get` makes, as
                // `get_or_insert` answers a present key before it writes.
                if only_if_absent && let Some(entry) = table.lookup(hash, pending.key(), &guard) {
                    return Outcome::Present(entry.value.clone());
                }
                spin_hint();
                continue;
            };

            let outcome = Self::try_insert(table, &mut home, pending, only_if_absent, &guard);
            // A new entry is counted once, before its home guard is released: concurrent
            // removes cannot decrement the count below the true entry count, and a clear
            // (which resets the count while it holds every home guard) never sees the entry
            // without its count.
            let new_count = match outcome {
                InsertResult::Linked(_) => Some(self.count.fetch_add(1, Ordering::Relaxed) + 1),
                _ => None,
            };
            // The guard is released (publishing the new entry's hop bit) before the resize arms
            // below drop the pin and migrate: the release touches this table's bucket, which is
            // only safe while the pin keeps the table alive.
            drop(home);
            match outcome {
                InsertResult::Linked(entry) => {
                    #[cfg(test)]
                    pause::at(pause::Point::AfterLanding);
                    // Final. The write landed in a live table under its home
                    // guard, so a resize that starts after it copies it; a
                    // retry here would meet this call's own migrated entry
                    // and report it as present (`insert_if_absent` answering
                    // `Some(own value)` for an insert that happened).
                    // SAFETY: linked by this call under `guard`, which keeps it (and its table)
                    // from being freed even if a writer unlinks it now.
                    let answer = on_insert(unsafe { &(*entry).value });
                    if let Some(new_count) = new_count
                        && overfull(new_count, table.capacity)
                    {
                        let current_capacity = table.capacity;
                        drop(guard);
                        self.try_resize(current_capacity * 2);
                    }
                    return Outcome::Linked(answer);
                }
                InsertResult::Replaced(old) => return Outcome::Replaced(old),
                InsertResult::Exists(existing) => return Outcome::Present(existing),
                InsertResult::NeedResize(back) => {
                    pending = back;
                    let current_capacity = table.capacity;
                    drop(guard);
                    self.try_resize(current_capacity * 2);
                }
                InsertResult::Retry(back) => {
                    pending = back;
                    spin_hint();
                }
            }
        }
    }

    /// Returns the value corresponding to the key, or inserts the given value if the key is not present.
    ///
    /// Linearizable and exact: of the callers racing for an absent key, the
    /// one whose write links the key's entry (under the key's home guard)
    /// gets its own value back, and every other gets the value it found
    /// there (the winner's, unless a later write already replaced it). A
    /// present key is answered by a lookup that takes no guard.
    pub fn get_or_insert(&self, key: K, value: V) -> V {
        if let Some(v) = self.get(&key) {
            return v;
        }
        match self.insert_impl(key, value, true, V::clone) {
            Outcome::Linked(own) | Outcome::Present(own) | Outcome::Replaced(own) => own,
        }
    }

    /// Insert a key-value pair only if the key does not exist.
    /// Returns `None` if inserted, `Some(existing_value)` if the key already exists.
    ///
    /// Exact: `None` exactly when this call linked the key's entry, `Some`
    /// with the value it found under the key's home guard otherwise; never
    /// both, and a resize in flight changes neither (a write that landed is
    /// final). A call that finds the key present drops its own key and value,
    /// once.
    pub fn insert_if_absent(&self, key: K, value: V) -> Option<V> {
        match self.insert_impl(key, value, true, |_| ()) {
            Outcome::Linked(()) => None,
            Outcome::Present(existing) | Outcome::Replaced(existing) => Some(existing),
        }
    }

    /// Remove **all** nodes matching `key`, returning the most recent value
    /// if the key was present.
    ///
    /// Every write of a home runs under its writer guard, so a key has at
    /// most one entry and this removes what [`remove`](Self::remove)
    /// removes. It keeps removing until a scan finds no match, so the key is
    /// absent at the linearization point of the final scan, and it is kept
    /// for parity with `HashMap::force_remove`.
    ///
    /// Note: a concurrent `insert` of the same key can land after the final
    /// scan, as with any removal under contention.
    pub fn force_remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut newest = None;
        loop {
            match self.remove(key) {
                Some(v) => {
                    // The first removal unlinks the first match in scan
                    // order - the live (most recent) version.
                    if newest.is_none() {
                        newest = Some(v);
                    }
                }
                None => return newest,
            }
        }
    }

    /// Removes a key from the map, returning the value at the key if the key was previously in the map.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);

        loop {
            self.wait_for_resize();

            let guard = pin();
            let table_ptr = self.table.load(Ordering::Acquire, &guard);
            let table = unsafe { &*table_ptr.as_raw() };

            if self.resizing.load(Ordering::Acquire) {
                continue;
            }

            let home_idx = table.bucket_index(hash);
            // A home without hop bits has no entry to remove, and seeing that takes no guard:
            // an entry's bit stays set from its publication to its unlink. Relaxed: no slot is
            // read.
            let word = table.get_bucket(home_idx).control.load(Ordering::Relaxed);
            if hop_bits(word) == 0 {
                return None;
            }

            // The home guard, as an insert takes it: the scan below is stable, the unlink cannot
            // race an update or a move of the entry, and a resize copies the table either before
            // this call takes the guard (and this call then waits for the new table) or after
            // the unlink.
            let Some(mut home) = table.home_guard(home_idx) else {
                #[cfg(test)]
                pause::at(pause::Point::WriterMetHeldGuard);
                spin_hint();
                continue;
            };
            let (offset, word) = table.find(home_idx, home.hops(), hash, key, &guard)?;
            let entry_ptr = word.ptr();
            // SAFETY: loaded under `guard`, which keeps it from being freed.
            let old_value = unsafe { &*entry_ptr }.value.clone();
            // A store, not a CAS: no other thread writes an occupied slot of a home whose guard
            // this call holds. Release: a reader that acquires the free slot sees everything this
            // call wrote before it.
            table
                .get_bucket(home_idx + offset)
                .store(Word::free(), Ordering::Release);
            home.stage_unlinked(offset);

            // Counted down before the home guard is released, as an insert counts up: a clear
            // resets the count while it holds every home guard, so no decrement for an entry it
            // already cleared lands after the reset and eats the count of a later insert.
            // Saturating decrement: prevent count from wrapping to usize::MAX which would
            // trigger catastrophic cascading resizes.
            let shrink_to = if let Ok(prev) =
                self.count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| c.checked_sub(1))
            {
                underfull(prev - 1, table.capacity).then_some(table.capacity / 2)
            } else {
                None
            };
            drop(home);

            // SAFETY: unlinked above under its home guard, so no other thread unlinks or
            // retires it; a reader that loaded it holds a guard that keeps it alive.
            unsafe { retire(entry_ptr) };

            if let Some(cap) = shrink_to {
                drop(guard);
                self.try_resize(cap);
            }
            return Some(old_value);
        }
    }

    /// Clears the map, removing all key-value pairs.
    pub fn clear(&self) {
        while self
            .resizing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spin_hint();
        }

        let guard = pin();
        let table_ptr = self.table.load(Ordering::Acquire, &guard);
        let table = unsafe { &*table_ptr.as_raw() };
        // No insert is between its scan and its claim while the slots are
        // cleared: one that landed is cleared with the rest, one that did
        // not lands after the clear with its hop bit intact. No move has an
        // entry in two slots either (a move holds its entry's home guard).
        Self::hold_writers(table);

        for i in 0..(table.capacity + NEIGHBORHOOD_SIZE) {
            let bucket = table.get_bucket(i);
            let word = bucket.load(Ordering::Acquire, &guard);

            if !word.is_free()
                && bucket.replace(
                    word,
                    Word::free(),
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                )
            {
                unsafe { retire(word.ptr()) };
            }

            if i < table.capacity {
                // Keeps the guard (held by this clear) and the move stamp.
                let b = table.get_bucket(i);
                b.control.fetch_and(!HOP_MASK, Ordering::Release);
            }
        }

        self.count.store(0, Ordering::Release);
        Self::release_writers(table);
        self.resizing.store(false, Ordering::Release);
    }

    /// Returns `true` if the key is present.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Insert all `(K, V)` pairs from `iter`. Takes `&self` (concurrent map).
    pub fn extend<I: IntoIterator<Item = (K, V)>>(&self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

unsafe impl<K: Send, V: Send, S: Send> Send for HopscotchMap<K, V, S> {}
// SAFETY: Shared references allow moving K and V across threads (via insert/remove),
// so K and V must be Send in addition to Sync.
unsafe impl<K: Send + Sync, V: Send + Sync, S: Send + Sync> Sync for HopscotchMap<K, V, S> {}

impl<K, V, S> Drop for HopscotchMap<K, V, S> {
    fn drop(&mut self) {
        // SAFETY: `drop(&mut self)` guarantees exclusive ownership - no concurrent
        // readers can exist. The Table's destructor frees the remaining entries.
        let guard = pin();
        let table_ptr = self.table.load(Ordering::Acquire, &guard);

        unsafe {
            drop(Box::from_raw(table_ptr.as_raw()));
        }

        // Flush nodes previously retired by concurrent operations (insert/remove/resize)
        // to prevent use-after-free during process teardown.
        drop(guard);
        kovan::flush();
    }
}

#[cfg(test)]
mod pause;

#[cfg(test)]
mod tests;
