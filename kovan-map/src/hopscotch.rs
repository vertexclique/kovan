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

use crate::hashmap::resize_spin_hint;
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::marker::PhantomData;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use foldhash::fast::FixedState;
use kovan::{Atomic, RetiredNode, Shared, pin, retire};

/// Neighborhood size (H parameter in hopscotch hashing)
const NEIGHBORHOOD_SIZE: usize = 32;

/// Initial capacity
const INITIAL_CAPACITY: usize = 64;

/// Load factor threshold for growing (75%)
const GROW_THRESHOLD: f64 = 0.75;

/// Load factor threshold for shrinking (25%)
const SHRINK_THRESHOLD: f64 = 0.25;

/// Minimum capacity to prevent excessive shrinking
const MIN_CAPACITY: usize = 64;

/// Maximum probe distance before giving up and resizing
const MAX_PROBE_DISTANCE: usize = 512;

// A bucket's control word describes the bucket as a home, in three fields:
// - the hop bits (`HOP_MASK`): bit `i` set means slot `home + i` holds an entry of this home. An
//   entry's bit is set after it is linked and cleared after it is unlinked, and a move sets the
//   new slot's bit before it unlinks the old slot, so from the publication of its bit to its
//   removal an entry is always in a slot its home's bits name;
// - the home's writer guard (`GUARD`, Herlihy-style hopscotch): held across every write that
//   links, replaces, unlinks or moves an entry of the home (an insert's existence scan and slot
//   claim, a remove, a displacement moving one of the home's entries) and, for every home at
//   once, by a resize or a clear. Readers never take it;
// - the move stamp (`STAMP_MASK`): advanced each time an entry of the home moves, before the entry
//   leaves its old slot. A lookup that misses re-reads the word and scans again when the stamp
//   moved (see `get`). It wraps after 2^31 moves; a lookup could only be fooled by exactly a
//   multiple of 2^31 moves of one home's entries between its two reads of the word, and an entry
//   moves at most `NEIGHBORHOOD_SIZE - 1` times.
// Only the guard's holder changes the hop bits and the stamp (a writer that fails to take the
// guard ORs in a bit that is already set, changing nothing), so the holder writes the word with
// plain stores from its own copy (`HomeGuard`). One word keeps a bucket at 16 bytes and gives a
// lookup its hop bits and its stamp in one load.

/// The hop bits of a control word.
const HOP_MASK: u64 = (1u64 << NEIGHBORHOOD_SIZE) - 1;

/// The home's writer guard in a control word.
const GUARD: u64 = 1u64 << NEIGHBORHOOD_SIZE;

/// One step of a control word's move stamp.
const STAMP_UNIT: u64 = GUARD << 1;

/// The move stamp of a control word.
const STAMP_MASK: u64 = !(HOP_MASK | GUARD);

const _: () = assert!(NEIGHBORHOOD_SIZE <= 32, "a home's hop bits fit a u32");

/// The hop bits of a control word.
#[inline(always)]
fn hop_bits(word: u64) -> u32 {
    (word & HOP_MASK) as u32
}

/// The hop bit of the slot `offset` past its home.
#[inline(always)]
fn hop_bit(offset: usize) -> u64 {
    1u64 << offset
}

/// A bucket in the hopscotch hash table
struct Bucket<K, V> {
    /// This bucket as a home: its hop bits, its writer guard and its move stamp (see the
    /// control word comment above `HOP_MASK`).
    control: AtomicU64,
    /// The actual key-value slot at this position
    slot: Atomic<Entry<K, V>>,
}

/// A held writer guard of one home bucket, with the home's control word as this holder last
/// wrote it. Dropping it releases the guard, publishing the staged hop bits in the same store.
struct HomeGuard<'t> {
    control: &'t AtomicU64,
    /// The home bucket's index.
    idx: usize,
    /// The control word as this holder publishes it next, guard bit set.
    word: u64,
}

impl HomeGuard<'_> {
    /// The home's hop bits.
    #[inline(always)]
    fn hops(&self) -> u32 {
        hop_bits(self.word)
    }

    /// Stage the hop bit of the slot `offset` past the home (an entry linked there), published
    /// when the guard is released.
    #[inline(always)]
    fn stage_linked(&mut self, offset: usize) {
        self.word |= hop_bit(offset);
    }

    /// Stage clearing the hop bit of the slot `offset` past the home (its entry unlinked),
    /// published when the guard is released.
    #[inline(always)]
    fn stage_unlinked(&mut self, offset: usize) {
        self.word &= !hop_bit(offset);
    }

    /// Publish now, still holding the guard, the hop bit of the slot `offset` past the home (an
    /// entry moved there) with the move stamp advanced.
    #[inline]
    fn publish_moved_in(&mut self, offset: usize) {
        // Wrapping: the stamp is the top of the word, a carry out of it is dropped.
        self.word = (self.word | hop_bit(offset)).wrapping_add(STAMP_UNIT);
        // Release: a reader that acquires this word sees the entry linked at `offset` before it.
        self.control.store(self.word, Ordering::Release);
    }
}

impl Drop for HomeGuard<'_> {
    fn drop(&mut self) {
        // Release: pairs with the next holder's acquire (and a reader's), publishing every slot
        // and hop bit this holder wrote.
        self.control.store(self.word & !GUARD, Ordering::Release);
    }
}

/// An entry in the hash table
#[repr(C)]
struct Entry<K, V> {
    retired: RetiredNode,
    hash: u64,
    key: K,
    value: V,
}

// SAFETY (kovan retirement rule): a retired Entry's destructor may run on
// any thread, and entries (with K and V inside) move between threads -
// hence `K: Send, V: Send` for Send. Lookups DO produce `&K`/`&V` from a
// shared `&Entry` (get() clones V through &V under concurrent readers),
// so Sync additionally requires `K: Sync, V: Sync` - the same bounds the
// map-level Sync impl has always required for sharing the map.
unsafe impl<K: Send, V: Send> Send for Entry<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Entry<K, V> {}

/// The null entry pointer: a free slot.
#[inline(always)]
fn null_entry<'g, K, V>() -> Shared<'g, Entry<K, V>> {
    // SAFETY: a null `Shared` points at nothing, so it is valid for any lifetime.
    unsafe { Shared::from_raw(core::ptr::null_mut()) }
}

/// Claim the free slot `slot` for `entry`: `Ok` when the slot took it (the table owns it now),
/// `Err` with the entry back when another writer took the slot first.
fn link<K, V>(
    slot: &Atomic<Entry<K, V>>,
    entry: Box<Entry<K, V>>,
    guard: &kovan::Guard,
) -> Result<(), Box<Entry<K, V>>> {
    let raw = Box::into_raw(entry);
    // Release: a reader that acquires the slot sees the entry's fields. Relaxed on failure: the
    // value read is not used.
    match slot.compare_exchange(
        null_entry(),
        unsafe { Shared::from_raw(raw) },
        Ordering::Release,
        Ordering::Relaxed,
        guard,
    ) {
        Ok(_) => Ok(()),
        // SAFETY: the CAS failed, so `raw` was never published: it is still the allocation
        // `Box::into_raw` gave this call.
        Err(_) => Err(unsafe { Box::from_raw(raw) }),
    }
}

/// The hash table structure
#[repr(C)]
struct Table<K, V> {
    retired: RetiredNode,
    buckets: Box<[Bucket<K, V>]>,
    capacity: usize,
    mask: usize,
}

// SAFETY (kovan retirement rule): same reasoning as Entry - a retired
// Table's destructor may run on any thread (hence K, V: Send via the
// contained entries); shared access to entries through a `&Table` carries
// Entry's Sync requirements.
unsafe impl<K: Send, V: Send> Send for Table<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Table<K, V> {}

impl<K, V> Table<K, V> {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(MIN_CAPACITY);
        // We add padding to the array so we don't have to check bounds constantly
        // during neighborhood scans.
        let mut buckets = Vec::with_capacity(capacity + NEIGHBORHOOD_SIZE);

        for _ in 0..(capacity + NEIGHBORHOOD_SIZE) {
            buckets.push(Bucket {
                control: AtomicU64::new(0),
                slot: Atomic::null(),
            });
        }

        Self {
            retired: RetiredNode::new(),
            buckets: buckets.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
        }
    }

    #[inline(always)]
    fn bucket_index(&self, hash: u64) -> usize {
        (hash as usize) & self.mask
    }

    #[inline(always)]
    fn get_bucket(&self, idx: usize) -> &Bucket<K, V> {
        // SAFETY: Internal indices are calculated via mask or bounded offset loops.
        // The buckets array has padding to handle overflow up to NEIGHBORHOOD_SIZE.
        unsafe { self.buckets.get_unchecked(idx) }
    }

    /// Whether slot `idx` looks free. Relaxed: only a hint for where to try, the claim's CAS
    /// decides.
    #[inline(always)]
    fn looks_free(&self, idx: usize, guard: &kovan::Guard) -> bool {
        self.get_bucket(idx)
            .slot
            .load(Ordering::Relaxed, guard)
            .is_null()
    }

    /// Take the writer guard of the home bucket `idx` without waiting: `None` when another
    /// writer holds it.
    #[inline]
    fn home_guard(&self, idx: usize) -> Option<HomeGuard<'_>> {
        let control = &self.get_bucket(idx).control;
        // Acquire: pairs with the previous holder's release, so every slot and hop bit it wrote
        // is visible to this holder.
        let prev = control.fetch_or(GUARD, Ordering::Acquire);
        if prev & GUARD != 0 {
            return None;
        }
        Some(HomeGuard {
            control,
            idx,
            word: prev | GUARD,
        })
    }

    /// The entry of `key` among the slots `hops` names past the home `home`, and its offset
    /// there.
    #[inline]
    fn find<'g, Q>(
        &self,
        home: usize,
        hops: u32,
        hash: u64,
        key: &Q,
        guard: &'g kovan::Guard,
    ) -> Option<(usize, Shared<'g, Entry<K, V>>)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut rest = hops;
        while rest != 0 {
            let offset = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            let slot = &self.get_bucket(home + offset).slot;
            // Acquire: pairs with the release that linked the entry, so its fields are visible.
            let entry_ptr = slot.load(Ordering::Acquire, guard);
            if !entry_ptr.is_null() {
                // SAFETY: loaded under `guard`, which keeps it from being freed.
                let entry = unsafe { &*entry_ptr.as_raw() };
                if entry.hash == hash && entry.key.borrow() == key {
                    return Some((offset, entry_ptr));
                }
            }
        }
        None
    }
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        // Exclusive at this point: either the map itself is being dropped,
        // or kovan reclaimed the table after every guard that could observe
        // it has been released. Slots hold exactly the entries that were
        // never individually unlinked+retired (remove/clear null the slot
        // before retiring), so each entry is freed exactly once. No entry is
        // in two slots: a move links an entry twice only while it holds the
        // entry's home guard, and a resize holds every home guard before it
        // replaces (and retires) a table. Without this, every resize leaked
        // the old table's entries.
        let guard = pin();
        for i in 0..(self.capacity + NEIGHBORHOOD_SIZE) {
            let entry_ptr = self.buckets[i]
                .slot
                .load(Ordering::Relaxed, &guard)
                .as_raw();
            if !entry_ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(entry_ptr));
                }
            }
        }
    }
}

/// A concurrent, lock-free hash map based on Hopscotch Hashing.
pub struct HopscotchMap<K: 'static, V: 'static, S = FixedState> {
    table: Atomic<Table<K, V>>,
    count: AtomicUsize,
    /// Prevents concurrent writes during resize migration to avoid lost updates
    resizing: AtomicBool,
    hasher: S,
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
    /// Creates a new `HopscotchMap` with the specified hasher and default capacity.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(INITIAL_CAPACITY, hasher)
    }

    /// Creates a new `HopscotchMap` with the specified capacity and hasher.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let table = Table::new(capacity);
        Self {
            table: Atomic::new(Box::into_raw(Box::new(table))),
            count: AtomicUsize::new(0),
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

    #[inline]
    fn wait_for_resize(&self) {
        while self.resizing.load(Ordering::Acquire) {
            resize_spin_hint();
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

        let home = table.bucket_index(hash);
        let control = &table.get_bucket(home).control;
        // Acquire: pairs with the release store that set each hop bit, so the entry linked
        // before it is visible to the scan.
        let mut word = control.load(Ordering::Acquire);
        loop {
            let hops = hop_bits(word);
            if hops == 0 {
                return None;
            }
            #[cfg(test)]
            pause::at(pause::Point::LookupReadHops);
            if let Some((_, entry_ptr)) = table.find(home, hops, hash, key, &guard) {
                // SAFETY: loaded under `guard`, which keeps it from being freed.
                return Some(unsafe { &*entry_ptr.as_raw() }.value.clone());
            }
            // A miss is final unless an entry of this home moved while the scan ran. A move
            // links the entry at its new slot, sets that slot's hop bit and advances the stamp,
            // and only then empties the old slot with a release store. A scan that found the
            // old slot empty (or reused) acquired that store, so this re-read sees the advanced
            // stamp and the new bit with it. Acquire: the rescan reads the slots the new bits
            // name.
            let again = control.load(Ordering::Acquire);
            if (again ^ word) & STAMP_MASK == 0 {
                return None;
            }
            word = again;
        }
    }

    /// Inserts a key-value pair into the map.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.insert_impl(key, value, false)
    }

    /// Helper for get_or_insert logic.
    fn insert_impl(&self, key: K, value: V, only_if_absent: bool) -> Option<V> {
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
                resize_spin_hint();
                continue;
            };

            let outcome = Self::try_insert(table, &mut home, pending, only_if_absent, &guard);
            // A new entry is counted once, before its home guard is released: concurrent
            // removes cannot decrement the count below the true entry count, and a clear
            // (which resets the count while it holds every home guard) never sees the entry
            // without its count.
            let new_count = match outcome {
                InsertResult::Success(None) => {
                    Some(self.count.fetch_add(1, Ordering::Relaxed) + 1)
                }
                _ => None,
            };
            // The guard is released (publishing the new entry's hop bit) before the resize arms
            // below drop the pin and migrate: the release touches this table's bucket, which is
            // only safe while the pin keeps the table alive.
            drop(home);
            match outcome {
                InsertResult::Success(old_val) => {
                    #[cfg(test)]
                    pause::at(pause::Point::AfterLanding);
                    // Final. The write landed in a live table under its home
                    // guard, so a resize that starts after it copies it; a
                    // retry here would meet this call's own migrated entry
                    // and report it as present (`insert_if_absent` answering
                    // `Some(own value)` for an insert that happened).
                    if let Some(new_count) = new_count {
                        let current_capacity = table.capacity;
                        let load_factor = new_count as f64 / current_capacity as f64;

                        if load_factor > GROW_THRESHOLD {
                            drop(guard);
                            self.try_resize(current_capacity * 2);
                        }
                    }
                    return old_val;
                }
                InsertResult::Exists(existing_val) => {
                    return Some(existing_val);
                }
                InsertResult::NeedResize(back) => {
                    pending = back;
                    let current_capacity = table.capacity;
                    drop(guard);
                    self.try_resize(current_capacity * 2);
                }
                InsertResult::Retry(back) => {
                    pending = back;
                    resize_spin_hint();
                }
            }
        }
    }

    /// Returns the value corresponding to the key, or inserts the given value if the key is not present.
    ///
    /// When multiple threads call this concurrently for the same key (without
    /// concurrent removes), all callers receive the same value.
    pub fn get_or_insert(&self, key: K, value: V) -> V {
        // Fast path: key already exists - no clone, no insert.
        if let Some(v) = self.get(&key) {
            return v;
        }
        // Slow path: insert_if_absent and use the return value directly.
        // We must NOT do insert-then-get because a concurrent remove between
        // the two operations would cause get to return None.
        let key2 = key.clone();
        match self.insert_impl(key, value.clone(), true) {
            None => {
                // Inserted. The map's current value is the answer: a concurrent
                // `insert` of the key may have replaced this call's value already.
                self.get(&key2).unwrap_or(value)
            }
            Some(existing) => existing, // Key already existed
        }
    }

    /// Insert a key-value pair only if the key does not exist.
    /// Returns `None` if inserted, `Some(existing_value)` if the key already exists.
    pub fn insert_if_absent(&self, key: K, value: V) -> Option<V> {
        self.insert_impl(key, value, true)
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
                resize_spin_hint();
                continue;
            };
            let (offset, entry_ptr) = table.find(home_idx, home.hops(), hash, key, &guard)?;
            // SAFETY: loaded under `guard`, which keeps it from being freed.
            let old_value = unsafe { &*entry_ptr.as_raw() }.value.clone();
            // A store, not a CAS: no other thread writes an occupied slot of a home whose guard
            // this call holds. Release: a reader that acquires the free slot sees everything this
            // call wrote before it.
            table
                .get_bucket(home_idx + offset)
                .slot
                .store(null_entry(), Ordering::Release);
            home.stage_unlinked(offset);

            // Counted down before the home guard is released, as an insert counts up: a clear
            // resets the count while it holds every home guard, so no decrement for an entry it
            // already cleared lands after the reset and eats the count of a later insert.
            // Saturating decrement: prevent count from wrapping to usize::MAX which would
            // trigger catastrophic cascading resizes.
            let shrink_to = if let Ok(prev) = self.count.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |c| c.checked_sub(1),
            ) {
                let new_count = prev - 1;
                let current_capacity = table.capacity;
                let load_factor = new_count as f64 / current_capacity as f64;
                (load_factor < SHRINK_THRESHOLD && current_capacity > MIN_CAPACITY)
                    .then_some(current_capacity / 2)
            } else {
                None
            };
            drop(home);

            // SAFETY: unlinked above under its home guard, so no other thread unlinks or
            // retires it; a reader that loaded it holds a guard that keeps it alive.
            unsafe { retire(entry_ptr.as_raw()) };

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
            resize_spin_hint();
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
            let entry_ptr = bucket.slot.load(Ordering::Acquire, &guard);

            if !entry_ptr.is_null()
                && bucket
                    .slot
                    .compare_exchange(
                        entry_ptr,
                        unsafe { Shared::from_raw(core::ptr::null_mut()) },
                        Ordering::Release,
                        Ordering::Relaxed,
                        &guard,
                    )
                    .is_ok()
            {
                unsafe { retire(entry_ptr.as_raw()) };
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

    /// Take every home bucket's writer guard of `table`, waiting out each
    /// writer that holds one (bounded: a guard holder runs its write to its
    /// end and waits for nothing). Once this returns no writer is inside a
    /// home anywhere in `table` (no insert between its existence scan and its
    /// slot claim, no remove, no move with an entry in two slots), and none
    /// can start there: a writer that fails its guard reloads the table. Only
    /// the thread that set `resizing` calls it (a resize or a clear), so two
    /// such callers never wait on each other.
    fn hold_writers(table: &Table<K, V>) {
        for idx in 0..table.capacity {
            let control = &table.get_bucket(idx).control;
            // Acquire pairs with a holder's release of the guard: every slot
            // and hop bit it wrote is visible to the copy or clear after this.
            while control.fetch_or(GUARD, Ordering::Acquire) & GUARD != 0 {
                #[cfg(test)]
                pause::at(pause::Point::ResizerMetHeldGuard);
                resize_spin_hint();
            }
        }
    }

    /// Give back what [`Self::hold_writers`] took, for a table that stays
    /// live (a clear, or a resize that did not publish its new table).
    fn release_writers(table: &Table<K, V>) {
        for idx in 0..table.capacity {
            // Release: pairs with the next holder's acquire.
            table
                .get_bucket(idx)
                .control
                .fetch_and(!GUARD, Ordering::Release);
        }
    }

    /// One insert attempt under the home guard `home`: update the key's entry, or link a new one
    /// in the first free slot of the neighborhood, displacing entries of other homes to free one
    /// when the neighborhood is full.
    fn try_insert(
        table: &Table<K, V>,
        home: &mut HomeGuard<'_>,
        pending: Pending<K, V>,
        only_if_absent: bool,
        guard: &kovan::Guard,
    ) -> InsertResult<K, V> {
        let hash = pending.key_hash();

        // 1. The key's entry. The scan is stable: only this guard's holder links, replaces,
        // unlinks or moves an entry of the home.
        let existing = table.find(home.idx, home.hops(), hash, pending.key(), guard);
        if let Some((offset, found_ptr)) = existing {
            // SAFETY: loaded under `guard`, which keeps it from being freed.
            let found = unsafe { &*found_ptr.as_raw() };
            if only_if_absent {
                return InsertResult::Exists(found.value.clone());
            }
            let old_value = found.value.clone();
            let entry = Box::into_raw(pending.into_entry());
            // A store, not a CAS: no other thread writes an occupied slot of a home whose guard
            // this call holds. Release: a reader that acquires the slot sees the entry's fields.
            let slot = &table.get_bucket(home.idx + offset).slot;
            slot.store(unsafe { Shared::from_raw(entry) }, Ordering::Release);
            // SAFETY: unlinked above under its home guard, so no other thread unlinks or
            // retires it; a reader that loaded it holds a guard that keeps it alive.
            unsafe { retire(found_ptr.as_raw()) };
            return InsertResult::Success(Some(old_value));
        }

        #[cfg(test)]
        pause::at(pause::Point::InGuardBeforeClaim);

        // 2. The first free slot of the neighborhood (a home is below the capacity, so the
        // neighborhood ends within the padded bucket array).
        let mut entry = pending.into_entry();
        for offset in 0..NEIGHBORHOOD_SIZE {
            if !table.looks_free(home.idx + offset, guard) {
                continue;
            }
            match link(&table.get_bucket(home.idx + offset).slot, entry, guard) {
                Ok(()) => {
                    home.stage_linked(offset);
                    return InsertResult::Success(None);
                }
                Err(back) => entry = back,
            }
        }

        // 3. Displace entries of other homes until a slot of the neighborhood is free.
        match Self::displace(table, home.idx, guard) {
            Freed::Slot(offset) => {
                let slot = &table.get_bucket(home.idx + offset).slot;
                match link(slot, entry, guard) {
                    Ok(()) => {
                        home.stage_linked(offset);
                        InsertResult::Success(None)
                    }
                    Err(back) => InsertResult::Retry(Pending::Built(back)),
                }
            }
            Freed::Contended => InsertResult::Retry(Pending::Built(entry)),
            Freed::Full => InsertResult::NeedResize(Pending::Built(entry)),
        }
    }

    /// Free a slot of the neighborhood of `home` by moving entries: find the first free slot
    /// past the neighborhood within `MAX_PROBE_DISTANCE`, then move it toward the home one step
    /// at a time.
    fn displace(table: &Table<K, V>, home: usize, guard: &kovan::Guard) -> Freed {
        let reach = table.buckets.len().min(home + MAX_PROBE_DISTANCE);
        let mut probe = home + NEIGHBORHOOD_SIZE..reach;
        let Some(mut free) = probe.find(|&idx| table.looks_free(idx, guard)) else {
            return Freed::Full;
        };
        while free >= home + NEIGHBORHOOD_SIZE {
            free = match Self::move_toward(table, free, guard) {
                Ok(nearer) => nearer,
                Err(end) => return end,
            };
        }
        Freed::Slot(free - home)
    }

    /// One displacement step: move into the free slot `free` the entry of the farthest slot
    /// before it that stays within its home's neighborhood there, and return the slot that
    /// frees. `Err(Freed::Contended)` when a step lost a race (a retry can succeed),
    /// `Err(Freed::Full)` when no entry can move.
    fn move_toward(
        table: &Table<K, V>,
        free: usize,
        guard: &kovan::Guard,
    ) -> Result<usize, Freed> {
        let mut contended = false;
        // `free` is past the inserting home's neighborhood, so `nearest` is past the home.
        let nearest = free + 1 - NEIGHBORHOOD_SIZE;
        for from in nearest..free {
            // Acquire: pairs with the release that linked the entry; its hash is read below.
            let entry_ptr = table.get_bucket(from).slot.load(Ordering::Acquire, guard);
            if entry_ptr.is_null() {
                // Freed meanwhile, and nearer the home: nothing to move.
                return Ok(from);
            }
            // SAFETY: loaded under `guard`, which keeps it from being freed.
            let owner = table.bucket_index(unsafe { &*entry_ptr.as_raw() }.hash);
            // An entry stays within its home's neighborhood. This also keeps the inserting
            // call's own home (whose guard it holds) from being an owner: that home's entries
            // sit below `home + NEIGHBORHOOD_SIZE`, which is at most `free`.
            if free >= owner + NEIGHBORHOOD_SIZE {
                continue;
            }
            // Taken without waiting: a guard holder never waits for another guard, so writers
            // cannot deadlock on each other or on a resize holding guards.
            let Some(mut owner_guard) = table.home_guard(owner) else {
                contended = true;
                continue;
            };
            if Self::move_entry(table, &mut owner_guard, from, free, entry_ptr, guard)? {
                return Ok(from);
            }
            contended = true;
        }
        if contended {
            Err(Freed::Contended)
        } else {
            Err(Freed::Full)
        }
    }

    /// Move the entry `entry_ptr` from slot `from` to the free slot `to` under `owner`, its
    /// home's guard: `Ok(true)` when it moved, `Ok(false)` when it left `from` before the guard
    /// was taken, `Err(Freed::Contended)` when another writer took `to` first.
    ///
    /// The entry is linked at `to` before it is unlinked from `from`, so it is always in a slot
    /// its home's hop bits name, and the move stamp advances in between: a lookup that read the
    /// hop bits before the move and finds `from` empty after it rescans (see `get`). It is the
    /// same allocation, not a copy: only a holder of its home guard unlinks or retires an entry,
    /// and this call holds it until the entry is in one slot again. A walk that meets it in both
    /// slots recognizes it (see `HopscotchIter`).
    fn move_entry(
        table: &Table<K, V>,
        owner: &mut HomeGuard<'_>,
        from: usize,
        to: usize,
        entry_ptr: Shared<'_, Entry<K, V>>,
        guard: &kovan::Guard,
    ) -> Result<bool, Freed> {
        let from_slot = &table.get_bucket(from).slot;
        // Under the guard an entry of the home that is still here stays until this call moves
        // it. Relaxed: only the pointer is compared, its fields were acquired by the caller.
        if from_slot.load(Ordering::Relaxed, guard).as_raw() != entry_ptr.as_raw() {
            return Ok(false);
        }
        // 1. Link it at `to` as well. Release: a reader that acquires `to` sees the entry's
        // fields (this thread acquired them from `from`). Relaxed on failure: nothing is read.
        let linked = table.get_bucket(to).slot.compare_exchange(
            null_entry(),
            entry_ptr,
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        );
        if linked.is_err() {
            return Err(Freed::Contended);
        }
        // 2. Name `to` in the hop bits and advance the stamp, published before `from` empties.
        owner.publish_moved_in(to - owner.idx);
        #[cfg(test)]
        pause::at(pause::Point::MoveLinkedTwice);
        // 3. Unlink `from`: a store, as no other thread writes an occupied slot of a held home.
        // Release: a reader that acquires the free slot also sees step 2, the new bit and the
        // advanced stamp.
        from_slot.store(null_entry(), Ordering::Release);
        #[cfg(test)]
        pause::at(pause::Point::MoveUnlinked);
        // 4. Drop `from`'s hop bit, published when `owner` is released.
        owner.stage_unlinked(from - owner.idx);
        Ok(true)
    }

    fn insert_into_new_table(
        &self,
        table: &Table<K, V>,
        hash: u64,
        key: K,
        value: V,
        guard: &kovan::Guard,
    ) -> bool {
        let bucket_idx = table.bucket_index(hash);

        for probe_offset in 0..(table.capacity + NEIGHBORHOOD_SIZE) {
            let probe_idx = bucket_idx + probe_offset;
            if probe_idx >= table.capacity + NEIGHBORHOOD_SIZE {
                break;
            }

            let probe_bucket = table.get_bucket(probe_idx);
            let slot_ptr = probe_bucket.slot.load(Ordering::Relaxed, guard);

            if slot_ptr.is_null() {
                let offset_from_home = probe_idx - bucket_idx;

                if offset_from_home < NEIGHBORHOOD_SIZE {
                    let new_entry = Box::into_raw(Box::new(Entry {
                        retired: RetiredNode::new(),
                        hash,
                        key,
                        value,
                    }));
                    probe_bucket
                        .slot
                        .store(unsafe { Shared::from_raw(new_entry) }, Ordering::Release);

                    let bucket = table.get_bucket(bucket_idx);
                    bucket
                        .control
                        .fetch_or(hop_bit(offset_from_home), Ordering::Relaxed);
                    return true;
                } else {
                    return false;
                }
            }
        }
        false
    }

    fn try_resize(&self, new_capacity: usize) {
        if self
            .resizing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let new_capacity = new_capacity.next_power_of_two().max(MIN_CAPACITY);
        let guard = pin();
        let old_table_ptr = self.table.load(Ordering::Acquire, &guard);
        let old_table = unsafe { &*old_table_ptr.as_raw() };

        if old_table.capacity == new_capacity {
            self.resizing.store(false, Ordering::Release);
            return;
        }

        // Every insert into the old table lands before the copy below reads
        // its slot, or waits and lands in the new table: none is lost, and
        // none has to retry into the new table and meet its own entry there.
        // No move has an entry in two slots for the copy to meet twice either
        // (a move holds its entry's home guard).
        Self::hold_writers(old_table);

        let new_table = Box::into_raw(Box::new(Table::new(new_capacity)));
        let new_table_ref = unsafe { &*new_table };

        let mut success = true;

        for i in 0..(old_table.capacity + NEIGHBORHOOD_SIZE) {
            let bucket = old_table.get_bucket(i);
            let entry_ptr = bucket.slot.load(Ordering::Acquire, &guard);

            if !entry_ptr.is_null() {
                let entry = unsafe { &*entry_ptr.as_raw() };
                if !self.insert_into_new_table(
                    new_table_ref,
                    entry.hash,
                    entry.key.clone(),
                    entry.value.clone(),
                    &guard,
                ) {
                    success = false;
                    break;
                }
            }
        }

        if success {
            match self.table.compare_exchange(
                old_table_ptr,
                unsafe { Shared::from_raw(new_table) },
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => {
                    // The replaced table keeps its writer guards held: a
                    // writer that still holds its pointer fails its guard,
                    // reloads and writes to the new table.
                    unsafe { retire(old_table_ptr.as_raw()) };
                }
                Err(_) => {
                    success = false;
                }
            }
        }

        if !success {
            // The unpublished new table's destructor frees its cloned entries;
            // the old table stays live, so its writers get their guards back.
            unsafe {
                drop(Box::from_raw(new_table));
            }
            Self::release_writers(old_table);
        }

        self.resizing.store(false, Ordering::Release);
    }
    /// Returns an iterator over the map entries.
    ///
    /// The iterator walks the table that is current when it is created, to
    /// its end, even when a resize replaces that table meanwhile (the walk's
    /// position means nothing in another table, whose layout differs). An
    /// entry present from the iterator's creation to its end is yielded
    /// exactly once: a resize neither skips nor repeats it, and a
    /// displacement, which moves an entry to a higher slot of its
    /// neighborhood, links it there before unlinking it from its old slot (so
    /// the walk meets it) and the walk recognizes a key it already met there
    /// (so it is not repeated). An entry inserted, removed or updated
    /// concurrently may or may not be reflected, and no key is yielded twice.
    pub fn iter(&self) -> HopscotchIter<'_, K, V, S> {
        let guard = pin();
        let table = self.table.load(Ordering::Acquire, &guard).as_raw();
        HopscotchIter {
            table,
            bucket_idx: 0,
            recent: [core::ptr::null(); NEIGHBORHOOD_SIZE],
            guard,
            _map: PhantomData,
        }
    }

    /// Returns an iterator over the map keys.
    pub fn keys(&self) -> HopscotchKeys<'_, K, V, S> {
        HopscotchKeys { iter: self.iter() }
    }

    /// Returns an iterator over the map values (clones `V`).
    pub fn values(&self) -> HopscotchValues<'_, K, V, S> {
        HopscotchValues { iter: self.iter() }
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

    /// Get the underlying hasher.
    pub fn hasher(&self) -> &S {
        &self.hasher
    }
}

/// Iterator over HopscotchMap entries ([`HopscotchMap::iter`]).
pub struct HopscotchIter<'a, K: 'static, V: 'static, S> {
    /// The table the walk started on, loaded under `guard`.
    table: *const Table<K, V>,
    bucket_idx: usize,
    /// The entry the walk read in each of its last `NEIGHBORHOOD_SIZE` slots (slot `i` at
    /// `i % NEIGHBORHOOD_SIZE`, null for a free slot), loaded under `guard`: how the walk
    /// recognizes a key it already met.
    recent: [*const Entry<K, V>; NEIGHBORHOOD_SIZE],
    guard: kovan::Guard,
    _map: PhantomData<&'a HopscotchMap<K, V, S>>,
}

impl<K: Eq, V, S> HopscotchIter<'_, K, V, S> {
    /// Whether the walk already met the key of `entry`, read at slot `idx`, in a lower slot of
    /// the key's neighborhood (which starts at `home`). A move carries an entry to a higher slot
    /// of its neighborhood, so the walk can meet it a second time there (or a newer entry of its
    /// key, after an update or a re-insert). Every slot of that neighborhood below `idx` is still
    /// in `recent`: the neighborhood spans `NEIGHBORHOOD_SIZE` slots.
    fn met_before(&self, home: usize, idx: usize, entry: &Entry<K, V>) -> bool {
        let lowest = home.max(idx.saturating_sub(NEIGHBORHOOD_SIZE - 1));
        (lowest..idx).any(|seen_idx| {
            let seen = self.recent[seen_idx % NEIGHBORHOOD_SIZE];
            // SAFETY: null for a free slot, else loaded under `self.guard`, which keeps it from
            // being freed.
            unsafe { seen.as_ref() }.is_some_and(|seen| {
                seen.hash == entry.hash && (core::ptr::eq(seen, entry) || seen.key == entry.key)
            })
        })
    }
}

impl<'a, K, V, S> Iterator for HopscotchIter<'a, K, V, S>
where
    K: Eq + Clone,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: owned by this iterator's `guard`, which was pinned before
        // `table` was loaded and dies with the iterator: a resize that
        // retires the table cannot free it (or its entries) while the guard
        // is held.
        let table = unsafe { &*self.table };

        while self.bucket_idx < table.buckets.len() {
            let idx = self.bucket_idx;
            self.bucket_idx += 1;

            let slot = &table.get_bucket(idx).slot;
            let entry_ptr: *const Entry<K, V> = slot.load(Ordering::Acquire, &self.guard).as_raw();
            self.recent[idx % NEIGHBORHOOD_SIZE] = entry_ptr;
            if entry_ptr.is_null() {
                continue;
            }
            // SAFETY: loaded under `self.guard`, which keeps it from being freed.
            let entry = unsafe { &*entry_ptr };
            if !self.met_before(table.bucket_index(entry.hash), idx, entry) {
                return Some((entry.key.clone(), entry.value.clone()));
            }
        }
        None
    }
}

/// Iterator over HopscotchMap keys.
pub struct HopscotchKeys<'a, K: 'static, V: 'static, S> {
    iter: HopscotchIter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for HopscotchKeys<'a, K, V, S>
where
    K: Eq + Clone,
    V: Clone,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

/// Iterator over HopscotchMap values (clones `V`).
pub struct HopscotchValues<'a, K: 'static, V: 'static, S> {
    iter: HopscotchIter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for HopscotchValues<'a, K, V, S>
where
    K: Eq + Clone,
    V: Clone,
{
    type Item = V;

    #[inline]
    fn next(&mut self) -> Option<V> {
        self.iter.next().map(|(_, v)| v)
    }
}

/// Owned iterator yielding `(K, V)` by value - moves out of the entries, no
/// clone. Each drained slot is nulled so the table destructor stays a no-op.
pub struct HopscotchIntoIter<K: 'static, V: 'static> {
    table: *mut Table<K, V>,
    bucket_idx: usize,
    guard: kovan::Guard,
}

impl<K, V> Iterator for HopscotchIntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        let table = unsafe { &*self.table };
        while self.bucket_idx < table.buckets.len() {
            let bucket = table.get_bucket(self.bucket_idx);
            self.bucket_idx += 1;
            let entry = bucket.slot.load(Ordering::Acquire, &self.guard).as_raw();
            if !entry.is_null() {
                bucket.slot.store(
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::Relaxed,
                );
                let k = unsafe { core::ptr::read(&(*entry).key) };
                let v = unsafe { core::ptr::read(&(*entry).value) };
                unsafe {
                    alloc::alloc::dealloc(
                        entry as *mut u8,
                        core::alloc::Layout::new::<Entry<K, V>>(),
                    );
                }
                return Some((k, v));
            }
        }
        None
    }
}

impl<K, V> Drop for HopscotchIntoIter<K, V> {
    fn drop(&mut self) {
        while self.next().is_some() {}
        // All slots nulled above; Table::drop frees only the bucket array.
        unsafe { drop(Box::from_raw(self.table)) };
    }
}

impl<K, V, S> IntoIterator for HopscotchMap<K, V, S>
where
    K: 'static,
    V: 'static,
{
    type Item = (K, V);
    type IntoIter = HopscotchIntoIter<K, V>;

    fn into_iter(self) -> HopscotchIntoIter<K, V> {
        let mut me = core::mem::ManuallyDrop::new(self);
        let guard = pin();
        let table = me.table.load(Ordering::Relaxed, &guard).as_raw();
        unsafe { core::ptr::drop_in_place(&mut me.hasher) };
        HopscotchIntoIter {
            table,
            bucket_idx: 0,
            guard,
        }
    }
}

impl<K, V, S> core::iter::FromIterator<(K, V)> for HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
    S: BuildHasher + Default,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let map = Self::with_hasher(S::default());
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

impl<'a, K, V, S> IntoIterator for &'a HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    type Item = (K, V);
    type IntoIter = HopscotchIter<'a, K, V, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// The entry an insert links: its key and value until an attempt needs the allocation, then
/// that allocation, carried across the attempts that fail to link it, so a retry neither clones
/// the key and value nor allocates again.
enum Pending<K, V> {
    Parts { hash: u64, key: K, value: V },
    Built(Box<Entry<K, V>>),
}

impl<K, V> Pending<K, V> {
    fn key_hash(&self) -> u64 {
        match self {
            Self::Parts { hash, .. } => *hash,
            Self::Built(entry) => entry.hash,
        }
    }

    fn key(&self) -> &K {
        match self {
            Self::Parts { key, .. } => key,
            Self::Built(entry) => &entry.key,
        }
    }

    /// The entry, allocated the first time an attempt needs it.
    fn into_entry(self) -> Box<Entry<K, V>> {
        match self {
            Self::Parts { hash, key, value } => Box::new(Entry {
                retired: RetiredNode::new(),
                hash,
                key,
                value,
            }),
            Self::Built(entry) => entry,
        }
    }
}

enum InsertResult<K, V> {
    Success(Option<V>),
    Exists(V),
    NeedResize(Pending<K, V>),
    Retry(Pending<K, V>),
}

/// How freeing a slot of an insert's neighborhood by displacement ended.
enum Freed {
    /// The slot this far past the home is free.
    Slot(usize),
    /// A step lost a race (the free slot was taken, an entry changed, or its home guard was
    /// held): the insert retries.
    Contended,
    /// No free slot within reach, or none the entries before it can move into: the table grows.
    Full,
}

#[cfg(feature = "std")]
impl<K, V> Default for HopscotchMap<K, V, FixedState>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
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
#[path = "hopscotch_pause.rs"]
mod pause;

#[cfg(test)]
#[path = "hopscotch_tests.rs"]
mod tests;
