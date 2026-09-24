//! Lock-Free Growing/Shrinking Hopscotch Hash Map
//!
//! # Features
//!
//! - **Robust Concurrency**: Uses Copy-on-Move displacement to prevent Use-After-Free.
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
use core::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
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

/// A bucket in the hopscotch hash table
struct Bucket<K, V> {
    /// Bitmap indicating which of the next H slots contain items that hash to this bucket
    hop_info: AtomicU32,
    /// The actual key-value slot at this position
    slot: Atomic<Entry<K, V>>,
    /// Per-home-bucket writer guard (Herlihy-style hopscotch): held only
    /// across `try_insert`'s existence-scan + slot-claim so two same-key
    /// inserters can never both claim a slot. Readers never touch it -
    /// `get`/iteration stay lock-free. Without it, phase-1 (scan hop bits
    /// for the key) and phase-2 (CAS any empty neighborhood slot, THEN set
    /// the hop bit) are not atomic: two `insert_if_absent` callers could
    /// both be told "absent", leaving two live versions of one key with
    /// `get` only ever returning one of them. A resize or a clear takes
    /// every home guard of the table (`hold_writers`) before it touches a
    /// slot, and a resize keeps the guards of the table it replaced.
    write_guard: AtomicBool,
}

/// Releases a bucket's writer guard on drop, so every exit path out of the
/// guarded section (success, retry, resize, early return) unlocks exactly
/// once.
struct WriteGuardRelease<'t, K: 'static, V: 'static> {
    table: &'t Table<K, V>,
    idx: usize,
}

impl<K, V> Drop for WriteGuardRelease<'_, K, V> {
    fn drop(&mut self) {
        self.table
            .get_bucket(self.idx)
            .write_guard
            .store(false, Ordering::Release);
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

impl<K: Clone, V: Clone> Clone for Entry<K, V> {
    fn clone(&self) -> Self {
        Self {
            retired: RetiredNode::new(),
            hash: self.hash,
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }
}

// SAFETY (kovan retirement rule): a retired Entry's destructor may run on
// any thread, and entries (with K and V inside) move between threads -
// hence `K: Send, V: Send` for Send. Lookups DO produce `&K`/`&V` from a
// shared `&Entry` (get() clones V through &V under concurrent readers),
// so Sync additionally requires `K: Sync, V: Sync` - the same bounds the
// map-level Sync impl has always required for sharing the map.
unsafe impl<K: Send, V: Send> Send for Entry<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Entry<K, V> {}

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
                hop_info: AtomicU32::new(0),
                slot: Atomic::null(),
                write_guard: AtomicBool::new(false),
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
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        // Exclusive at this point: either the map itself is being dropped,
        // or kovan reclaimed the table after every guard that could observe
        // it has been released. Slots hold exactly the entries that were
        // never individually unlinked+retired (remove/clear null the slot
        // before retiring), so each entry is freed exactly once. Without
        // this, every resize leaked the old table's entries.
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

        let bucket_idx = table.bucket_index(hash);
        let bucket = table.get_bucket(bucket_idx);

        let hop_info = bucket.hop_info.load(Ordering::Acquire);

        if hop_info == 0 {
            return None;
        }

        for offset in 0..NEIGHBORHOOD_SIZE {
            if hop_info & (1 << offset) != 0 {
                let slot_idx = bucket_idx + offset;
                let slot_bucket = table.get_bucket(slot_idx);
                let entry_ptr = slot_bucket.slot.load(Ordering::Acquire, &guard);

                if !entry_ptr.is_null() {
                    let entry = unsafe { &*entry_ptr.as_raw() };
                    if entry.hash == hash && entry.key.borrow() == key {
                        return Some(entry.value.clone());
                    }
                }
            }
        }

        None
    }

    /// Inserts a key-value pair into the map.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.insert_impl(key, value, false)
    }

    /// Helper for get_or_insert logic.
    fn insert_impl(&self, key: K, value: V, only_if_absent: bool) -> Option<V> {
        let hash = self.hasher.hash_one(&key);

        loop {
            self.wait_for_resize();

            let guard = pin();
            let table_ptr = self.table.load(Ordering::Acquire, &guard);
            let table = unsafe { &*table_ptr.as_raw() };

            if self.resizing.load(Ordering::Acquire) {
                continue;
            }

            // Home-bucket writer guard: serialize same-bucket inserts so the
            // existence scan and the slot claim inside try_insert are one
            // atomic step. Contended -> spin via the outer loop, which keeps
            // re-checking `resizing` and reloads the table. A resize takes
            // every home guard of the table before it copies a slot
            // (`hold_writers`) and keeps the guards of a table it replaced,
            // so holding this guard means the table is live and every slot
            // this call writes is copied by any resize that follows. No
            // lock-order deadlock: a guard holder never waits for anything.
            if self.acquire_write_guard(table, hash).is_none() {
                resize_spin_hint();
                continue;
            }

            // Note: We pass clones to try_insert if we loop here, but try_insert consumes them.
            // Since `insert_impl` owns `key` and `value`, we must clone them for the call
            // because `try_insert` might return `Retry` (looping again).
            //
            // The writer guard is scoped to the try_insert call alone: it
            // must release BEFORE the resize arms below drop the pin and
            // migrate (the release touches this table's bucket, which is
            // only safe while the pin keeps the table alive).
            let (insert_result, new_count) = {
                let _wg = WriteGuardRelease {
                    table,
                    idx: table.bucket_index(hash),
                };
                let insert_result = self.try_insert(
                    table,
                    hash,
                    key.clone(),
                    value.clone(),
                    only_if_absent,
                    &guard,
                );
                // A new entry is counted once, before its home guard is
                // released: concurrent removes cannot decrement the count
                // below the true entry count, and a clear (which resets the
                // count while it holds every home guard) never sees the
                // entry without its count.
                let new_count = match insert_result {
                    InsertResult::Success(None) => {
                        Some(self.count.fetch_add(1, Ordering::Relaxed) + 1)
                    }
                    _ => None,
                };
                (insert_result, new_count)
            };
            match insert_result {
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
                InsertResult::NeedResize => {
                    let current_capacity = table.capacity;
                    drop(guard);
                    self.try_resize(current_capacity * 2);
                    continue;
                }
                InsertResult::Retry => {
                    continue;
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
                // We inserted, but concurrent inserts may have also placed
                // the same key at a different offset (the CAS-then-hop-bit
                // window allows duplicates). Re-get returns the canonical
                // (lowest-offset) entry so every caller agrees on one value.
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
    /// [`remove`](Self::remove) unlinks only the first matching entry.
    /// Insert/remove races can transiently leave more than one entry for
    /// the same key ("versions"); after a plain `remove()` an older version
    /// would become visible again. This method keeps removing until a full
    /// scan finds no match, so the key is guaranteed absent at the
    /// linearization point of the final scan.
    ///
    /// Use `remove()` for single-version removal semantics and
    /// `force_remove()` when the key must be fully evicted.
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
        // First successful removal's value is the linearized result;
        // re-validation retries only evict migrated clones.
        let mut result: Option<V> = None;

        'outer: loop {
            self.wait_for_resize();

            let guard = pin();
            let table_ptr = self.table.load(Ordering::Acquire, &guard);
            let table_raw = table_ptr.as_raw();
            let table = unsafe { &*table_raw };

            if self.resizing.load(Ordering::Acquire) {
                continue;
            }

            let bucket_idx = table.bucket_index(hash);
            let bucket = table.get_bucket(bucket_idx);

            let hop_info = bucket.hop_info.load(Ordering::Acquire);
            if hop_info == 0 {
                return result;
            }

            for offset in 0..NEIGHBORHOOD_SIZE {
                if hop_info & (1 << offset) != 0 {
                    let slot_idx = bucket_idx + offset;
                    let slot_bucket = table.get_bucket(slot_idx);
                    let entry_ptr = slot_bucket.slot.load(Ordering::Acquire, &guard);

                    if !entry_ptr.is_null() {
                        let entry = unsafe { &*entry_ptr.as_raw() };
                        if entry.hash == hash && entry.key.borrow() == key {
                            let old_value = entry.value.clone();

                            match slot_bucket.slot.compare_exchange(
                                entry_ptr,
                                unsafe { Shared::from_raw(core::ptr::null_mut()) },
                                Ordering::Release,
                                Ordering::Relaxed,
                                &guard,
                            ) {
                                Ok(_) => {
                                    let mask = !(1u32 << offset);
                                    bucket.hop_info.fetch_and(mask, Ordering::Release);

                                    unsafe { retire(entry_ptr.as_raw()) };
                                    if result.is_none() {
                                        result = Some(old_value);
                                    }

                                    // Saturating decrement: prevent count from wrapping
                                    // to usize::MAX which would trigger catastrophic
                                    // cascading resizes.
                                    let shrink_to = if let Ok(prev) = self.count.fetch_update(
                                        Ordering::Relaxed,
                                        Ordering::Relaxed,
                                        |c| c.checked_sub(1),
                                    ) {
                                        let new_count = prev - 1;
                                        let current_capacity = table.capacity;
                                        let load_factor =
                                            new_count as f64 / current_capacity as f64;
                                        (load_factor < SHRINK_THRESHOLD
                                            && current_capacity > MIN_CAPACITY)
                                            .then_some(current_capacity / 2)
                                    } else {
                                        None
                                    };

                                    // Re-validate: a concurrent migration may
                                    // have cloned this entry into a new table
                                    // before we unlinked it here - redo the
                                    // removal on the current table so the key
                                    // does not resurrect.
                                    if self.resizing.load(Ordering::SeqCst)
                                        || self.table.load(Ordering::SeqCst, &guard).as_raw()
                                            != table_raw
                                    {
                                        continue 'outer;
                                    }

                                    if let Some(cap) = shrink_to {
                                        drop(guard);
                                        self.try_resize(cap);
                                    }
                                    return result;
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            return result;
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
        // not lands after the clear with its hop bit intact.
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
                let b = table.get_bucket(i);
                b.hop_info.store(0, Ordering::Release);
            }
        }

        self.count.store(0, Ordering::Release);
        Self::release_writers(table);
        self.resizing.store(false, Ordering::Release);
    }

    /// Take every home bucket's writer guard of `table`, waiting out each
    /// insert that holds one (bounded: a guard holder runs `try_insert` to its
    /// end and waits for nothing). Once this returns no insert is between its
    /// existence scan and its slot claim anywhere in `table`, and none can
    /// start there: a writer that fails its guard reloads the table. Only the
    /// thread that set `resizing` calls it (a resize or a clear), so two
    /// holders never wait on each other.
    fn hold_writers(table: &Table<K, V>) {
        for idx in 0..table.capacity {
            // Acquire pairs with a writer's release of the guard: every slot
            // and hop bit it wrote is visible to the copy or clear after this.
            while table
                .get_bucket(idx)
                .write_guard
                .swap(true, Ordering::Acquire)
            {
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
            table
                .get_bucket(idx)
                .write_guard
                .store(false, Ordering::Release);
        }
    }

    /// Try to take the home bucket's writer guard. `Some(())` on success;
    /// `None` when another writer holds it (caller re-loops, staying
    /// responsive to resize).
    fn acquire_write_guard(&self, table: &Table<K, V>, hash: u64) -> Option<()> {
        let bucket = table.get_bucket(table.bucket_index(hash));
        if bucket.write_guard.swap(true, Ordering::Acquire) {
            None
        } else {
            Some(())
        }
    }

    fn try_insert(
        &self,
        table: &Table<K, V>,
        hash: u64,
        key: K,
        value: V,
        only_if_absent: bool,
        guard: &kovan::Guard,
    ) -> InsertResult<V> {
        let bucket_idx = table.bucket_index(hash);
        let bucket = table.get_bucket(bucket_idx);

        // 1. Check if key exists (Update)
        let hop_info = bucket.hop_info.load(Ordering::Acquire);
        for offset in 0..NEIGHBORHOOD_SIZE {
            if hop_info & (1 << offset) != 0 {
                let slot_idx = bucket_idx + offset;
                let slot_bucket = table.get_bucket(slot_idx);
                let entry_ptr = slot_bucket.slot.load(Ordering::Acquire, guard);

                if !entry_ptr.is_null() {
                    let entry = unsafe { &*entry_ptr.as_raw() };
                    if entry.hash == hash && entry.key == key {
                        if only_if_absent {
                            return InsertResult::Exists(entry.value.clone());
                        }

                        let old_value = entry.value.clone();
                        // Clone key and value because if CAS fails, we retry, and we are inside a loop.
                        // We cannot move out of `key` or `value` inside a loop.
                        let new_entry = Box::into_raw(Box::new(Entry {
                            retired: RetiredNode::new(),
                            hash,
                            key: key.clone(),
                            value: value.clone(),
                        }));

                        match slot_bucket.slot.compare_exchange(
                            entry_ptr,
                            unsafe { Shared::from_raw(new_entry) },
                            Ordering::Release,
                            Ordering::Relaxed,
                            guard,
                        ) {
                            Ok(_) => {
                                unsafe { retire(entry_ptr.as_raw()) };
                                return InsertResult::Success(Some(old_value));
                            }
                            Err(_) => {
                                drop(unsafe { Box::from_raw(new_entry) });
                                return InsertResult::Retry;
                            }
                        }
                    }
                }
            }
        }

        #[cfg(test)]
        pause::at(pause::Point::InGuardBeforeClaim);

        // 2. Find empty slot
        for offset in 0..NEIGHBORHOOD_SIZE {
            let slot_idx = bucket_idx + offset;
            if slot_idx >= table.capacity + NEIGHBORHOOD_SIZE {
                return InsertResult::NeedResize;
            }

            let slot_bucket = table.get_bucket(slot_idx);
            let entry_ptr = slot_bucket.slot.load(Ordering::Acquire, guard);

            if entry_ptr.is_null() {
                // Clone key and value. If CAS fails, we continue the loop, so we need the originals
                // for the next iteration.
                let new_entry = Box::into_raw(Box::new(Entry {
                    retired: RetiredNode::new(),
                    hash,
                    key: key.clone(),
                    value: value.clone(),
                }));

                match slot_bucket.slot.compare_exchange(
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    unsafe { Shared::from_raw(new_entry) },
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(_) => {
                        bucket.hop_info.fetch_or(1u32 << offset, Ordering::Release);
                        return InsertResult::Success(None);
                    }
                    Err(_) => {
                        drop(unsafe { Box::from_raw(new_entry) });
                        continue;
                    }
                }
            }
        }

        // 3. Try displacement
        match self.try_find_closer_slot(table, bucket_idx, guard) {
            Some(final_offset) if final_offset < NEIGHBORHOOD_SIZE => {
                let slot_idx = bucket_idx + final_offset;
                let slot_bucket = table.get_bucket(slot_idx);

                let curr = slot_bucket.slot.load(Ordering::Relaxed, guard);
                if !curr.is_null() {
                    return InsertResult::Retry;
                }

                // This is the final attempt in this function. We can move key/value here
                // because previous usages were clones.
                let new_entry = Box::into_raw(Box::new(Entry {
                    retired: RetiredNode::new(),
                    hash,
                    key,
                    value,
                }));

                match slot_bucket.slot.compare_exchange(
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    unsafe { Shared::from_raw(new_entry) },
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(_) => {
                        bucket
                            .hop_info
                            .fetch_or(1u32 << final_offset, Ordering::Release);
                        InsertResult::Success(None)
                    }
                    Err(_) => {
                        drop(unsafe { Box::from_raw(new_entry) });
                        InsertResult::Retry
                    }
                }
            }
            _ => InsertResult::NeedResize,
        }
    }

    fn try_find_closer_slot(
        &self,
        table: &Table<K, V>,
        bucket_idx: usize,
        guard: &kovan::Guard,
    ) -> Option<usize> {
        for probe_offset in NEIGHBORHOOD_SIZE..MAX_PROBE_DISTANCE {
            let probe_idx = bucket_idx + probe_offset;
            if probe_idx >= table.capacity + NEIGHBORHOOD_SIZE {
                return None;
            }

            let probe_bucket = table.get_bucket(probe_idx);
            let entry_ptr = probe_bucket.slot.load(Ordering::Acquire, guard);

            if entry_ptr.is_null() {
                return self.try_move_closer(table, bucket_idx, probe_idx, guard);
            }
        }
        None
    }

    fn try_move_closer(
        &self,
        table: &Table<K, V>,
        target_idx: usize,
        empty_idx: usize,
        guard: &kovan::Guard,
    ) -> Option<usize> {
        let mut current_empty = empty_idx;

        while current_empty > target_idx + NEIGHBORHOOD_SIZE - 1 {
            let mut moved = false;

            for offset in 1..NEIGHBORHOOD_SIZE.min(current_empty - target_idx) {
                let candidate_idx = current_empty - offset;
                let candidate_bucket = table.get_bucket(candidate_idx);
                let entry_ptr = candidate_bucket.slot.load(Ordering::Acquire, guard);

                if !entry_ptr.is_null() {
                    let entry = unsafe { &*entry_ptr.as_raw() };
                    let entry_home = table.bucket_index(entry.hash);

                    if entry_home <= candidate_idx && current_empty < entry_home + NEIGHBORHOOD_SIZE
                    {
                        // Copy-on-Move for safety
                        let new_entry = Box::into_raw(Box::new(entry.clone()));
                        let empty_bucket = table.get_bucket(current_empty);

                        match empty_bucket.slot.compare_exchange(
                            unsafe { Shared::from_raw(core::ptr::null_mut()) },
                            unsafe { Shared::from_raw(new_entry) },
                            Ordering::Release,
                            Ordering::Relaxed,
                            guard,
                        ) {
                            Ok(_) => {
                                match candidate_bucket.slot.compare_exchange(
                                    entry_ptr,
                                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                                    Ordering::Release,
                                    Ordering::Relaxed,
                                    guard,
                                ) {
                                    Ok(_) => {
                                        let old_offset = candidate_idx - entry_home;
                                        let new_offset = current_empty - entry_home;

                                        let home_bucket = table.get_bucket(entry_home);
                                        home_bucket
                                            .hop_info
                                            .fetch_and(!(1u32 << old_offset), Ordering::Release);
                                        home_bucket
                                            .hop_info
                                            .fetch_or(1u32 << new_offset, Ordering::Release);

                                        unsafe { retire(entry_ptr.as_raw()) };
                                        current_empty = candidate_idx;
                                        moved = true;
                                        break;
                                    }
                                    Err(_) => {
                                        // Attempt to revert the insertion of new_entry.
                                        match empty_bucket.slot.compare_exchange(
                                            unsafe { Shared::from_raw(new_entry) },
                                            unsafe { Shared::from_raw(core::ptr::null_mut()) },
                                            Ordering::Release,
                                            Ordering::Relaxed,
                                            guard,
                                        ) {
                                            Ok(_) => {
                                                // Revert succeeded: no other thread touched it.
                                                // Safe to drop immediately.
                                                unsafe { drop(Box::from_raw(new_entry)) };
                                            }
                                            Err(_) => {
                                                // Displacement already happened here...
                                                // Too late, so we just drop it. Another thread found new_entry,
                                                // displaced it, and replaced it with something else.
                                                // This means new_entry is now part of the blocks
                                                // and was already "retired" by the other thread,
                                                // OR it was just moved. In either case, we must
                                                // let the reclamation system handle it to avoid a
                                                // double free.
                                                unsafe { retire(new_entry) };
                                            }
                                        }
                                        continue;
                                    }
                                }
                            }
                            Err(_) => {
                                unsafe { drop(Box::from_raw(new_entry)) };
                                continue;
                            }
                        }
                    }
                }
            }

            if !moved {
                return None;
            }
        }

        if current_empty >= target_idx && current_empty < target_idx + NEIGHBORHOOD_SIZE {
            Some(current_empty - target_idx)
        } else {
            None
        }
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
                        .hop_info
                        .fetch_or(1u32 << offset_from_home, Ordering::Relaxed);
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
        // No displacement moves an entry past the copy's position either
        // (displacement runs under a home guard).
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
    /// entry present from the iterator's creation to its end is yielded, and
    /// a resize neither skips nor repeats it; an entry inserted, removed or
    /// updated concurrently may or may not be reflected. A concurrent insert
    /// that displaces an entry within its neighborhood moves it to a higher
    /// slot, so it is never skipped, but one already yielded can be yielded
    /// again.
    pub fn iter(&self) -> HopscotchIter<'_, K, V, S> {
        let guard = pin();
        let table = self.table.load(Ordering::Acquire, &guard).as_raw();
        HopscotchIter {
            table,
            bucket_idx: 0,
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
    guard: kovan::Guard,
    _map: PhantomData<&'a HopscotchMap<K, V, S>>,
}

impl<'a, K, V, S> Iterator for HopscotchIter<'a, K, V, S>
where
    K: Clone,
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
            let bucket = table.get_bucket(self.bucket_idx);
            self.bucket_idx += 1;

            let entry_ptr = bucket.slot.load(Ordering::Acquire, &self.guard);
            if !entry_ptr.is_null() {
                let entry = unsafe { &*entry_ptr.as_raw() };
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
    K: Clone,
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
    K: Clone,
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

enum InsertResult<V> {
    Success(Option<V>),
    Exists(V),
    NeedResize,
    Retry,
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
