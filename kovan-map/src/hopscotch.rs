//! Lock-Free Growing/Shrinking Hopscotch Hash Map
//!
//! # Features
//!
//! - **Robust Concurrency**: Uses Copy-on-Move displacement to prevent Use-After-Free.
//! - **Safe Resizing**: Uses a lightweight resize lock to prevent lost updates during migration.
//! - **Memory reclamation**: Uses Kovan.
//! - **Clone Support**: Supports V: Clone (e.g., Arc<T>) instead of just Copy.

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use foldhash::fast::FixedState;
use kovan::{Atomic, Shared, pin, retire};

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
}

/// An entry in the hash table
#[derive(Clone)]
struct Entry<K, V> {
    hash: u64,
    key: K,
    value: V,
}

/// The hash table structure
struct Table<K, V> {
    buckets: Box<[Bucket<K, V>]>,
    capacity: usize,
    mask: usize,
}

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
            });
        }

        Self {
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

/// A concurrent, lock-free hash map based on Hopscotch Hashing.
pub struct HopscotchMap<K, V, S = FixedState> {
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
    pub fn new() -> Self {
        Self::with_hasher(FixedState::default())
    }

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
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(INITIAL_CAPACITY, hasher)
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let table = Table::new(capacity);
        Self {
            table: Atomic::new(Box::into_raw(Box::new(table))),
            count: AtomicUsize::new(0),
            resizing: AtomicBool::new(false),
            hasher,
        }
    }

    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        let guard = pin();
        let table_ptr = self.table.load(Ordering::Acquire, &guard);
        unsafe { (*table_ptr.as_raw()).capacity }
    }

    #[inline]
    fn wait_for_resize(&self) {
        while self.resizing.load(Ordering::Acquire) {
            core::hint::spin_loop();
        }
    }

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

            // Note: We pass clones to try_insert if we loop here, but try_insert consumes them.
            // Since `insert_impl` owns `key` and `value`, we must clone them for the call 
            // because `try_insert` might return `Retry` (looping again).
            match self.try_insert(table, hash, key.clone(), value.clone(), only_if_absent, &guard) {
                InsertResult::Success(old_val) => {
                    // CRITICAL FIX: If a resize started while we were inserting, our update
                    // might have been missed by the migration.
                    // We must retry to ensure we write to the new table.
                    // We check BOTH the resizing flag (active resize) AND the table pointer (completed resize).
                    if self.resizing.load(Ordering::SeqCst) || self.table.load(Ordering::SeqCst, &guard) != table_ptr {
                        continue;
                    }

                    if old_val.is_none() {
                        let new_count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
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

    pub fn get_or_insert(&self, key: K, value: V) -> V {
        match self.insert_impl(key, value.clone(), true) {
            Some(existing) => existing,
            None => value,
        }
    }

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

                                    retire(entry_ptr.as_raw());

                                    let new_count = self.count.fetch_sub(1, Ordering::Relaxed) - 1;
                                    let current_capacity = table.capacity;
                                    let load_factor = new_count as f64 / current_capacity as f64;

                                    if load_factor < SHRINK_THRESHOLD
                                        && current_capacity > MIN_CAPACITY
                                    {
                                        drop(guard);
                                        self.try_resize(current_capacity / 2);
                                    }

                                    return Some(old_value);
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            return None;
        }
    }

    pub fn clear(&self) {
        while self
            .resizing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }

        let guard = pin();
        let table_ptr = self.table.load(Ordering::Acquire, &guard);
        let table = unsafe { &*table_ptr.as_raw() };

        for i in 0..(table.capacity + NEIGHBORHOOD_SIZE) {
            let bucket = table.get_bucket(i);
            let entry_ptr = bucket.slot.load(Ordering::Acquire, &guard);

            if !entry_ptr.is_null() {
                match bucket.slot.compare_exchange(
                    entry_ptr,
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                ) {
                    Ok(_) => retire(entry_ptr.as_raw()),
                    Err(_) => {}
                }
            }

            if i < table.capacity {
                let b = table.get_bucket(i);
                b.hop_info.store(0, Ordering::Release);
            }
        }

        self.count.store(0, Ordering::Release);
        self.resizing.store(false, Ordering::Release);
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
                            hash, 
                            key: key.clone(), 
                            value: value.clone() 
                        }));

                        match slot_bucket.slot.compare_exchange(
                            entry_ptr,
                            unsafe { Shared::from_raw(new_entry) },
                            Ordering::Release,
                            Ordering::Relaxed,
                            guard,
                        ) {
                            Ok(_) => {
                                retire(entry_ptr.as_raw());
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
                let new_entry = Box::into_raw(Box::new(Entry { hash, key, value }));

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

                                        retire(entry_ptr.as_raw());
                                        current_empty = candidate_idx;
                                        moved = true;
                                        break;
                                    }
                                    Err(_) => {
                                        let _ = empty_bucket.slot.compare_exchange(
                                            unsafe { Shared::from_raw(new_entry) },
                                            unsafe { Shared::from_raw(core::ptr::null_mut()) },
                                            Ordering::Release,
                                            Ordering::Relaxed,
                                            guard,
                                        );
                                        unsafe { drop(Box::from_raw(new_entry)) };
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
                    let new_entry = Box::into_raw(Box::new(Entry { hash, key, value }));
                    probe_bucket
                        .slot
                        .store(unsafe { Shared::from_raw(new_entry) }, Ordering::Release);

                    let bucket = table.get_bucket(bucket_idx);
                    let mut hop = bucket.hop_info.load(Ordering::Relaxed);
                    hop |= 1u32 << offset_from_home;
                    bucket.hop_info.store(hop, Ordering::Relaxed);
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
                    retire(old_table_ptr.as_raw());
                }
                Err(_) => {
                    success = false;
                }
            }
        }

        if !success {
            for i in 0..(new_table_ref.capacity + NEIGHBORHOOD_SIZE) {
                let bucket = new_table_ref.get_bucket(i);
                let entry_ptr = bucket.slot.load(Ordering::Relaxed, &guard);
                if !entry_ptr.is_null() {
                    unsafe {
                        drop(Box::from_raw(entry_ptr.as_raw() as *mut Entry<K, V>));
                    }
                }
            }
            unsafe {
                drop(Box::from_raw(new_table));
            }
        }

        self.resizing.store(false, Ordering::Release);
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
unsafe impl<K: Sync, V: Sync, S: Sync> Sync for HopscotchMap<K, V, S> {}

impl<K, V, S> Drop for HopscotchMap<K, V, S> {
    fn drop(&mut self) {
        let guard = pin();
        let table_ptr = self.table.load(Ordering::Acquire, &guard);
        let table = unsafe { &*table_ptr.as_raw() };

        for i in 0..(table.capacity + NEIGHBORHOOD_SIZE) {
            let bucket = table.get_bucket(i);
            let entry_ptr = bucket.slot.load(Ordering::Acquire, &guard);

            if !entry_ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(entry_ptr.as_raw() as *mut Entry<K, V>));
                }
            }
        }

        unsafe {
            drop(Box::from_raw(table_ptr.as_raw() as *mut Table<K, V>));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let map = HopscotchMap::new();
        assert_eq!(map.insert(1, 100), None);
        assert_eq!(map.get(&1), Some(100));
        assert_eq!(map.get(&2), None);
    }

    #[test]
    fn test_growing() {
        let map = HopscotchMap::with_capacity(32);
        for i in 0..100 {
            map.insert(i, i * 2);
        }
        for i in 0..100 {
            assert_eq!(map.get(&i), Some(i * 2));
        }
    }

    #[test]
    fn test_concurrent() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HopscotchMap::with_capacity(64));
        let mut handles = alloc::vec::Vec::new();

        for thread_id in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    let key = thread_id * 1000 + i;
                    map_clone.insert(key, key * 2);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for thread_id in 0..4 {
            for i in 0..1000 {
                let key = thread_id * 1000 + i;
                assert_eq!(map.get(&key), Some(key * 2));
            }
        }
    }
}
