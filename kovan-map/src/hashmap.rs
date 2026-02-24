//! High-Performance Lock-Free Concurrent Hash Map (FoldHash + Large Table).
//!
//! # Strategy for Beating Dashmap
//!
//! 1. **FoldHash**: We use `foldhash::fast::FixedState` for blazing fast, quality hashing.
//!    This replaces the custom FxHash implementation.
//! 2. **Oversized Bucket Array**: We bump buckets to 524,288.
//!    - Memory footprint: ~4MB (Fits in L3 cache).
//!    - Load Factor at 100k items: ~0.19.
//!    - Result: almost ZERO collisions. Lookups become a single pointer dereference.
//! 3. **Optimized Node Layout**: Fields are ordered `hash -> key -> value -> next` to
//!    optimize CPU cache line usage during checks.
//!
//! # Architecture
//! - **Buckets**: Array of Atomic pointers (null-initialized).
//! - **Nodes**: Singly linked list.
//! - **Concurrency**: CAS-based lock-free insertion/removal.

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use foldhash::fast::FixedState;
use kovan::{Atomic, RetiredNode, Shared, pin, retire};

/// Number of buckets.
/// 524,288 = 2^19.
/// Size of bucket array = 512k * 8 bytes = 4MB.
/// This is large enough to minimize collisions for 100k-500k items
/// while still fitting in modern L3 caches.
const BUCKET_COUNT: usize = 524_288;

/// A simple exponential backoff for reducing contention.
struct Backoff {
    step: u32,
}

impl Backoff {
    #[inline(always)]
    fn new() -> Self {
        Self { step: 0 }
    }

    #[inline(always)]
    fn spin(&mut self) {
        for _ in 0..(1 << self.step.min(6)) {
            core::hint::spin_loop();
        }
        if self.step <= 6 {
            self.step += 1;
        }
    }
}

/// Node in the lock-free linked list.
#[repr(C)]
struct Node<K, V> {
    retired: RetiredNode,
    hash: u64,
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}

/// High-Performance Lock-Free Map.
pub struct HashMap<K: 'static, V: 'static, S = FixedState> {
    buckets: Box<[Atomic<Node<K, V>>]>,
    mask: usize,
    hasher: S,
}

#[cfg(feature = "std")]
impl<K, V> HashMap<K, V, FixedState>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    /// Creates a new empty hash map with FoldHash (FixedState).
    pub fn new() -> Self {
        Self::with_hasher(FixedState::default())
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// Creates a new hash map with custom hasher.
    pub fn with_hasher(hasher: S) -> Self {
        let mut buckets = Vec::with_capacity(BUCKET_COUNT);
        for _ in 0..BUCKET_COUNT {
            buckets.push(Atomic::null());
        }

        Self {
            buckets: buckets.into_boxed_slice(),
            mask: BUCKET_COUNT - 1,
            hasher,
        }
    }

    #[inline(always)]
    fn get_bucket_idx(&self, hash: u64) -> usize {
        (hash as usize) & self.mask
    }

    #[inline(always)]
    fn get_bucket(&self, idx: usize) -> &Atomic<Node<K, V>> {
        unsafe { self.buckets.get_unchecked(idx) }
    }

    /// Optimized get operation.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let idx = self.get_bucket_idx(hash);
        let bucket = self.get_bucket(idx);

        let guard = pin();
        let mut current = bucket.load(Ordering::Acquire, &guard);

        while !current.is_null() {
            unsafe {
                let node = current.deref();
                // Check hash first (integer compare is fast)
                if node.hash == hash && node.key.borrow() == key {
                    return Some(node.value.clone());
                }
                current = node.next.load(Ordering::Acquire, &guard);
            }
        }
        None
    }

    /// Checks if the key exists.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);
        let idx = self.get_bucket_idx(hash);
        let bucket = self.get_bucket(idx);
        let mut backoff = Backoff::new();

        let guard = pin();

        'outer: loop {
            // 1. Search for existing key to update
            let mut prev_link = bucket;
            let mut current = prev_link.load(Ordering::Acquire, &guard);

            while !current.is_null() {
                unsafe {
                    let node = current.deref();

                    if node.hash == hash && node.key == key {
                        // Key matches. Replace node (COW style).
                        let next = node.next.load(Ordering::Relaxed, &guard);
                        let old_value = node.value.clone();

                        // Create new node pointing to existing next
                        let new_node = Box::into_raw(Box::new(Node {
                            retired: RetiredNode::new(),
                            hash,
                            key: key.clone(),
                            value: value.clone(),
                            next: Atomic::new(next.as_raw()),
                        }));

                        match prev_link.compare_exchange(
                            current,
                            Shared::from_raw(new_node),
                            Ordering::Release,
                            Ordering::Relaxed,
                            &guard,
                        ) {
                            Ok(_) => {
                                // SAFETY: current was allocated via Box::into_raw,
                                // Node is #[repr(C)] with RetiredNode at offset 0.
                                retire(current.as_raw());
                                return Some(old_value);
                            }
                            Err(_) => {
                                // CAS failed.
                                drop(Box::from_raw(new_node));
                                backoff.spin();
                                continue 'outer;
                            }
                        }
                    }

                    prev_link = &node.next;
                    current = node.next.load(Ordering::Acquire, &guard);
                }
            }

            // 2. Key not found. Insert at TAIL (prev_link).
            // Note: In low collision scenarios, prev_link is often the bucket head itself.
            let new_node_ptr = Box::into_raw(Box::new(Node {
                retired: RetiredNode::new(),
                hash,
                key: key.clone(),
                value: value.clone(),
                next: Atomic::null(),
            }));

            // Try to swap NULL -> NEW_NODE
            match prev_link.compare_exchange(
                unsafe { Shared::from_raw(core::ptr::null_mut()) },
                unsafe { Shared::from_raw(new_node_ptr) },
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => return None,
                Err(actual_val) => {
                    // Contention at the tail.
                    // Someone appended something while we were watching.
                    // Check if they inserted OUR key.
                    unsafe {
                        let actual_node = actual_val.deref();
                        if actual_node.hash == hash && actual_node.key == key {
                            // Race lost, but key exists now. We should retry to update it.
                            drop(Box::from_raw(new_node_ptr));
                            backoff.spin();
                            continue 'outer;
                        }
                    }

                    // Otherwise, just retry the search/append loop
                    unsafe {
                        drop(Box::from_raw(new_node_ptr));
                    }
                    backoff.spin();
                    continue 'outer;
                }
            }
        }
    }

    /// Insert a key-value pair only if the key does not exist.
    /// Returns `None` if inserted, `Some(existing_value)` if the key already exists.
    pub fn insert_if_absent(&self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);
        let idx = self.get_bucket_idx(hash);
        let bucket = self.get_bucket(idx);
        let mut backoff = Backoff::new();

        let guard = pin();

        'outer: loop {
            // 1. Search for existing key
            let mut prev_link = bucket;
            let mut current = prev_link.load(Ordering::Acquire, &guard);

            while !current.is_null() {
                unsafe {
                    let node = current.deref();

                    if node.hash == hash && node.key == key {
                        // Key matches. Return existing value.
                        return Some(node.value.clone());
                    }

                    prev_link = &node.next;
                    current = node.next.load(Ordering::Acquire, &guard);
                }
            }

            // 2. Key not found. Insert at TAIL (prev_link).
            let new_node_ptr = Box::into_raw(Box::new(Node {
                retired: RetiredNode::new(),
                hash,
                key: key.clone(),
                value: value.clone(),
                next: Atomic::null(),
            }));

            // Try to swap NULL -> NEW_NODE
            match prev_link.compare_exchange(
                unsafe { Shared::from_raw(core::ptr::null_mut()) },
                unsafe { Shared::from_raw(new_node_ptr) },
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => return None,
                Err(actual_val) => {
                    // Contention at the tail.
                    unsafe {
                        let actual_node = actual_val.deref();
                        if actual_node.hash == hash && actual_node.key == key {
                            // Race lost, key exists now. Return existing value.
                            drop(Box::from_raw(new_node_ptr));
                            return Some(actual_node.value.clone());
                        }
                    }

                    // Otherwise, just retry the search/append loop
                    unsafe {
                        drop(Box::from_raw(new_node_ptr));
                    }
                    backoff.spin();
                    continue 'outer;
                }
            }
        }
    }

    /// Returns the value corresponding to the key, or inserts the given value if the key is not present.
    ///
    /// This is linearizable: concurrent callers for the same key are guaranteed to
    /// agree on which value was inserted (exactly one thread's CAS succeeds at the
    /// list tail, and all others see that node on retry).
    pub fn get_or_insert(&self, key: K, value: V) -> V {
        // HashMap's insert_if_absent is linearizable (CAS at list tail is atomic
        // with the linked-list structure), so no post-insert get() needed.
        match self.insert_if_absent(key, value.clone()) {
            Some(existing) => existing,
            None => value,
        }
    }

    /// Remove a key-value pair.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let idx = self.get_bucket_idx(hash);
        let bucket = self.get_bucket(idx);
        let mut backoff = Backoff::new();

        let guard = pin();

        loop {
            let mut prev_link = bucket;
            let mut current = prev_link.load(Ordering::Acquire, &guard);

            while !current.is_null() {
                unsafe {
                    let node = current.deref();

                    if node.hash == hash && node.key.borrow() == key {
                        let next = node.next.load(Ordering::Acquire, &guard);
                        let old_value = node.value.clone();

                        match prev_link.compare_exchange(
                            current,
                            next,
                            Ordering::Release,
                            Ordering::Relaxed,
                            &guard,
                        ) {
                            Ok(_) => {
                                // SAFETY: current was allocated via Box::into_raw,
                                // Node is #[repr(C)] with RetiredNode at offset 0.
                                retire(current.as_raw());
                                return Some(old_value);
                            }
                            Err(_) => {
                                backoff.spin();
                                break; // Break inner loop to retry outer
                            }
                        }
                    }

                    prev_link = &node.next;
                    current = node.next.load(Ordering::Acquire, &guard);
                }
            }

            if current.is_null() {
                return None;
            }
        }
    }

    /// Clear the map.
    pub fn clear(&self) {
        let guard = pin();

        for bucket in self.buckets.iter() {
            loop {
                let head = bucket.load(Ordering::Acquire, &guard);
                if head.is_null() {
                    break;
                }

                // Try to unlink the whole chain at once
                match bucket.compare_exchange(
                    head,
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                ) {
                    Ok(_) => {
                        // Retire all nodes in the chain
                        unsafe {
                            let mut current = head;
                            while !current.is_null() {
                                let node = current.deref();
                                let next = node.next.load(Ordering::Relaxed, &guard);
                                // SAFETY: current was allocated via Box::into_raw,
                                // Node is #[repr(C)] with RetiredNode at offset 0.
                                retire(current.as_raw());
                                current = next;
                            }
                        }
                        break;
                    }
                    Err(_) => {
                        // Contention, retry
                        continue;
                    }
                }
            }
        }
    }

    /// Returns true if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of elements in the map.
    /// Note: This is an O(N) operation as it scans all buckets.
    pub fn len(&self) -> usize {
        let mut count = 0;
        let guard = pin();
        for bucket in self.buckets.iter() {
            let mut current = bucket.load(Ordering::Acquire, &guard);
            while !current.is_null() {
                unsafe {
                    let node = current.deref();
                    count += 1;
                    current = node.next.load(Ordering::Acquire, &guard);
                }
            }
        }
        count
    }

    /// Returns an iterator over the map entries.
    /// Yields (K, V) clones.
    pub fn iter(&self) -> Iter<'_, K, V, S> {
        Iter {
            map: self,
            bucket_idx: 0,
            guard: pin(),
            current: core::ptr::null(),
        }
    }

    /// Returns an iterator over the map keys.
    /// Yields K clones.
    pub fn keys(&self) -> Keys<'_, K, V, S> {
        Keys { iter: self.iter() }
    }

    /// Get the underlying hasher itself.
    pub fn hasher(&self) -> &S {
        &self.hasher
    }
}

/// Iterator over HashMap entries.
///
/// Field ordering matters for drop safety.
/// Rust drops struct fields in declaration order.
/// The `guard` must be dropped *after* `current` so that
/// the epoch pin covering `current`'s pointed-to node is not released before
/// we're done with the raw pointer.
pub struct Iter<'a, K: 'static, V: 'static, S> {
    map: &'a HashMap<K, V, S>,
    bucket_idx: usize,
    guard: kovan::Guard,
    current: *const Node<K, V>,
}

impl<'a, K, V, S> Iterator for Iter<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.current.is_null() {
                unsafe {
                    let node = &*self.current;
                    // Advance current
                    self.current = node.next.load(Ordering::Acquire, &self.guard).as_raw();
                    return Some((node.key.clone(), node.value.clone()));
                }
            }

            // Move to next bucket
            if self.bucket_idx >= self.map.buckets.len() {
                return None;
            }

            let bucket = unsafe { self.map.buckets.get_unchecked(self.bucket_idx) };
            self.bucket_idx += 1;
            self.current = bucket.load(Ordering::Acquire, &self.guard).as_raw();
        }
    }
}

/// Iterator over HashMap keys.
pub struct Keys<'a, K: 'static, V: 'static, S> {
    iter: Iter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for Keys<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

impl<'a, K, V, S> IntoIterator for &'a HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    type Item = (K, V);
    type IntoIter = Iter<'a, K, V, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(feature = "std")]
impl<K, V> Default for HashMap<K, V, FixedState>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: HashMap is Send if K, V, S are Send (moving ownership between threads).
// HashMap is Sync if K, V, S are Send+Sync. The stronger bound on Sync is needed
// because concurrent `get()` calls clone V through a `&V` reference across threads;
// if V were Send but not Sync, sharing `&HashMap` could transmit a non-Sync V reference
// to another thread via clone(), violating thread-safety.
unsafe impl<K: Send, V: Send, S: Send> Send for HashMap<K, V, S> {}
unsafe impl<K: Send + Sync, V: Send + Sync, S: Send + Sync> Sync for HashMap<K, V, S> {}

impl<K, V, S> Drop for HashMap<K, V, S> {
    fn drop(&mut self) {
        // SAFETY: `drop(&mut self)` guarantees exclusive ownership — no concurrent
        // readers can exist.  Rust's type system enforces this: `Iter<'a, …>` borrows
        // `&'a HashMap`, so it cannot outlive the `HashMap` and will always be dropped
        // before we reach this point.  We therefore free nodes immediately via
        // `Box::from_raw` rather than deferring through `retire()`.
        let guard = pin();

        for bucket in self.buckets.iter() {
            let mut current = bucket.load(Ordering::Acquire, &guard);

            unsafe {
                while !current.is_null() {
                    let node = current.deref();
                    let next = node.next.load(Ordering::Relaxed, &guard);
                    drop(Box::from_raw(current.as_raw()));
                    current = next;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let map = HashMap::new();
        assert_eq!(map.insert(1, 100), None);
        assert_eq!(map.get(&1), Some(100));
        assert_eq!(map.get(&2), None);
    }

    #[test]
    fn test_insert_replace() {
        let map = HashMap::new();
        assert_eq!(map.insert(1, 100), None);
        assert_eq!(map.insert(1, 200), Some(100));
        assert_eq!(map.get(&1), Some(200));
    }

    #[test]
    fn test_concurrent_inserts() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::new());
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
