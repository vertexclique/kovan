//! Lock-free concurrent hash map using kovan memory reclamation
//!
//! This crate provides a lock-free concurrent hash map that uses the kovan
//! memory reclamation scheme for safe concurrent access without mutexes or
//! other blocking primitives.
//!
//! # Features
//!
//! - **Lock-Free**: No mutexes, spinlocks, or blocking operations
//! - **Wait-Free Reads**: Get operations never wait for other threads
//! - **Safe Memory Reclamation**: Uses kovan's zero-overhead memory reclamation
//! - **Concurrent**: Insert, get, and remove can all happen concurrently
//! - **Flexible Hashing**: Support for custom hash builders
//!
//! # Example
//!
//! ```rust
//! use kovan_map::HashMap;
//!
//! let map = HashMap::new();
//!
//! // Insert from multiple threads safely
//! map.insert(42, "hello");
//! map.insert(100, "world");
//!
//! // Read concurrently with zero overhead
//! if let Some(value) = map.get(&42) {
//!     println!("Found: {}", value);
//! }
//!
//! // Remove entries
//! map.remove(&42);
//! ```

#![warn(missing_docs)]
#![no_std]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{pin, retire, Atomic, Shared};
use portable_atomic::AtomicU64;

#[cfg(feature = "std")]
use std::collections::hash_map::RandomState;

/// Default number of buckets in the hash map
const DEFAULT_CAPACITY: usize = 512;

/// Node in a lock-free linked list (per bucket)
struct Node<K, V> {
    key: K,
    value: V,
    hash: u64,
    next: Atomic<Node<K, V>>,
}

/// Lock-free concurrent hash map
///
/// This hash map allows concurrent insertions, lookups, and removals without
/// any locks. It uses kovan's memory reclamation for safe concurrent access.
///
/// # Type Parameters
///
/// - `K`: Key type (must implement `Hash` and `Eq`)
/// - `V`: Value type
/// - `S`: Hash builder (defaults to `RandomState` when `std` feature is enabled)
pub struct HashMap<K, V, S = RandomState> {
    buckets: Vec<Atomic<Node<K, V>>>,
    count: AtomicU64,
    hasher: S,
    mask: usize,
}

#[cfg(feature = "std")]
impl<K, V> HashMap<K, V, RandomState>
where
    K: Hash + Eq + 'static,
    V: 'static,
{
    /// Creates a new empty hash map with default capacity
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map: HashMap<i32, String> = HashMap::new();
    /// ```
    pub fn new() -> Self {
        Self::with_capacity_and_hasher(DEFAULT_CAPACITY, RandomState::new())
    }

    /// Creates a new hash map with the specified capacity
    ///
    /// The capacity is the number of buckets, not the number of elements.
    /// The actual capacity will be rounded up to the next power of two.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map: HashMap<i32, String> = HashMap::with_capacity(128);
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + 'static,
    V: 'static,
    S: BuildHasher,
{
    /// Creates a new hash map with the specified capacity and hasher
    ///
    /// The capacity will be rounded up to the next power of two and at least 64.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let capacity = capacity.next_power_of_two().max(64);
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(Atomic::null());
        }

        Self {
            buckets,
            count: AtomicU64::new(0),
            hasher,
            mask: capacity - 1,
        }
    }

    /// Creates a new hash map with default capacity and the specified hasher
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(DEFAULT_CAPACITY, hasher)
    }

    /// Returns the capacity (number of buckets) of the map
    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    /// Returns the number of elements in the map
    ///
    /// Note: This is an approximate count in concurrent scenarios.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }

    /// Returns true if the map is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Computes the hash of a key using the hash builder
    #[inline(always)]
    fn hash<Q>(&self, key: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        self.hasher.hash_one(key)
    }

    /// Gets the bucket index for a hash using bitwise AND (power of 2 optimization)
    #[inline(always)]
    fn bucket_index(&self, hash: u64) -> usize {
        (hash as usize) & self.mask
    }

    /// Clears the map, removing all entries
    pub fn clear(&self) {
        let guard = pin();
        for bucket in &self.buckets {
            let mut current = bucket.swap(
                unsafe { Shared::from_raw(core::ptr::null_mut()) },
                Ordering::AcqRel,
                &guard
            );

            while !current.is_null() {
                unsafe {
                    let node_ref = current.deref();
                    let next = node_ref.next.load(Ordering::Acquire, &guard);
                    retire(current.as_raw());
                    current = next;
                }
            }
        }
        self.count.store(0, Ordering::Relaxed);
    }

    /// Inserts a key-value pair into the map
    ///
    /// If the key already exists, the old value is replaced and returned.
    /// This operation is lock-free and can be called concurrently.
    pub fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: Clone,
        V: Copy,
    {
        let hash = self.hash(&key);
        let bucket = &self.buckets[self.bucket_index(hash)];

        let guard = pin();

        loop {
            let head = bucket.load(Ordering::Acquire, &guard);
            let mut current = head;
            let mut prev: Option<Shared<Node<K, V>>> = None;

            // Search for existing key
            while !current.is_null() {
                unsafe {
                    let node_ref = current.deref();

                    if node_ref.hash == hash && node_ref.key == key {
                        let next = node_ref.next.load(Ordering::Acquire, &guard);
                        let new_node = Box::into_raw(Box::new(Node {
                            key: key.clone(),
                            value,
                            hash,
                            next: Atomic::new(next.as_raw()),
                        }));

                        let cas_result = if let Some(prev_shared) = prev {
                            let prev_ref = prev_shared.deref();
                            prev_ref.next.compare_exchange(
                                current,
                                Shared::from_raw(new_node),
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        } else {
                            bucket.compare_exchange(
                                current,
                                Shared::from_raw(new_node),
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        };

                        if cas_result.is_ok() {
                            let old_value = node_ref.value;
                            retire(current.as_raw());
                            return Some(old_value);
                        } else {
                            drop(Box::from_raw(new_node));
                            break; // Restart search
                        }
                    }

                    prev = Some(current);
                    current = node_ref.next.load(Ordering::Acquire, &guard);
                }
            }

            // Key not found - insert at head
            let new_node = Box::into_raw(Box::new(Node {
                key: key.clone(),
                value,
                hash,
                next: Atomic::new(head.as_raw()),
            }));

            match bucket.compare_exchange(
                head,
                unsafe { Shared::from_raw(new_node) },
                Ordering::Release,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => {
                    self.count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Err(_) => {
                    unsafe { drop(Box::from_raw(new_node)); }
                }
            }
        }
    }

    /// Gets a copy of the value associated with a key
    ///
    /// This operation is wait-free and has zero overhead.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Copy,
    {
        let hash = self.hash(key);
        let bucket = &self.buckets[self.bucket_index(hash)];

        let guard = pin();
        let mut current = bucket.load(Ordering::Acquire, &guard);

        while !current.is_null() {
            unsafe {
                let node_ref = current.deref();
                if node_ref.hash == hash && node_ref.key.borrow() == key {
                    return Some(node_ref.value);
                }
                current = node_ref.next.load(Ordering::Acquire, &guard);
            }
        }

        None
    }

    /// Gets a copy of the key-value pair
    pub fn get_key_value<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q> + Copy,
        Q: Hash + Eq + ?Sized,
        V: Copy,
    {
        let hash = self.hash(key);
        let bucket = &self.buckets[self.bucket_index(hash)];

        let guard = pin();
        let mut current = bucket.load(Ordering::Acquire, &guard);

        while !current.is_null() {
            unsafe {
                let node_ref = current.deref();
                if node_ref.hash == hash && node_ref.key.borrow() == key {
                    return Some((node_ref.key, node_ref.value));
                }
                current = node_ref.next.load(Ordering::Acquire, &guard);
            }
        }

        None
    }

    /// Returns true if the map contains the specified key
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Copy,
    {
        self.get(key).is_some()
    }

    /// Removes a key from the map, returning the value if it existed
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Copy,
    {
        let hash = self.hash(key);
        let bucket = &self.buckets[self.bucket_index(hash)];

        let guard = pin();

        loop {
            let mut current = bucket.load(Ordering::Acquire, &guard);
            let mut prev: Option<Shared<Node<K, V>>> = None;

            while !current.is_null() {
                unsafe {
                    let node_ref = current.deref();

                    if node_ref.hash == hash && node_ref.key.borrow() == key {
                        let next = node_ref.next.load(Ordering::Acquire, &guard);
                        let value = node_ref.value;

                        let cas_result = if let Some(prev_shared) = prev {
                            let prev_ref = prev_shared.deref();
                            prev_ref.next.compare_exchange(
                                current,
                                next,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        } else {
                            bucket.compare_exchange(
                                current,
                                next,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        };

                        if cas_result.is_ok() {
                            retire(current.as_raw());
                            self.count.fetch_sub(1, Ordering::Relaxed);
                            return Some(value);
                        } else {
                            break; // Restart
                        }
                    }

                    prev = Some(current);
                    current = node_ref.next.load(Ordering::Acquire, &guard);
                }
            }

            // Not found
            return None;
        }
    }

    /// Removes a key from the map, returning the key-value pair if it existed
    pub fn remove_entry<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q> + Copy,
        Q: Hash + Eq + ?Sized,
        V: Copy,
    {
        let hash = self.hash(key);
        let bucket = &self.buckets[self.bucket_index(hash)];

        let guard = pin();

        loop {
            let mut current = bucket.load(Ordering::Acquire, &guard);
            let mut prev: Option<Shared<Node<K, V>>> = None;

            while !current.is_null() {
                unsafe {
                    let node_ref = current.deref();

                    if node_ref.hash == hash && node_ref.key.borrow() == key {
                        let next = node_ref.next.load(Ordering::Acquire, &guard);
                        let key_copy = node_ref.key;
                        let value = node_ref.value;

                        let cas_result = if let Some(prev_shared) = prev {
                            let prev_ref = prev_shared.deref();
                            prev_ref.next.compare_exchange(
                                current,
                                next,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        } else {
                            bucket.compare_exchange(
                                current,
                                next,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        };

                        if cas_result.is_ok() {
                            retire(current.as_raw());
                            self.count.fetch_sub(1, Ordering::Relaxed);
                            return Some((key_copy, value));
                        } else {
                            break;
                        }
                    }

                    prev = Some(current);
                    current = node_ref.next.load(Ordering::Acquire, &guard);
                }
            }

            return None;
        }
    }

    /// Retains only the elements specified by the predicate
    pub fn retain<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V) -> bool,
        V: Copy,
    {
        let guard = pin();

        for bucket in &self.buckets {
            loop {
                let mut current = bucket.load(Ordering::Acquire, &guard);
                let mut prev: Option<Shared<Node<K, V>>> = None;
                let mut modified = false;

                while !current.is_null() {
                    unsafe {
                        let node_ref = current.deref();
                        let should_keep = f(&node_ref.key, &node_ref.value);
                        let next = node_ref.next.load(Ordering::Acquire, &guard);

                        if !should_keep {
                            let cas_result = if let Some(prev_shared) = prev {
                                let prev_ref = prev_shared.deref();
                                prev_ref.next.compare_exchange(
                                    current,
                                    next,
                                    Ordering::Release,
                                    Ordering::Acquire,
                                    &guard,
                                )
                            } else {
                                bucket.compare_exchange(
                                    current,
                                    next,
                                    Ordering::Release,
                                    Ordering::Acquire,
                                    &guard,
                                )
                            };

                            if cas_result.is_ok() {
                                retire(current.as_raw());
                                self.count.fetch_sub(1, Ordering::Relaxed);
                                modified = true;
                                current = next;
                                continue;
                            } else {
                                break;
                            }
                        }

                        prev = Some(current);
                        current = next;
                    }
                }

                if !modified {
                    break;
                }
            }
        }
    }

    /// Returns a reference to the map's hash builder
    pub fn hasher(&self) -> &S {
        &self.hasher
    }
}

#[cfg(feature = "std")]
impl<K, V> Default for HashMap<K, V, RandomState>
where
    K: Hash + Eq + 'static,
    V: 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

// Safety: HashMap can be sent between threads if K, V, and S are Send
unsafe impl<K: Send, V: Send, S: Send> Send for HashMap<K, V, S> {}

// Safety: HashMap can be shared between threads if K, V, and S are Sync
unsafe impl<K: Sync, V: Sync, S: Sync> Sync for HashMap<K, V, S> {}

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
    fn test_remove() {
        let map = HashMap::new();
        map.insert(1, 100);
        map.insert(2, 200);

        assert_eq!(map.remove(&1), Some(100));
        assert_eq!(map.remove(&1), None);
        assert_eq!(map.get(&2), Some(200));
    }

    #[test]
    fn test_len_and_empty() {
        let map = HashMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        map.insert(1, 100);
        assert!(!map.is_empty());
        assert_eq!(map.len(), 1);

        map.remove(&1);
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_multiple_entries() {
        let map = HashMap::new();

        for i in 0..100 {
            map.insert(i, i * 10);
        }

        for i in 0..100 {
            assert_eq!(map.get(&i), Some(i * 10));
        }

        assert_eq!(map.len(), 100);
    }

    #[test]
    fn test_concurrent_inserts() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::new());
        let mut handles = alloc::vec::Vec::new();

        // Spawn 4 threads, each inserting 100 elements
        for thread_id in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = thread_id * 100 + i;
                    map_clone.insert(key, key * 2);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all 400 entries are present
        for thread_id in 0..4 {
            for i in 0..100 {
                let key = thread_id * 100 + i;
                assert_eq!(map.get(&key), Some(key * 2));
            }
        }

        assert_eq!(map.len(), 400);
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::new());

        // Pre-populate some data
        for i in 0..100 {
            map.insert(i, i);
        }

        let mut handles = alloc::vec::Vec::new();

        // Spawn reader threads
        for _ in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let _ = map_clone.get(&i);
                }
            });
            handles.push(handle);
        }

        // Spawn writer threads
        for thread_id in 0..2 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..50 {
                    let key = 100 + thread_id * 50 + i;
                    map_clone.insert(key, key);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify original data still exists
        for i in 0..100 {
            assert_eq!(map.get(&i), Some(i));
        }
    }

    #[test]
    fn test_concurrent_remove() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::new());

        // Pre-populate data
        for i in 0..200 {
            map.insert(i, i * 10);
        }

        let mut handles = alloc::vec::Vec::new();

        // Spawn threads to remove different ranges
        for thread_id in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..50 {
                    let key = thread_id * 50 + i;
                    map_clone.remove(&key);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all entries are removed
        for i in 0..200 {
            assert_eq!(map.get(&i), None);
        }

        assert_eq!(map.len(), 0);
    }
}
