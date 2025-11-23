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

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};
use core::sync::atomic::Ordering;
use kovan::{pin, retire, Atomic, Shared};
use portable_atomic::AtomicU64;

/// Default number of buckets in the hash map
const DEFAULT_CAPACITY: usize = 64;

/// Node in a lock-free linked list (per bucket)
struct Node<K, V> {
    key: K,
    value: V,
    hash: u64,
    next: Atomic<Node<K, V>>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, hash: u64) -> Self {
        Self {
            key,
            value,
            hash,
            next: Atomic::null(),
        }
    }
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
pub struct HashMap<K, V> {
    buckets: Vec<Atomic<Node<K, V>>>,
    count: AtomicU64,
}

impl<K, V> HashMap<K, V>
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
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Creates a new hash map with the specified capacity
    ///
    /// The capacity is the number of buckets, not the number of elements.
    /// More buckets reduce contention but use more memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map: HashMap<i32, String> = HashMap::with_capacity(128);
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(Atomic::null());
        }

        Self {
            buckets,
            count: AtomicU64::new(0),
        }
    }

    /// Returns the number of elements in the map
    ///
    /// Note: This is an approximate count in concurrent scenarios.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }

    /// Returns true if the map is empty
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map: HashMap<i32, String> = HashMap::new();
    /// assert!(map.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Computes the hash of a key
    #[inline]
    fn hash(&self, key: &K) -> u64 {
        // Simple FNV-1a hasher for no_std compatibility
        struct FnvHasher(u64);

        impl Hasher for FnvHasher {
            fn finish(&self) -> u64 {
                self.0
            }

            fn write(&mut self, bytes: &[u8]) {
                for &byte in bytes {
                    self.0 ^= byte as u64;
                    self.0 = self.0.wrapping_mul(0x100000001b3);
                }
            }
        }

        let mut hasher = FnvHasher(0xcbf29ce484222325);
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Gets the bucket index for a hash
    #[inline]
    fn bucket_index(&self, hash: u64) -> usize {
        (hash as usize) % self.buckets.len()
    }

    /// Inserts a key-value pair into the map
    ///
    /// If the key already exists, the old value is replaced and returned.
    /// This operation is lock-free and can be called concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// assert_eq!(map.insert(1, "a"), None);
    /// assert_eq!(map.insert(1, "b"), Some("a"));
    /// ```
    pub fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: Clone,
        V: Clone,
    {
        let hash = self.hash(&key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &self.buckets[bucket_idx];

        loop {
            let guard = pin();
            let head = bucket.load(Ordering::Acquire, &guard);

            // Search for existing key in the list
            let mut current = head;
            let mut prev: Option<Shared<Node<K, V>>> = None;

            while !current.is_null() {
                unsafe {
                    let node_ref = current.deref();

                    // Found matching key - replace value
                    if node_ref.hash == hash && node_ref.key == key {
                        let next = node_ref.next.load(Ordering::Acquire, &guard);

                        // Create new node with updated value
                        let new_node = Box::into_raw(Box::new(Node {
                            key: key.clone(),
                            value: value.clone(),
                            hash,
                            next: Atomic::new(next.as_raw()),
                        }));

                        // Try CAS to replace old node
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
                            // Successfully replaced - extract old value and retire
                            let old_value = core::ptr::read(&node_ref.value as *const V);
                            retire(current.as_raw());
                            return Some(old_value);
                        } else {
                            // CAS failed - cleanup and retry
                            drop(Box::from_raw(new_node));
                            continue;
                        }
                    }

                    prev = Some(current);
                    current = node_ref.next.load(Ordering::Acquire, &guard);
                }
            }

            // Key not found - insert new node at head
            let new_node = Box::into_raw(Box::new(Node::new(key.clone(), value.clone(), hash)));

            unsafe {
                (*new_node).next.store(head, Ordering::Relaxed);
            }

            // Try to CAS the new node as the head
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
                    // CAS failed, retry
                    // Cleanup the node we created
                    unsafe {
                        drop(Box::from_raw(new_node));
                    }
                    continue;
                }
            }
        }
    }

    /// Gets a reference to the value associated with a key
    ///
    /// This operation is wait-free and has zero overhead.
    ///
    /// # Safety
    ///
    /// The returned reference is only valid while the guard is alive.
    /// This method returns a copy of the value to ensure safety.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.get(&1), Some("a"));
    /// assert_eq!(map.get(&2), None);
    /// ```
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Copy,
    {
        let hash = self.hash(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &self.buckets[bucket_idx];

        let guard = pin();
        let mut current = bucket.load(Ordering::Acquire, &guard);

        while !current.is_null() {
            unsafe {
                let node_ref = current.deref();

                if node_ref.hash == hash && &node_ref.key == key {
                    return Some(node_ref.value);
                }

                current = node_ref.next.load(Ordering::Acquire, &guard);
            }
        }

        None
    }

    /// Removes a key from the map, returning the value if it existed
    ///
    /// This operation is lock-free and can be called concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.remove(&1), Some("a"));
    /// assert_eq!(map.remove(&1), None);
    /// ```
    pub fn remove(&self, key: &K) -> Option<V>
    where
        V: Copy,
    {
        let hash = self.hash(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &self.buckets[bucket_idx];

        'outer: loop {
            let guard = pin();
            let mut current = bucket.load(Ordering::Acquire, &guard);
            let mut prev: Option<Shared<Node<K, V>>> = None;

            while !current.is_null() {
                unsafe {
                    let node_ref = current.deref();

                    if node_ref.hash == hash && &node_ref.key == key {
                        let next = node_ref.next.load(Ordering::Acquire, &guard);
                        let value = node_ref.value;

                        let cas_result = if let Some(prev_shared) = prev {
                            let prev_ref = prev_shared.deref();
                            // Update predecessor's next pointer to skip this node
                            prev_ref.next.compare_exchange(
                                current,
                                next,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        } else {
                            // Update bucket head to skip this node
                            bucket.compare_exchange(
                                current,
                                next,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            )
                        };

                        if cas_result.is_ok() {
                            // Successfully unlinked - retire the node
                            retire(current.as_raw());
                            self.count.fetch_sub(1, Ordering::Relaxed);
                            return Some(value);
                        } else {
                            // CAS failed, retry from beginning
                            continue 'outer;
                        }
                    }

                    prev = Some(current);
                    current = node_ref.next.load(Ordering::Acquire, &guard);
                }
            }

            // Key not found
            return None;
        }
    }

    /// Returns true if the map contains the specified key
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.insert(1, "a");
    /// assert!(map.contains_key(&1));
    /// assert!(!map.contains_key(&2));
    /// ```
    pub fn contains_key(&self, key: &K) -> bool
    where
        V: Copy,
    {
        self.get(key).is_some()
    }
}

impl<K, V> Default for HashMap<K, V>
where
    K: Hash + Eq + 'static,
    V: 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

// Safety: HashMap can be sent between threads if K and V are Send
unsafe impl<K: Send, V: Send> Send for HashMap<K, V> {}

// Safety: HashMap can be shared between threads if K and V are Sync
unsafe impl<K: Sync, V: Sync> Sync for HashMap<K, V> {}

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
