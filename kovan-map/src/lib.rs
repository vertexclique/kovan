//! Lock-free concurrent hash map using Michael's algorithm with kovan memory reclamation
//!
//! This implementation uses a fixed array of buckets, where each bucket is a lock-free
//! linked list. Operations only touch the specific nodes being modified, providing
//! true lock-free performance.

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
use kovan::{Atomic, Shared, pin, retire};

#[cfg(feature = "std")]
use std::collections::hash_map::RandomState;

/// Number of buckets (must be power of 2)
const BUCKET_COUNT: usize = 65536;

/// Node in the lock-free linked list
struct Node<K, V> {
    hash: u64,
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}

/// Lock-free concurrent hash map using Michael's algorithm
///
/// This hash map uses a fixed array of buckets, where each bucket is a lock-free
/// linked list. Operations are wait-free for reads and lock-free for writes.
///
/// # Type Parameters
///
/// - `K`: Key type (must implement `Hash` and `Eq`)
/// - `V`: Value type (must implement `Copy`)
/// - `S`: Hash builder (defaults to `RandomState` when `std` feature is enabled)
pub struct HashMap<K, V, S = RandomState> {
    buckets: Box<[Atomic<Node<K, V>>]>,
    mask: usize,
    hasher: S,
}

#[cfg(feature = "std")]
impl<K, V> HashMap<K, V, RandomState>
where
    K: Hash + Eq + Clone + 'static,
    V: Copy + 'static,
{
    /// Creates a new empty hash map with default hasher
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map: HashMap<i32, i32> = HashMap::new();
    /// ```
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Copy + 'static,
    S: BuildHasher,
{
    /// Creates a new hash map with custom hasher
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hasher = RandomState::new();
    /// let map: HashMap<i32, i32, _> = HashMap::with_hasher(hasher);
    /// ```
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

    /// Get bucket for a hash value
    #[inline(always)]
    fn get_bucket(&self, hash: u64) -> &Atomic<Node<K, V>> {
        &self.buckets[(hash as usize) & self.mask]
    }

    /// Get a value by key (wait-free, never blocks)
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.insert(1, 100);
    /// assert_eq!(map.get(&1), Some(100));
    /// assert_eq!(map.get(&2), None);
    /// ```
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let bucket = self.get_bucket(hash);

        let guard = pin();
        let mut current = bucket.load(Ordering::Acquire, &guard);

        while !current.is_null() {
            unsafe {
                let node = current.deref();
                if node.hash == hash && node.key.borrow() == key {
                    return Some(node.value);
                }
                current = node.next.load(Ordering::Acquire, &guard);
            }
        }
        None
    }

    /// Check if key exists
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.insert(1, 100);
    /// assert!(map.contains_key(&1));
    /// assert!(!map.contains_key(&2));
    /// ```
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Insert a key-value pair (lock-free)
    ///
    /// Returns the previous value if the key already existed.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// assert_eq!(map.insert(1, 100), None);
    /// assert_eq!(map.insert(1, 200), Some(100));
    /// ```
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);
        let bucket = self.get_bucket(hash);

        'outer: loop {
            let guard = pin();

            // First, check if key exists
            let mut prev = bucket;
            let mut current = bucket.load(Ordering::Acquire, &guard);

            while !current.is_null() {
                unsafe {
                    let node = current.deref();

                    if node.hash == hash && node.key == key {
                        // Key exists - replace the node
                        let old_value = node.value;
                        let next = node.next.load(Ordering::Relaxed, &guard);

                        let new_node = Box::into_raw(Box::new(Node {
                            hash,
                            key: key.clone(),
                            value,
                            next: Atomic::new(next.as_raw()),
                        }));

                        match prev.compare_exchange(
                            current,
                            Shared::from_raw(new_node),
                            Ordering::Release,
                            Ordering::Acquire,
                            &guard,
                        ) {
                            Ok(_) => {
                                retire(current.as_raw());
                                return Some(old_value);
                            }
                            Err(_) => {
                                drop(Box::from_raw(new_node));
                                continue 'outer; // Retry from start
                            }
                        }
                    }

                    prev = &node.next;
                    current = node.next.load(Ordering::Acquire, &guard);
                }
            }

            // Key not found - insert at head
            let head = bucket.load(Ordering::Acquire, &guard);
            let new_node = Box::into_raw(Box::new(Node {
                hash,
                key: key.clone(),
                value,
                next: Atomic::new(head.as_raw()),
            }));

            match bucket.compare_exchange(
                head,
                unsafe { Shared::from_raw(new_node) },
                Ordering::Release,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => return None,
                Err(_) => {
                    unsafe {
                        drop(Box::from_raw(new_node));
                    }
                    continue 'outer;
                }
            }
        }
    }

    /// Remove a key-value pair (lock-free)
    ///
    /// Returns the value if the key existed.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.insert(1, 100);
    /// assert_eq!(map.remove(&1), Some(100));
    /// assert_eq!(map.remove(&1), None);
    /// ```
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let bucket = self.get_bucket(hash);

        'outer: loop {
            let guard = pin();
            let mut prev = bucket;
            let mut current = bucket.load(Ordering::Acquire, &guard);

            while !current.is_null() {
                unsafe {
                    let node = current.deref();

                    if node.hash == hash && node.key.borrow() == key {
                        let next = node.next.load(Ordering::Acquire, &guard);
                        let old_value = node.value;

                        match prev.compare_exchange(
                            current,
                            next,
                            Ordering::Release,
                            Ordering::Acquire,
                            &guard,
                        ) {
                            Ok(_) => {
                                retire(current.as_raw());
                                return Some(old_value);
                            }
                            Err(_) => continue 'outer, // Retry from start
                        }
                    }

                    prev = &node.next;
                    current = node.next.load(Ordering::Acquire, &guard);
                }
            }

            // Traversed entire list, key not found
            return None;
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        for bucket in self.buckets.iter() {
            loop {
                let guard = pin();
                let head = bucket.load(Ordering::Acquire, &guard);

                if head.is_null() {
                    break;
                }

                match bucket.compare_exchange(
                    head,
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::Release,
                    Ordering::Acquire,
                    &guard,
                ) {
                    Ok(_) => {
                        // Retire entire chain
                        unsafe {
                            let mut current = head;
                            while !current.is_null() {
                                let node = current.deref();
                                let next = node.next.load(Ordering::Relaxed, &guard);
                                retire(current.as_raw());
                                current = next;
                            }
                        }
                        break;
                    }
                    Err(_) => continue,
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
    K: Hash + Eq + Clone + 'static,
    V: Copy + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: HashMap can be sent between threads if K, V are Send
unsafe impl<K: Send, V: Send, S: Send> Send for HashMap<K, V, S> {}

// SAFETY: HashMap can be shared between threads if K, V are Sync
unsafe impl<K: Sync, V: Sync, S: Sync> Sync for HashMap<K, V, S> {}

impl<K, V, S> Drop for HashMap<K, V, S> {
    fn drop(&mut self) {
        // Clean up all bucket chains
        for bucket in self.buckets.iter() {
            let guard = pin();
            let mut current = bucket.load(Ordering::Acquire, &guard);

            unsafe {
                while !current.is_null() {
                    let node = current.deref();
                    let next = node.next.load(Ordering::Relaxed, &guard);
                    drop(Box::from_raw(current.as_raw() as *mut Node<K, V>));
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
    fn test_remove() {
        let map = HashMap::new();
        map.insert(1, 100);
        map.insert(2, 200);

        assert_eq!(map.remove(&1), Some(100));
        assert_eq!(map.remove(&1), None);
        assert_eq!(map.get(&2), Some(200));
    }

    #[test]
    fn test_multiple_entries() {
        let map = HashMap::new();

        for i in 0..1000 {
            map.insert(i, i * 10);
        }

        for i in 0..1000 {
            assert_eq!(map.get(&i), Some(i * 10));
        }
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

    #[test]
    fn test_concurrent_reads_and_writes() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::new());

        for i in 0..1000 {
            map.insert(i, i);
        }

        let mut handles = alloc::vec::Vec::new();

        for _ in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    let _ = map_clone.get(&i);
                }
            });
            handles.push(handle);
        }

        for thread_id in 0..2 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..500 {
                    let key = 1000 + thread_id * 500 + i;
                    map_clone.insert(key, key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..1000 {
            assert_eq!(map.get(&i), Some(i));
        }
    }

    #[test]
    fn test_concurrent_remove() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::new());

        for i in 0..1000 {
            map.insert(i, i * 10);
        }

        let mut handles = alloc::vec::Vec::new();

        for thread_id in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..250 {
                    let key = thread_id * 250 + i;
                    map_clone.remove(&key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..1000 {
            assert_eq!(map.get(&i), None);
        }
    }
}
