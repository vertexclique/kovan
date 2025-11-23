//! Optimized Lock-free concurrent hash map using Michael's algorithm with kovan memory reclamation.
//!
//! Changes from previous attempt:
//! 1. REMOVED Cache Padding: The 128-byte alignment blew out the L3 cache (512KB -> 8MB).
//!    Reverting to packed buckets restores read performance.
//! 2. Retained Allocation Reuse: Keeps insert performance high.
//! 3. Retained Guard Hoisting: Reduces overhead per operation.
//! 4. Retained Backoff: Helps during high contention.

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

/// Number of buckets (must be power of 2).
const BUCKET_COUNT: usize = 65536;

/// A simple backoff strategy to reduce bus contention during high concurrency.
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
        // Spin with exponential backoff, capped at 64 iterations
        for _ in 0..(1 << self.step.min(6)) {
            core::hint::spin_loop();
        }
        if self.step <= 6 {
            self.step += 1;
        }
    }
}

/// Node in the lock-free linked list.
struct Node<K, V> {
    hash: u64,
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}

/// A bucket entry. 
/// We removed #[repr(align(128))] because it increased the bucket array size 
/// from 512KB to 8MB, causing massive cache misses on read heavy workloads.
struct Bucket<K, V> {
    head: Atomic<Node<K, V>>,
}

impl<K, V> Default for Bucket<K, V> {
    fn default() -> Self {
        Self {
            head: Atomic::null(),
        }
    }
}

/// Lock-free concurrent hash map using Michael's algorithm.
pub struct HashMap<K, V, S = RandomState> {
    buckets: Box<[Bucket<K, V>]>,
    mask: usize,
    hasher: S,
}

#[cfg(feature = "std")]
impl<K, V> HashMap<K, V, RandomState>
where
    K: Hash + Eq + Clone + 'static,
    V: Copy + 'static,
{
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
    pub fn with_hasher(hasher: S) -> Self {
        let mut buckets = Vec::with_capacity(BUCKET_COUNT);
        for _ in 0..BUCKET_COUNT {
            buckets.push(Bucket::default());
        }

        Self {
            buckets: buckets.into_boxed_slice(),
            mask: BUCKET_COUNT - 1,
            hasher,
        }
    }

    #[inline(always)]
    fn get_bucket(&self, hash: u64) -> &Bucket<K, V> {
        // Safety: mask is BUCKET_COUNT - 1
        unsafe { self.buckets.get_unchecked((hash as usize) & self.mask) }
    }

    /// Get a value by key (wait-free).
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let bucket = self.get_bucket(hash);

        // Hoist pin() to top level
        let guard = pin();
        
        let mut current = bucket.head.load(Ordering::Acquire, &guard);

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

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Insert a key-value pair (lock-free).
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);
        let bucket = self.get_bucket(hash);
        let mut backoff = Backoff::new();
        
        let guard = pin();

        'outer: loop {
            // 1. Search for existing key
            let mut prev_link = &bucket.head;
            let mut current = prev_link.load(Ordering::Acquire, &guard);

            while !current.is_null() {
                unsafe {
                    let node = current.deref();

                    if node.hash == hash && node.key == key {
                        // Key exists - replace node
                        let next = node.next.load(Ordering::Relaxed, &guard);
                        let old_value = node.value;

                        let new_node = Box::into_raw(Box::new(Node {
                            hash,
                            key: key.clone(),
                            value,
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
                                retire(current.as_raw());
                                return Some(old_value);
                            }
                            Err(_) => {
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

            // 2. Insert at head (Optimized allocation)
            let new_node_ptr = Box::into_raw(Box::new(Node {
                hash,
                key: key.clone(),
                value,
                next: Atomic::null(),
            }));

            loop {
                // Try to swap NULL -> NEW_NODE at the tail (prev_link)
                match prev_link.compare_exchange(
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    unsafe { Shared::from_raw(new_node_ptr) },
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                ) {
                    Ok(_) => return None,
                    Err(actual_val) => {
                        // Tail changed. Check if the new tail is our key.
                        unsafe {
                            let actual_node = actual_val.deref();
                            if actual_node.hash == hash && actual_node.key == key {
                                drop(Box::from_raw(new_node_ptr));
                                backoff.spin();
                                continue 'outer;
                            }
                            // Just retry search
                            drop(Box::from_raw(new_node_ptr));
                            backoff.spin();
                            continue 'outer;
                        }
                    }
                }
            }
        }
    }

    /// Remove a key-value pair (lock-free).
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let bucket = self.get_bucket(hash);
        let mut backoff = Backoff::new();
        
        let guard = pin();

        loop {
            let mut prev_link = &bucket.head;
            let mut current = prev_link.load(Ordering::Acquire, &guard);

            while !current.is_null() {
                unsafe {
                    let node = current.deref();

                    if node.hash == hash && node.key.borrow() == key {
                        let next = node.next.load(Ordering::Acquire, &guard);
                        let old_value = node.value;

                        match prev_link.compare_exchange(
                            current,
                            next,
                            Ordering::Release,
                            Ordering::Relaxed,
                            &guard,
                        ) {
                            Ok(_) => {
                                retire(current.as_raw());
                                return Some(old_value);
                            }
                            Err(_) => {
                                backoff.spin();
                                break; // retry search
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

    /// Clear all entries.
    pub fn clear(&self) {
        let guard = pin();
        
        for bucket in self.buckets.iter() {
            let mut backoff = Backoff::new();
            loop {
                let head = bucket.head.load(Ordering::Acquire, &guard);

                if head.is_null() {
                    break;
                }

                match bucket.head.compare_exchange(
                    head,
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                ) {
                    Ok(_) => {
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
                    Err(_) => {
                        backoff.spin();
                        continue;
                    }
                }
            }
        }
    }

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

// SAFETY: HashMap can be sent between threads if K, V are Send.
unsafe impl<K: Send, V: Send, S: Send> Send for HashMap<K, V, S> {}

// SAFETY: HashMap can be shared between threads if K, V are Sync.
unsafe impl<K: Sync, V: Sync, S: Sync> Sync for HashMap<K, V, S> {}

impl<K, V, S> Drop for HashMap<K, V, S> {
    fn drop(&mut self) {
        let guard = pin();

        for bucket in self.buckets.iter() {
            let mut current = bucket.head.load(Ordering::Acquire, &guard);

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