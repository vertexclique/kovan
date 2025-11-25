//! A high-performance lock-free concurrent hash map built on top of the Kovan memory reclamation system.
//!
//! This crate provides two lock-free hash map implementations:
//!
//! - [`HashMap`]: A chaining-based hash map with fixed capacity and minimal collisions
//! - [`HopscotchMap`]: A growing/shrinking hopscotch hash map with dynamic resizing
//!
//! Both implementations are thread-safe and allow concurrent reads, writes,
//! and deletions without any locks or blocking. They achieve this through:
//!
//! - **Lock-free algorithms**: All operations use compare-and-swap (CAS) based techniques
//! - **Kovan memory reclamation**: Safe memory management in concurrent environments without garbage collection
//! - **FoldHash**: Fast, high-quality hashing for minimal collision rates
//! - **Large bucket arrays**: Pre-allocated oversized tables (524,288 buckets) that fit in L3 cache
//!
//! # Features
//!
//! - `std` (default): Enables standard library support for additional functionality
//! - `no_std` compatible when the `std` feature is disabled
//!
//! # Performance Characteristics
//!
//! The hash map is optimized for high-throughput concurrent workloads:
//!
//! - **Memory footprint**: ~4MB for the bucket array (fits in modern L3 caches)
//! - **Load factor**: ~0.19 at 100k items with 524k buckets
//! - **Collision rate**: Near-zero collisions in typical workloads
//! - **Lookup performance**: Often a single pointer dereference due to minimal collisions
//! - **Cache-optimized**: Node layout ordered for efficient CPU cache line usage
//!
//! # Architecture
//!
//! The implementation uses:
//! - **Bucket array**: Fixed-size array of atomic pointers (null-initialized)
//! - **Collision resolution**: Singly-linked lists for hash collisions
//! - **Node layout**: `hash → key → value → next` for optimal cache performance
//! - **Updates**: Copy-on-write style replacements for existing keys
//!
//! # Example
//!
//! ```rust
//! use kovan_map::HashMap;
//! use std::sync::Arc;
//! use std::thread;
//!
//! // Create a shared hash map
//! let map = Arc::new(HashMap::new());
//!
//! // Spawn multiple threads for concurrent inserts
//! let mut handles = vec![];
//! for thread_id in 0..4 {
//!     let map = Arc::clone(&map);
//!     let handle = thread::spawn(move || {
//!         for i in 0..1000 {
//!             let key = thread_id * 1000 + i;
//!             map.insert(key, key * 2);
//!         }
//!     });
//!     handles.push(handle);
//! }
//!
//! // Wait for all threads to complete
//! for handle in handles {
//!     handle.join().unwrap();
//! }
//!
//! // Read values from any thread
//! assert_eq!(map.get(&0), Some(0));
//! assert_eq!(map.get(&1000), Some(2000));
//!
//! // Remove values
//! assert_eq!(map.remove(&0), Some(0));
//! assert_eq!(map.remove(&0), None);
//! ```
//!
//! # Limitations
//!
//! - **Copy values**: Currently requires `V: Copy` for simplicity and performance
//! - **Clone keys**: Keys must implement `Clone` for update operations
//! - **Static lifetimes**: Both `K` and `V` must be `'static` for safe memory reclamation
//! - **Fixed capacity**: Uses a pre-allocated bucket array (cannot grow dynamically)
//!
//! # Use Cases
//!
//! This hash map is ideal for:
//! - High-throughput concurrent caching
//! - Lock-free data structures in systems programming
//! - Real-time applications where blocking is unacceptable
//! - Scenarios with many readers and occasional writers
//!
//! # Safety
//!
//! The implementation uses `unsafe` code internally for performance but provides a safe API.
//! Memory safety is guaranteed through the Kovan memory reclamation system, which ensures
//! nodes are only deallocated when no threads are accessing them.

#![warn(missing_docs)]
#![no_std]

mod hashmap;
mod hopscotch;

pub use hashmap::*;
pub use hopscotch::HopscotchMap;
