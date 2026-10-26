#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! Two concurrent hash maps built on kovan's memory reclamation.
//!
//! - [`HashMap`]: separate chaining. [`HashMap::new`] starts with 524,288 buckets (about 4 MiB,
//!   so a large map rarely resizes and a lookup is often one pointer); [`HashMap::with_capacity`]
//!   starts with as many as asked. It grows when its load passes 3/4 and shrinks below 1/4, never
//!   under its initial capacity.
//! - [`HopscotchMap`]: open addressing in hopscotch neighborhoods. It starts at 64 slots and
//!   grows and shrinks with its contents.
//!
//! Reads never wait: a lookup reads the current table under a kovan guard while writers and a
//! resize run. Writers (inserts, removals, the conditional writes, `clear`) take no lock and race
//! each other by compare-and-swap; a writer that meets a resize in flight waits for it to finish,
//! and a hopscotch writer waits for the thread that holds its key's neighborhood. Hashing is
//! FoldHash (`foldhash::fast::FixedState`) unless a hasher is given.
//!
//! # Keys and values
//!
//! Keys are `Hash + Eq + Clone` and values `Clone` (a read hands out a clone), both `Send +
//! 'static`: a replaced or removed entry is freed by kovan once no reader can reach it, on
//! whichever thread frees it, so its destructor may run on another thread.
//!
//! ```compile_fail
//! // `Rc` is not `Send`: a map of them does not compile.
//! let map = kovan_map::HashMap::with_capacity(8);
//! map.insert(1, std::rc::Rc::new(1));
//! ```
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
//! # Safety
//!
//! The implementation uses `unsafe` code internally for performance but provides a safe API.
//! Memory safety is guaranteed through the Kovan memory reclamation system, which ensures
//! nodes are only deallocated when no threads are accessing them.

#![warn(missing_docs)]
#![no_std]

#[cfg(feature = "std")]
extern crate std;

mod hashmap;
mod hopscotch;
mod sync;
#[cfg(test)]
mod test_support;

pub use hashmap::*;
pub use hopscotch::*;
