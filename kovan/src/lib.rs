#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! Kovan: High-performance **wait-free** memory reclamation for lock-free data structures.
//! Bounded memory usage, predictable latency.
//!
//! Kovan implements a wait-free safe memory reclamation (SMR) algorithm based on
//! the Crystalline / ASMR design. Unlike epoch-based reclamation (EBR) or
//! hazard pointers, kovan guarantees **bounded worst-case latency** for every
//! operation — no thread can be starved, even under contention.
//!
//! # Key Properties
//!
//! - **Wait-Free Progress**: Every operation completes in a bounded number of
//!   steps, regardless of what other threads are doing. Stalled threads are
//!   helped to completion by concurrent threads.
//! - **Zero Read Overhead**: Object loads require only a single atomic read —
//!   the same cost as a raw `AtomicPtr::load`.
//! - **Bounded Memory**: Retired nodes are reclaimed in bounded batches.
//!   The reclamation system cannot accumulate unbounded garbage, even with
//!   stalled threads.
//! - **`no_std` Compatible**: Uses only `alloc`. No standard library required.
//!
//! # Architecture
//!
//! - **Per-thread epoch slots**: Each thread maintains a slot recording its
//!   current epoch. Slots are protected by 128-bit DCAS (double compare-and-swap).
//! - **Batch retirement**: Retired nodes are accumulated in thread-local batches
//!   of 64 and distributed across active slots via `try_retire`.
//! - **Wait-free helping**: Threads in the slow path publish their state so
//!   other threads can help them complete, ensuring system-wide progress.
//!
//! # Example
//!
//! ```rust
//! use kovan::Atom;
//!
//! // High-level API: safe, zero-overhead reads
//! let config = Atom::new(vec![1, 2, 3]);
//! let guard = config.load();
//! assert_eq!(guard.len(), 3);
//! ```

#![warn(missing_docs)]
#![cfg_attr(feature = "nightly", feature(thread_local))]

extern crate alloc;

mod atom;
mod atomic;
mod guard;
mod reclaim;
mod retired;
mod robust;
mod slot;
mod ttas;

pub use atom::{Atom, AtomGuard, AtomMap, AtomMapGuard, AtomOption, Removed};
pub use atomic::{Atomic, Shared};
pub use guard::{Guard, flush, pin};
pub use reclaim::Reclaimable;
pub use retired::RetiredNode;
pub use robust::{BirthEra, current_era};

// Re-export retire from guard (it's the public API)
pub use guard::retire;

// Re-export for convenience
pub use core::sync::atomic::Ordering;
