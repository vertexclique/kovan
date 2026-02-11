#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! Kovan: High-performance wait-free memory reclamation for lock-free data structures.
//! Bounded memory usage, predictable latency.
//!
//! Kovan implements the safe and transparent memory reclamation algorithm,
//! providing snapshot-free memory reclamation with zero overhead on read operations.
//!
//! # Key Features
//!
//! - **Zero Read Overhead**: Object loads require only a single atomic read
//! - **Lock-Free Progress**: System-wide progress guaranteed
//! - **Slot-Based Architecture**: Fixed slots, not per-thread structures
//! - **Batch Retirement**: Efficient amortized reclamation cost
//!
//! # Example
//!
//! ```rust
//! use std::sync::atomic::Ordering;
//! use kovan::{pin, retire, Atomic};
//!
//! let atomic = Atomic::new(Box::into_raw(Box::new(42)));
//!
//! // Enter critical section
//! let guard = pin();
//!
//! // Load with zero overhead (single atomic read)
//! let ptr = atomic.load(Ordering::Acquire, &guard);
//!
//! // Access safely within guard lifetime
//! unsafe {
//!     if let Some(value) = ptr.as_ref() {
//!         println!("Value: {}", value);
//!     }
//! }
//!
//! drop(guard);
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

pub use atom::{Atom, AtomGuard, AtomMap, AtomMapGuard, AtomOption};
pub use atomic::{Atomic, Shared};
pub use guard::{Guard, pin};
pub use reclaim::Reclaimable;
pub use retired::RetiredNode;
pub use robust::{BirthEra, current_era};

// Re-export retire from guard (it's the public API)
pub use guard::retire;

// Re-export for convenience
pub use core::sync::atomic::Ordering;
