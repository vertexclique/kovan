#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! Kovan: High-performance wait-free memory reclamation for lock-free data structures.
//! Bounded memory usage, predictable latency.
//!
//! Kovan implements the asynchronous safe memory reclamation algorithm,
//! providing wait-free memory reclamation with bounded memory usage.
//!
//! # Key Features
//!
//! - **Zero Read Overhead**: Object loads require only a single atomic read
//! - **Wait-Free Progress**: Bounded steps even under contention
//! - **Epoch-Based Architecture**: Per-thread slots with epoch tracking
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
mod ttas;

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
