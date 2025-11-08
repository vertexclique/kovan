//! Kovan: High-performance memory reclamation for lock-free data structures
//!
//! Kovan implements the Crystalline/Hyaline algorithm, providing snapshot-free
//! memory reclamation with zero overhead on read operations.
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
//! ```rust,ignore
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
//! if let Some(value) = ptr.as_ref() {
//!     println!("Value: {}", value);
//! }
//!
//! drop(guard);
//! ```

#![warn(missing_docs)]

extern crate alloc;

mod slot;
mod guard;
mod retired;
mod atomic;
mod reclaim;

pub use guard::{pin, Guard};
pub use retired::RetiredNode;
pub use atomic::{Atomic, Shared};
pub use reclaim::Reclaimable;

// Re-export retire from guard (it's the public API)
pub use guard::retire;

// Re-export for convenience
pub use core::sync::atomic::Ordering;
