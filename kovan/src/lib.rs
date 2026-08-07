#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! Kovan: High-performance **wait-free** memory reclamation for wait-free
//! data structures. Bounded memory usage, predictable latency.
//!
//! Kovan implements a asynchronous safe memory reclamation (SMR) algorithm.
//!
//! # Progress Guarantees (precise)
//!
//! Every core operation completes in a bounded number of its own steps,
//! regardless of what other threads are doing:
//!
//! - **Protected loads (`Atomic::load`, `Atom::load`) — wait-free.** The
//!   common case is one pointer load plus one epoch compare. If the global
//!   epoch keeps advancing, the load converges within a fixed number of
//!   attempts; on exhaustion it escalates to an *unconditional reservation*
//!   (the slot becomes eligible for every batch) and completes with one
//!   final load. Bound: 16 + 1 iterations, independent of all other threads.
//! - **`pin()` — wait-free.** Bounded fast path; on contention a helping
//!   slow path completes in O(T) steps. Every epoch advance is preceded by
//!   helping, so a pinning thread cannot be starved.
//! - **`retire()` — wait-free** (amortized O(1); the periodic `try_retire`
//!   scan is bounded by the thread count and batch size).
//! - **`Guard` drop — wait-free** (a counter decrement; plus one bounded
//!   slot transition when the section escalated to an unconditional
//!   reservation).
//!
//! `Atom::rcu` and `Atom::compare_and_swap` re-apply user closures on
//! contention as read-copy-update semantics require; the kovan primitives
//! they compose (load, store, retire) are individually wait-free.
//!
//! # Key Properties
//!
//! - **Near-zero read overhead**: the hot read path is a single atomic
//!   pointer load plus an epoch check against a thread-local cache.
//! - **Bounded Memory**: Retired nodes are reclaimed in batches. Batches
//!   that cannot be placed are accumulated or adopted, never dropped. A
//!   stalled reader only delays batches containing nodes born before its
//!   pinned epoch; younger garbage remains reclaimable. (A reader stalled
//!   *inside* an escalated critical section defers all younger batches
//!   until it resumes — escalation windows are bounded by the critical
//!   section that triggered them.)
//! - **`no_std` Compatible**: Uses only `alloc`. No standard library required.
//!
//! # Architecture
//!
//! - **Per-thread epoch slots**: Each thread maintains a slot recording its
//!   current epoch. Slots are protected by 128-bit DCAS (double compare-and-swap).
//! - **Batch retirement**: Retired nodes are accumulated in thread-local
//!   batches (at least 64; batches grow under high thread counts so each
//!   eligible slot can receive a node) and distributed across active slots
//!   via `try_retire`.
//! - **Wait-free helping**: Threads in the slow path publish their state so
//!   other threads can help them complete, ensuring system-wide progress.
//!
//! # Quiescence
//!
//! A thread's reservation slot stays active after its last `Guard` drops;
//! it is refreshed/drained on the next `pin()`, `flush()`, or thread exit.
//! Long-idle threads that once pinned should call [`flush`] before idling
//! to release retained garbage promptly.
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

// The DCAS slot protocol stores (pointer, seqno) pairs in the two 64-bit
// halves of a 128-bit word. On a 32-bit target the pointer half is merely
// zero-extended and the refcount bias is taken from `usize::BITS`, so the
// protocol itself is width-agnostic. What differs is how the 128-bit atomic
// is realised:
//
// - x86_64 / aarch64 / s390x use the `native` WordPair, which mixes sub-word
//   AtomicU64 accesses with a 128-bit compare-exchange over the same 16
//   bytes. That is only coherent with genuine hardware DCAS, which
//   `ASMRState::new` asserts via `AtomicU128::is_lock_free()`.
// - Every other target (wasm32, i686, ...) uses the `fallback` WordPair,
//   which routes *all* access through a single AtomicU128 and so stays
//   coherent even when `portable-atomic` emulates that atomic with a lock.
//
// The trade on those targets is that reclamation is no longer lock-free, and
// therefore no longer wait-free. The data structures remain correct; they
// lose the progress guarantee. See the platform-support table in README.md.

extern crate alloc;

mod atom;
mod atomic;
mod cache_padded;
mod guard;
mod reclaim;
mod retired;
mod slot;
mod ttas;

pub use atom::{Atom, AtomGuard, AtomMap, AtomMapGuard, AtomOption, Removed};
pub use atomic::{Atomic, Shared};
pub use cache_padded::CachePadded;
pub use guard::{Guard, flush, pin};
pub use reclaim::Reclaimable;
pub use retired::RetiredNode;

// Re-export retire from guard (it's the public API)
pub use guard::retire;

// Re-export for convenience
pub use core::sync::atomic::Ordering;
