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
//! whatever other threads do, including threads stopped for good at any
//! point of theirs: no step waits for another thread, and no loop retries
//! because another thread got in first more often than the bound below
//! allows. T is the number of thread IDs handed out (at most 65,536); a
//! step is one atomic instruction, as it is on x86_64 and on aarch64 with
//! LSE (an LL/SC read-modify-write is the hardware's own retry loop; on
//! targets whose 128-bit atomics are emulated with a lock, reclamation is
//! not lock-free at all, see Supported Platforms in README.md).
//!
//! - **Protected loads (`Atomic::load`, `Atom::load`): wait-free.** The
//!   common case is one pointer load plus one epoch compare. If the global
//!   epoch keeps advancing, the load re-publishes at most 15 times; then it
//!   escalates to an *unconditional reservation* (the slot becomes
//!   eligible for every batch) and completes with one final load.
//! - **`pin()`: wait-free.** A nested pin, or an outermost one that finds
//!   the global epoch where its thread's last transition left it, is a
//!   counter update. Otherwise a transition: at most 16 attempts, each one
//!   walk of the thread's own slot list (see below) and one publication,
//!   then a slow path whose loop passes once more only for an epoch advance
//!   made by a thread that had not yet seen its help request: every advance
//!   helps the pending requests first, and a helper completes the request.
//!   A thread's first pin also claims a thread ID: one pass over the
//!   released IDs, no lock.
//! - **`retire()`: wait-free.** O(1) per call; every 64th submits the
//!   batch (one pass over the slots, one insert per eligible slot), every
//!   128th helps the pending slow-path requests (O(T) each) and advances
//!   the epoch.
//! - **`Guard` drop: wait-free** (a counter store; plus one slot
//!   transition when the section escalated to an unconditional
//!   reservation).
//! - **`flush()` and thread exit: wait-free**, a fixed sequence of the
//!   steps above; batches an exiting thread cannot place are parked on its
//!   thread ID and adopted by `flush()`, both without a lock.
//!
//! Every walk of a slot list takes the list with one exchange first, so it
//! is as long as the list was at that instant: one entry per batch
//! submitted while the slot was eligible, since its previous walk. What
//! other threads insert meanwhile starts a new list, and an insert still
//! linking its entry ends the walk there (the inserter walks the rest), so
//! no walk grows while it runs. A thread that stays away from its slot for
//! long owes a walk as long as what was retired meanwhile; `flush()`
//! before idling avoids that (see Quiescence).
//!
//! The destructors of retired values run inside these operations (the
//! batches a walk frees); each kovan call a destructor makes is an
//! operation of its own, with the same bounds.
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
#[cfg(test)]
mod stall;

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
