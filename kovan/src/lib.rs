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
//! because another thread got in first. T is the number of thread IDs
//! handed out (at most 65,536); a step is one atomic instruction, as it is
//! on x86_64 and on aarch64 with LSE (an LL/SC read-modify-write is the
//! hardware's own retry loop).
//!
//! - **Protected loads (`Atomic::load`, `Atom::load`): wait-free.** The
//!   common case is one pointer load plus one epoch compare. If the global
//!   epoch moved, the load publishes it and loads again, at most 15 times;
//!   then it escalates to an *unconditional reservation* (the slot becomes
//!   eligible for every batch) and completes with one final load.
//! - **`pin()`: wait-free.** A nested pin is a counter update. An outermost
//!   pin activates the thread's reservation: one exchange of its slot's
//!   list word, and a walk of what that took (at most one node per retire
//!   whose insert into the left slot has not rolled back yet). It publishes
//!   no epoch. A thread's first pin also claims a thread ID: one pass over
//!   the released IDs, no lock.
//! - **`retire()`, `retire_reclaimable()`: wait-free.** O(1) per call;
//!   every 64th submits the batch: one pass over the T slots, one insert
//!   per eligible slot (one exchange, at most one compare-exchange after
//!   it, none retried).
//! - **Allocation (`RetiredNode::new`): wait-free.** One counter update and
//!   one epoch load; every 128th allocation of a thread also advances the
//!   epoch, one fetch-add.
//! - **`Guard` drop: wait-free.** A nested drop is a counter update. The
//!   outermost drop leaves the reservation: one exchange of the slot's
//!   list word to the inactive marker, one walk of the list it took (one
//!   entry per batch submitted while the section ran), then the frees of
//!   the batches that walk released, whose destructors are the thread's
//!   own work.
//! - **`flush()` and thread exit: wait-free**, a fixed sequence of the
//!   steps above: the thread's partial batch is placed (padded with
//!   fillers, one per eligible slot beyond its nodes, so it is always
//!   placed) and the released batches freed; repeated only for what those
//!   frees' destructors retired. A filler is one allocation, the same step
//!   as a node's.
//!
//! Every walk of a slot list takes the list with one exchange first, so it
//! is as long as the list was at that instant: one entry per batch
//! submitted while the slot was eligible, since its previous walk. What
//! other threads insert meanwhile starts a new list, and an insert still
//! linking its entry ends the walk there (the inserter walks the rest), so
//! no walk grows while it runs. A slot is eligible only while its thread
//! holds a guard, so a walk is as long as what was retired during one
//! critical section, never as long as an idle period.
//!
//! The destructors of retired values run inside these operations (the
//! batches a walk frees); each kovan call a destructor makes is an
//! operation of its own, with the same bounds.
//!
//! `Atom::rcu` re-applies its closure on contention, as read-copy-update
//! requires, so it is lock-free: an attempt fails only after another thread's
//! write. `Atom::compare_and_swap` makes one attempt. The kovan primitives
//! they compose (load, store, retire) are individually wait-free.
//!
//! # Key Properties
//!
//! - **Near-zero read overhead**: the hot read path is a single atomic
//!   pointer load plus an epoch check against the thread's published epoch.
//! - **Bounded Memory**: Retired nodes are reclaimed in batches. A batch a
//!   retire cannot place is accumulated, never dropped, and a flush or an
//!   exit places its batch padded, so it is never kept. A reader stalled
//!   inside a critical section only delays batches containing nodes born
//!   before its pinned epoch; younger garbage remains reclaimable. (A
//!   reader stalled *inside* an escalated critical section defers all
//!   younger batches until it resumes: escalation windows are bounded by
//!   the critical section that triggered them.) A thread with no live
//!   guard delays nothing.
//! - **`no_std` Compatible**: Uses only `alloc`. No standard library required.
//!
//! # Architecture
//!
//! - **Per-thread reservation slots**: Each thread with an ID owns a list
//!   word (the batches placed in it; the inactive marker between sections)
//!   and an era word (the epoch its loads are protected from), on a cache
//!   line of their own.
//! - **Batch retirement**: Retired nodes are accumulated in thread-local
//!   batches (at least 64; batches grow under high thread counts so each
//!   eligible slot can receive a node) and distributed across active slots
//!   via `try_retire`, one exchange per slot.
//! - **Epoch clock**: every 128th allocation of a thread advances the
//!   global epoch; a node's birth is the epoch read after that, and a
//!   reservation covers the nodes born up to the epoch it published.
//!
//! # Quiescence
//!
//! A thread's reservation is active exactly while the thread holds a
//! `Guard`: the outermost `pin()` enters its slot, and the drop of that
//! guard leaves it, traversing what retirers put in the slot meanwhile and
//! freeing the batches that released. A thread with no live guard is
//! counted by no batch, so an idle thread, a parked pool thread or a task
//! runtime that pins per turn holds no retired value back, however long it
//! stays away and whatever it loaded before. Nothing has to be flushed
//! before idling; [`flush`] submits what the calling thread itself retired
//! and has not submitted yet (its partial batch).
//!
//! # Recycling node memory (the `recycle` feature)
//!
//! A structure that publishes nodes allocates each one with [`alloc_node`]
//! and lets it go with [`retire_node`], [`drop_node`] or [`free_node`]
//! (every structure of the kovan crates does). Without the `recycle` feature,
//! off by default, these are `Box` allocations, frees and [`retire`]. With
//! it, the memory of a node of up to 1 KiB, aligned to at most 16 bytes, goes
//! back to a per-thread magazine cache of its size class when the node is
//! freed, and the next node of that size takes it from there: a thread that
//! frees about as much as it allocates makes no call into the global
//! allocator, and threads that only allocate or only free trade full and
//! empty magazines through a bounded lock-free depot per class. The
//! magazines hold at most 64 blocks per class per thread and 512 per class in
//! the depot. Under them, blocks are carved from 8 KiB chunks taken from the
//! global allocator one at a time, so even a run of allocations the magazines
//! cannot serve costs one allocator call per chunk, and a chunk goes back to
//! the global allocator once all its blocks are free (a chunk with one
//! long-lived node keeps its 8 KiB). A thread's magazines go back to the depot
//! when it exits, and its chunks are freed by whichever thread gives back each
//! one's last block.
//!
//! Whether the cache is faster depends on the global allocator it replaces.
//! Against glibc's, node churn takes a quarter to a half fewer instructions;
//! against mimalloc, whose fast path is shorter than the cache's, it takes 3 to
//! 11% more. Measure with the program's own allocator before turning it on.
//!
//! Turn it on in the application, never in a library: Cargo enables a feature
//! for every crate of the build at once, so a library that turns it on turns it
//! on for every kovan structure of the programs that depend on it, theirs too.
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

// A thread's reservation is two words, a pointer-sized list word and a
// 64-bit era word, each read and written with the target's own atomics: on
// every target with 64-bit atomics (x86_64, aarch64, s390x, riscv64, i686,
// wasm32, ...) reclamation is lock-free and wait-free alike. See the
// platform-support table in README.md.

extern crate alloc;

mod atom;
mod atomic;
mod cache_padded;
mod guard;
mod node;
mod reclaim;
#[cfg(feature = "recycle")]
mod recycle;
mod retired;
mod slot;
#[cfg(test)]
mod stall;

pub use atom::{Atom, AtomGuard, AtomMap, AtomMapGuard, AtomOption, Removed};
pub use atomic::{Atomic, Shared};
pub use cache_padded::CachePadded;
pub use guard::{Guard, flush, flush_pending, pin};
pub use reclaim::Reclaimable;
pub use retired::RetiredNode;

// Re-export retire from guard (it's the public API)
pub use guard::{retire, retire_reclaimable};
pub use node::{NodeBox, alloc_node, alloc_node_uninit, drop_node, free_node, retire_node};

// Re-export for convenience
pub use core::sync::atomic::Ordering;
