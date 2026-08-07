#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! Multi-producer multi-consumer channels using Kovan for memory reclamation.
//!
//! This crate provides high-performance, lock-free channel implementations built on top of the
//! [Kovan](https://github.com/vertexclique/kovan) memory reclamation system. It offers both
//! unbounded and bounded channels, along with a powerful `select!` macro for concurrent operations.
//!
//! # Key Features
//!
//! - **Multi-producer Multi-consumer (MPMC)**: All channels support multiple concurrent senders and receivers.
//! - **Lock-Free Operations**: Core operations are lock-free, ensuring system-wide progress.
//! - **Blocking Support**: Channels support blocking `send` (when full) and `recv` (when empty) operations.
//! - **Select Macro**: A `select!` macro for waiting on multiple channel operations, including a `default` case.
//! - **Special Channels**: Includes `after`, `tick`, and `never` channels for timing and control flow.
//! - **Kovan Integration**: Uses Kovan's safe memory reclamation to manage channel nodes without garbage collection.
//!
//! # Channel Flavors
//!
//! - [`unbounded()`]: A channel with infinite capacity. It never blocks on send, but can block on receive.
//! - [`bounded()`]: A channel with fixed capacity. It blocks on send when full and on receive when empty.
//!
//! # Example
//!
//! ```rust
//! use kovan_channel::unbounded;
//!
//! let (s, r) = unbounded::<i32>();
//! s.send(10);
//! assert_eq!(r.try_recv(), Some(10));
//! ```
//!
//! # Platform support
//!
//! Blocking APIs (`bounded::Sender::send`, `recv`/`recv_deadline` on both
//! flavors, `select!` without a `default` arm, `after`, `tick`) are
//! native-only and gated out on `wasm32-*` targets, where parking a thread
//! is unsupported (`wasm32-unknown-unknown`/`wasm32-wasip1` panic on
//! `thread::park`/`thread::sleep`/`Instant::now`/`SystemTime::now`, and both
//! are single-threaded so a park could never be woken anyway). See
//! [`signal::Signal`] for a blocking-`select!` example. wasm builds keep the
//! non-blocking surface: `try_recv`, `send_async`/`recv_async`,
//! `unbounded::Sender::send`, `select!` with a `default` arm, and `never`.
//!
//! # Safety
//!
//! This crate uses `unsafe` code internally for performance and to interface with the Kovan memory
//! reclamation system. However, it exposes a safe API. Memory safety is guaranteed by Kovan's
//! epoch-based reclamation, ensuring that nodes are only freed when no threads are accessing them.

#![warn(missing_docs)]
/// Channel flavors (unbounded, bounded, special).
pub mod flavors;
/// Select macro implementation.
pub mod select;
/// Signal mechanism for thread synchronization.
pub mod signal;
/// Lock-free registration queues for parked senders/receivers.
mod waitlist;

#[cfg(not(target_arch = "wasm32"))]
pub use flavors::RecvDeadline;
pub use flavors::bounded;
pub use flavors::unbounded;

/// Creates a channel of unbounded capacity.
///
/// This channel has a growable buffer that can hold any number of messages.
pub fn unbounded<T: 'static>() -> (unbounded::Sender<T>, unbounded::Receiver<T>) {
    unbounded::channel()
}

/// Creates a channel of bounded capacity.
///
/// This channel has a buffer of fixed capacity.
pub fn bounded<T: 'static>(cap: usize) -> (bounded::Sender<T>, bounded::Receiver<T>) {
    bounded::channel(cap)
}

#[cfg(not(target_arch = "wasm32"))]
pub use flavors::after::after;
pub use flavors::never::never;
#[cfg(not(target_arch = "wasm32"))]
pub use flavors::tick::tick;
