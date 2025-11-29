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
//! use kovan_channel::{unbounded, select};
//! use std::thread;
//!
//! let (s1, r1) = unbounded::<i32>();
//! let (s2, r2) = unbounded::<i32>();
//!
//! thread::spawn(move || {
//!     s1.send(10);
//! });
//!
//! thread::spawn(move || {
//!     s2.send(20);
//! });
//!
//! select! {
//!     v1 = r1 => println!("Received from s1: {}", v1),
//!     v2 = r2 => println!("Received from s2: {}", v2),
//! }
//! ```
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

pub use flavors::after::after;
pub use flavors::never::never;
pub use flavors::tick::tick;
