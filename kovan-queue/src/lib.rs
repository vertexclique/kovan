#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! High-performance queue primitives and Disruptor implementation for Kovan.
//!
//! ## Features
//!
//! - `ArrayQueue`: Bounded MPMC queue.
//! - `SegQueue`: Unbounded MPMC queue (segment based).
//! - `Disruptor`: Disruptor pattern implementation (native targets only).
//!
//! ## Usage
//!
//! ```rust
//! use kovan_queue::array_queue::ArrayQueue;
//!
//! let q: ArrayQueue<u64> = ArrayQueue::new(16);
//! q.push(42).unwrap();
//! assert_eq!(q.pop(), Some(42));
//! ```
//!
//! See [`disruptor`] for the Disruptor example.
//!
//! ## Target support
//!
//! `array_queue`, `seg_queue` and `utils` build and run on every supported
//! target, including 32-bit wasm. `disruptor` is native-only; see its module
//! documentation for why.

pub mod array_queue;

/// Disruptor ring buffer.
///
/// **Native targets only.** `Disruptor::start` spawns one OS thread per event
/// processor, and its wait strategies rely on `thread::yield_now` to hand off
/// between the producer and its consumers. Neither has a working
/// implementation on `wasm32-unknown-unknown` or `wasm32-wasip1`, both of
/// which are single-threaded -- and a Disruptor whose consumers never run
/// cannot make progress by construction, so compiling it there would only
/// produce a deadlock at runtime.
///
/// `array_queue` and `seg_queue` carry no such dependency and remain
/// available everywhere.
///
/// ```rust
/// use kovan_queue::disruptor::{Disruptor, EventHandler};
///
/// struct MyEvent { data: u64 }
/// struct MyHandler;
/// impl EventHandler<MyEvent> for MyHandler {
///     fn on_event(&self, event: &MyEvent, _: u64, _: bool) {
///         println!("Event: {}", event.data);
///     }
/// }
///
/// let mut disruptor = Disruptor::builder(|| MyEvent { data: 0 })
///     .build();
/// disruptor.handle_events_with(MyHandler);
/// let mut producer = disruptor.start();
/// producer.publish(|e| e.data = 42);
/// ```
#[cfg(not(target_arch = "wasm32"))]
pub mod disruptor;

pub mod seg_queue;
pub mod utils;
