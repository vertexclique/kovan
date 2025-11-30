#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! High-performance queue primitives and Disruptor implementation for Kovan.
//!
//! ## Features
//!
//! - `ArrayQueue`: Bounded MPMC queue.
//! - `SegQueue`: Unbounded MPMC queue (segment based).
//! - `Disruptor`: Disruptor pattern implementation.
//!
//! ## Usage
//!
//! ```rust
//! use kovan_queue::disruptor::{Disruptor, EventHandler, BusySpinWaitStrategy};
//!
//! struct MyEvent { data: u64 }
//! struct MyHandler;
//! impl EventHandler<MyEvent> for MyHandler {
//!     fn on_event(&self, event: &MyEvent, _: u64, _: bool) {
//!         println!("Event: {}", event.data);
//!     }
//! }
//!
//! let mut disruptor = Disruptor::builder(|| MyEvent { data: 0 })
//!     .build();
//! disruptor.handle_events_with(MyHandler);
//! let mut producer = disruptor.start();
//! producer.publish(|e| e.data = 42);
//! ```

pub mod array_queue;
pub mod disruptor;
pub mod seg_queue;
pub mod utils;
