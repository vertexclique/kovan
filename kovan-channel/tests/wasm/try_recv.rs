//! Single-threaded, wasm-capable coverage of `try_recv`/`is_empty`/
//! `is_disconnected`/`is_full` on both channel flavors. Bounded values are
//! inserted via `send_async` (driven by `common::block_on`) since bounded's
//! only synchronous inserter, `Sender::send`, blocks and is native-only.
//!
//! Ported from `tests/unbounded_test.rs::test_simple_send_recv`,
//! `test_receiver_clone`, and `tests/bounded_test.rs::test_bounded_simple`:
//! all three are already single-threaded and non-blocking end to end (once
//! bounded sends go through `send_async`'s always-resolves-immediately fast
//! path instead of `send`), so they carry over with that one substitution.
//!
//! Not ported (blocking/race-only): `unbounded::Receiver::recv`/
//! `recv_deadline`, `bounded::Sender::send`/`Receiver::recv`/`recv_deadline`
//! (all gated out on `wasm32-*`, see `kovan_channel::signal::Signal`), and
//! every threaded stress/aliasing test in `tests/unbounded_test.rs` and
//! `tests/stress_test.rs` (they exist to prove concurrent-access safety,
//! which a single wasm thread cannot exercise).

#![cfg(target_arch = "wasm32")]

// On wasm32 there is no libtest runner; the wasm-bindgen harness supplies one.
// Aliasing its attribute to `test` lets every case below run unmodified on both
// native (libtest) and wasm32-unknown-unknown (wasm-bindgen-test).
// wasm-bindgen-test only registers its harness export under
// `target_os = "unknown"`. wasm32-wasip1 is `target_os = "wasi"`, where the
// attribute compiles but libtest never sees the function -- the binary would
// report "0 tests" and exit 0. So wasip1 keeps the plain libtest attribute and
// runs under wasmtime; only unknown-unknown swaps in the bindgen harness.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

#[path = "common.rs"]
mod common;

use common::block_on;
use kovan_channel::{bounded, unbounded};

#[test]
fn unbounded_try_recv_roundtrip() {
    let (s, r) = unbounded();
    s.send(1);
    s.send(2);
    s.send(3);

    assert_eq!(r.try_recv(), Some(1));
    assert_eq!(r.try_recv(), Some(2));
    assert_eq!(r.try_recv(), Some(3));
    assert_eq!(r.try_recv(), None);
}

#[test]
fn unbounded_try_recv_empty_returns_none() {
    let (_s, r) = unbounded::<i32>();
    assert_eq!(r.try_recv(), None);
    assert!(r.is_empty());
}

#[test]
fn unbounded_receiver_clone_shares_queue() {
    let (s, r) = unbounded();
    let r2 = r.clone();

    s.send(1);
    s.send(2);

    assert_eq!(r.try_recv(), Some(1));
    assert_eq!(r2.try_recv(), Some(2));
}

#[test]
fn unbounded_is_disconnected_after_last_sender_drops() {
    let (s, r) = unbounded::<i32>();
    assert!(!r.is_disconnected());
    drop(s);
    assert!(r.is_disconnected());
    // Disconnected and empty: try_recv still returns None, not a panic.
    assert_eq!(r.try_recv(), None);
}

#[test]
fn unbounded_disconnect_drains_buffered_messages() {
    let (s, r) = unbounded();
    s.send(1);
    s.send(2);
    drop(s);

    assert!(r.is_disconnected());
    assert_eq!(r.try_recv(), Some(1));
    assert_eq!(r.try_recv(), Some(2));
    assert_eq!(r.try_recv(), None);
}

#[test]
fn bounded_try_recv_roundtrip() {
    let (s, r) = bounded(2);
    block_on(async {
        s.send_async(1).await;
        s.send_async(2).await;
    });

    assert_eq!(r.try_recv(), Some(1));
    assert_eq!(r.try_recv(), Some(2));
    assert_eq!(r.try_recv(), None);
}

/// A capacity of 0 is a rendezvous: a send's value goes in and the send
/// stays pending until a receiver has taken it. (It panicked.)
#[test]
fn bounded_zero_capacity_is_a_rendezvous() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    let (s, r) = bounded::<i32>(0);
    assert_eq!(r.try_recv(), None);
    let mut cx = Context::from_waker(Waker::noop());
    let mut send = pin!(s.send_async(7));
    assert!(
        send.as_mut().poll(&mut cx).is_pending(),
        "a send was done before any receive"
    );
    assert_eq!(r.try_recv(), Some(7));
    assert_eq!(send.as_mut().poll(&mut cx), Poll::Ready(()));
    assert_eq!(r.try_recv(), None);
}

#[test]
fn bounded_is_full_tracks_capacity() {
    let (s, r) = bounded(2);
    assert!(!s.is_full());

    block_on(s.send_async(1));
    assert!(!s.is_full());

    block_on(s.send_async(2));
    assert!(s.is_full());

    assert_eq!(r.try_recv(), Some(1));
    assert!(!s.is_full());
}

#[test]
fn bounded_is_disconnected_after_last_sender_drops() {
    let (s, r) = bounded::<i32>(2);
    assert!(!r.is_disconnected());
    drop(s);
    assert!(r.is_disconnected());
    assert_eq!(r.try_recv(), None);
}
