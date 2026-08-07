//! Single-threaded, wasm-capable coverage of `send_async`/`recv_async` on
//! both channel flavors, using `common::block_on` (see that module's docs
//! for why a busy-poll executor is sound here: every future in this file
//! resolves on its first poll).
//!
//! Ported from `tests/async_test.rs`'s already-synchronous bodies:
//! `test_unbounded_async`'s first two awaits, `test_bounded_async`'s first
//! `send_async`, `bounded_recv_async_drains_buffered_after_disconnect`
//! (already thread-free in the original), and the disconnect-resolves-None
//! shape of `unbounded_recv_async_disconnect_resolves_none`/
//! `bounded_recv_async_disconnect_resolves_none` (adapted to drop the
//! sender before awaiting instead of from a second thread after a sleep,
//! since there's no second thread here to race).
//!
//! Not ported (blocking/race-only): every case in `tests/async_test.rs`
//! that spawns a thread to send/disconnect *while* the future is already
//! parked waiting on a real wakeup (`test_mixed_async_blocking`,
//! `bounded_recv_async_roundtrip_cross_thread`, and the sleep-then-send
//! halves of `test_unbounded_async`/`test_bounded_async`) -- those need a
//! second thread to ever deliver the wakeup that resolves the `Pending`
//! poll, which `wasm32-*`'s single thread cannot provide. This file's
//! `block_on` would spin forever on a genuinely `Pending` future.

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
fn unbounded_recv_async_fast_path() {
    block_on(async {
        let (s, r) = unbounded();
        s.send(1);
        s.send(2);

        assert_eq!(r.recv_async().await, Some(1));
        assert_eq!(r.recv_async().await, Some(2));
    });
}

#[test]
fn unbounded_recv_async_disconnect_resolves_none() {
    block_on(async {
        let (s, r) = unbounded::<i32>();
        drop(s);
        assert_eq!(r.recv_async().await, None);
    });
}

#[test]
fn bounded_send_async_recv_async_fast_path() {
    block_on(async {
        let (s, r) = bounded(2);
        s.send_async(1).await;
        s.send_async(2).await;

        assert_eq!(r.recv_async().await, Some(1));
        assert_eq!(r.recv_async().await, Some(2));
    });
}

#[test]
fn bounded_recv_async_disconnect_resolves_none() {
    block_on(async {
        let (s, r) = bounded::<i32>(4);
        drop(s);
        assert_eq!(r.recv_async().await, None);
    });
}

#[test]
fn bounded_recv_async_drains_buffered_after_disconnect() {
    block_on(async {
        let (s, r) = bounded::<i32>(4);

        s.send_async(1).await;
        s.send_async(2).await;
        s.send_async(3).await;
        drop(s);

        assert_eq!(r.recv_async().await, Some(1));
        assert_eq!(r.recv_async().await, Some(2));
        assert_eq!(r.recv_async().await, Some(3));
        assert_eq!(r.recv_async().await, None);
    });
}
