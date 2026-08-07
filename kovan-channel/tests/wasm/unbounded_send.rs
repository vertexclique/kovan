//! Single-threaded, wasm-capable coverage of `unbounded::Sender::send` --
//! verified to be a plain CAS retry loop with no parking, so it stays
//! available on `wasm32-*` unlike every blocking API in this crate (see
//! `kovan_channel::signal::Signal`'s docs for why those are gated out).
//!
//! Ported from `tests/unbounded_test.rs`'s single-threaded assertions about
//! `send`'s enqueue/ordering behavior (`test_simple_send_recv`).
//!
//! Not ported (blocking/race-only): `test_threads`,
//! `test_no_aliasing_in_dequeue`, `test_no_double_free_on_recv`, and
//! `test_no_double_free_on_drop_with_pending` in `tests/unbounded_test.rs`
//! -- they exist to prove `send`'s CAS loop and kovan's reclamation are
//! race-safe under real concurrent senders/receivers, which a single wasm
//! thread cannot exercise. The threaded originals remain the real
//! contention coverage on native.

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

use kovan_channel::unbounded;

#[test]
fn send_preserves_fifo_order() {
    let (s, r) = unbounded();
    for i in 0..200 {
        s.send(i);
    }
    for i in 0..200 {
        assert_eq!(r.try_recv(), Some(i));
    }
    assert_eq!(r.try_recv(), None);
}

#[test]
fn cloned_senders_share_one_queue() {
    let (s, r) = unbounded();
    let s2 = s.clone();

    s.send(1);
    s2.send(2);
    s.send(3);

    assert_eq!(r.try_recv(), Some(1));
    assert_eq!(r.try_recv(), Some(2));
    assert_eq!(r.try_recv(), Some(3));
    assert_eq!(r.try_recv(), None);
}

#[test]
fn send_after_receiver_dropped_does_not_panic() {
    let (s, r) = unbounded();
    drop(r);
    // No receiver left to read it, but `send` itself must not panic or block.
    s.send(1);
}

#[test]
fn send_high_volume_no_loss() {
    const N: i32 = 5_000;
    let (s, r) = unbounded();
    for i in 0..N {
        s.send(i);
    }

    let mut received = Vec::with_capacity(N as usize);
    while let Some(v) = r.try_recv() {
        received.push(v);
    }
    let expected: Vec<i32> = (0..N).collect();
    assert_eq!(received, expected);
}
