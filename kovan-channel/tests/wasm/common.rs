//! Shared no-thread `block_on` helper for the wasm test suite.
//!
//! wasm32 targets are single-threaded with no async runtime included by
//! default, and `futures::executor::block_on` (used by the native
//! `tests/async_test.rs`) parks the calling thread while idle --
//! unsupported on `wasm32-unknown-unknown`/`wasm32-wasip1` (see
//! `kovan_channel::signal::Signal`'s docs for why parking panics there).
//! This polls a future in a busy loop instead, with a waker that does
//! nothing.
//!
//! That is only sound because every test in this suite drives its future to
//! readiness on its *first* poll (a message is already sent, or the channel
//! is already disconnected, before `block_on` is called) -- nothing here
//! ever relies on the waker actually firing. A test that needed a genuine
//! pending-then-woken transition would need a second thread to do the
//! waking, which wasm32 doesn't have; that case is out of scope for this
//! suite (see each test file's "Not ported" section).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

fn noop_raw_waker() -> RawWaker {
    fn clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }
    fn no_op(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
    RawWaker::new(std::ptr::null(), &VTABLE)
}

/// Polls `fut` to completion on the current thread. No thread is created or
/// parked; a `Pending` poll is retried immediately. See the module docs for
/// why that's sound for this suite's tests specifically.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut: Pin<Box<F>> = Box::pin(fut);
    loop {
        if let Poll::Ready(val) = fut.as_mut().poll(&mut cx) {
            return val;
        }
    }
}
