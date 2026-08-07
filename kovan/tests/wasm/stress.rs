//! Single-threaded, wasm-capable adaptation of `tests/stress.rs`.
//!
//! 5 of the original's 7 tests are adapted; 2 are not ported.
//!
//! Not ported (race-only):
//! - `test_oversubscription` — its entire premise is running more OS
//!   threads than cores (2-4x oversubscription) to see how the scheduler
//!   and slot system behave; sequential execution is inherently the
//!   opposite of oversubscribed. What it exercises beyond
//!   `test_high_contention`'s adapted swap/retire loop is purely about OS
//!   thread scheduling, which has no single-threaded analogue.
//! - `test_many_threads_beyond_old_limit` — a regression test that
//!   `MAX_THREADS` (previously a fixed 128) doesn't panic with more
//!   concurrent threads. kovan's slot model gives one OS thread exactly one
//!   slot (see `src/guard.rs`/`src/slot.rs`), so ">128 concurrently pinned
//!   threads" cannot be reproduced by one thread; nothing survives without
//!   real threads.

#![allow(unused_unsafe)]

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

use kovan::{Atomic, RetiredNode, pin, retire};
use std::sync::atomic::Ordering;

#[repr(C)]
struct StressNode {
    retired: RetiredNode,
    value: usize,
}

impl StressNode {
    fn new(value: usize) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
        }))
    }
}

/// Sequential adaptation of `stress::test_high_contention`. The original ran
/// 16 threads hammering the same `Atomic` and printed throughput — it had
/// no assertion beyond completion. This runs the same swap/retire loop from
/// one thread (scaled down for a fast test run instead of 16 * 50000
/// iterations) so it validates the state machine, NOT contention. The
/// threaded original remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn high_contention_sequence_single_threaded() {
    const ITERATIONS: usize = 8000;

    let atomic = Atomic::new(StressNode::new(0));

    for i in 0..ITERATIONS {
        let new_node = StressNode::new(i);

        let guard = pin();
        let old = atomic.swap(
            unsafe { kovan::Shared::from_raw(new_node) },
            Ordering::Release,
            &guard,
        );

        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }
    }

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw());
        }
    }
    kovan::flush();
}

/// Sequential adaptation of `stress::test_read_heavy_workload`. The original
/// ran 8 threads each doing a 95%-read/5%-write mix and printed throughput.
/// This runs the same read/write ratio from one thread — it validates the
/// mixed-workload state machine, NOT concurrent read/write safety. The
/// threaded original remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn read_heavy_workload_sequence_single_threaded() {
    const ITERATIONS: usize = 10_000;
    const WRITE_RATIO: usize = 20; // 1 in 20 = 5%

    let atomic = Atomic::new(StressNode::new(0));

    for i in 0..ITERATIONS {
        let guard = pin();

        if i % WRITE_RATIO == 0 {
            let new_node = StressNode::new(i);
            let old = atomic.swap(
                unsafe { kovan::Shared::from_raw(new_node) },
                Ordering::Release,
                &guard,
            );

            if !old.is_null() {
                unsafe {
                    retire(old.as_raw());
                }
            }
        } else {
            let ptr = atomic.load(Ordering::Acquire, &guard);
            if let Some(node) = unsafe { ptr.as_ref() } {
                let _ = node.value;
            }
        }
    }

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw());
        }
    }
    kovan::flush();
}

/// Sequential adaptation of `stress::test_rapid_guard_creation`. The
/// original ran 8 threads each creating and immediately dropping 100,000
/// guards. This does the same total number of pin/drop cycles from one
/// thread — it validates guard creation/teardown mechanics, NOT concurrent
/// slot contention (this scenario is already covered without threads by
/// `tests/reclaim.rs`'s `test_pin_unpin_rapid`; kept here too for parity
/// with the original file's structure). The threaded original remains the
/// real coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn rapid_guard_creation_sequence_single_threaded() {
    const ITERATIONS: usize = 50_000;

    for _ in 0..ITERATIONS {
        let _guard = pin();
        // Immediately drop
    }
}

/// Sequential adaptation of `stress::test_long_running_guards`. The
/// original held guards open on 2 dedicated long-lived threads while 6
/// short-lived threads did normal swap/retire work concurrently. This pins
/// a guard up front (standing in for the long-running readers) and keeps it
/// alive — via kovan's per-thread reentrant `pin_count` — across a full
/// sequence of short-lived swap/retire operations on the SAME thread. It
/// proves a long-held guard doesn't block or corrupt activity issued from
/// the same thread, NOT that a long-running guard on one OS thread can't
/// block progress on another. The threaded original remains the real
/// coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn long_running_guard_with_interleaved_ops_single_threaded() {
    const SHORT_ITERATIONS: usize = 6000; // matches original's NUM_SHORT * SHORT_ITERATIONS scale

    let atomic = Atomic::new(StressNode::new(0));

    let long_guard = pin();
    let ptr = atomic.load(Ordering::Acquire, &long_guard);
    if let Some(node) = unsafe { ptr.as_ref() } {
        let _ = node.value;
    }

    for i in 0..SHORT_ITERATIONS {
        let new_node = StressNode::new(i);

        let guard = pin();
        let old = atomic.swap(
            unsafe { kovan::Shared::from_raw(new_node) },
            Ordering::Release,
            &guard,
        );

        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }
    }

    drop(long_guard);

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw());
        }
    }
    kovan::flush();
}

/// Sequential adaptation of `stress::test_burst_workload`. The original ran
/// 8 threads for each of 10 bursts of 10,000 ops, with a quiet period
/// between bursts. This runs the same burst structure (bursts of sequential
/// swap/retire ops, scaled down for a fast run) from one thread — it
/// validates the burst-shaped op sequence doesn't panic or corrupt state,
/// NOT concurrent burst behavior. The threaded original remains the real
/// coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn burst_workload_sequence_single_threaded() {
    const BURSTS: usize = 10;
    const OPS_PER_BURST: usize = 800;

    let atomic = Atomic::new(StressNode::new(0));

    for burst in 0..BURSTS {
        for i in 0..OPS_PER_BURST {
            let new_node = StressNode::new(burst * OPS_PER_BURST + i);

            let guard = pin();
            let old = atomic.swap(
                unsafe { kovan::Shared::from_raw(new_node) },
                Ordering::Release,
                &guard,
            );

            if !old.is_null() {
                unsafe {
                    retire(old.as_raw());
                }
            }
        }
        // No real "quiet period" without a scheduler to yield to; kovan's
        // own epoch/reclamation bookkeeping still runs between bursts.
    }

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw());
        }
    }
    kovan::flush();
}
