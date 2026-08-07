//! Single-threaded, wasm-capable adaptation of `tests/robust.rs`.
//!
//! 2 of the original's 3 tests are adapted; 1 is not ported.
//!
//! Not ported (race-only):
//! - `test_bounded_memory_with_stalls` — a 5 real-second soak test with one
//!   stalled reader thread and 8 active writer threads, whose only
//!   assertion is that it completes (it prints throughput, it doesn't check
//!   a bound). Its distinct value over `test_stalled_thread_handling` is
//!   "the system stays responsive under a real concurrent stall for a
//!   sustained duration" — a genuinely concurrent throughput/robustness
//!   claim that a single serial thread cannot stall itself against (there
//!   is no scheduler to contend with). The correctness aspect it shares
//!   with `test_stalled_thread_handling` — a long-held guard doesn't
//!   corrupt or block other operations — is already covered by that test's
//!   adaptation below, so nothing distinct survives porting this one.

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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// `value` alone is stored, no drop-counting needed — bare struct, `Rc`
/// vs. `Arc` makes no difference here, so this stays as-is for fidelity
/// with the original.
#[repr(C)]
struct RobustNode {
    retired: RetiredNode,
    value: usize,
}

impl RobustNode {
    fn new(value: usize) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
        }))
    }
}

/// Sequential adaptation of `robust::test_adaptive_slot_selection`. The
/// original's own comment says its check is implicit: "if the system
/// doesn't hang, it's working." Run from 16 threads, that's really a claim
/// about slot allocation staying correct under concurrent contention across
/// many distinct slots. Run from one thread there's only one slot, so this
/// only proves the swap/retire loop itself doesn't hang or corrupt state
/// over many iterations — not concurrent slot-selection correctness. The
/// threaded original remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn adaptive_slot_selection_sequence_single_threaded() {
    const ITERATIONS: usize = 8000; // scaled down from 16 * 10000 for a fast run

    let atomic = Atomic::new(RobustNode::new(0));

    for i in 0..ITERATIONS {
        let new_node = RobustNode::new(i);

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

/// Sequential adaptation of `robust::test_stalled_thread_handling`. The
/// original held guards open on 2 dedicated "stalled" threads for a while
/// (sleeping between reads) while 4 "active" threads did normal swap/retire
/// work concurrently, then asserted every active operation completed. This
/// pins a guard up front (standing in for the stalled reader) and keeps it
/// alive — via kovan's per-thread reentrant `pin_count` — across a full
/// sequence of "active" swap/retire operations on the SAME thread, then
/// asserts every one of them completed and the exact op count matches. It
/// proves a long-held guard doesn't block or corrupt concurrent-shaped
/// activity issued from the same thread, NOT that a stalled reader on one
/// OS thread can't block progress on another. The threaded original remains
/// the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn active_ops_progress_with_long_held_guard_single_threaded() {
    const ACTIVE_ITERATIONS: usize = 4000; // matches original's NUM_ACTIVE * ITERATIONS scale

    let atomic = Atomic::new(RobustNode::new(0));
    let ops_count = Arc::new(AtomicUsize::new(0));

    // "Stalled" reader: pin and hold the guard across every active op below.
    let stalled_guard = pin();
    let ptr = atomic.load(Ordering::Acquire, &stalled_guard);
    if let Some(node) = unsafe { ptr.as_ref() } {
        let _ = node.value;
    }

    for i in 0..ACTIVE_ITERATIONS {
        let new_node = RobustNode::new(i);

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

        ops_count.fetch_add(1, Ordering::Relaxed);
    }

    drop(stalled_guard);

    let total_ops = ops_count.load(Ordering::Relaxed);
    assert_eq!(total_ops, ACTIVE_ITERATIONS);

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
