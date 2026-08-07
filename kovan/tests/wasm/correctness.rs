//! Single-threaded, wasm-capable adaptation of `tests/correctness.rs`.
//!
//! All 4 tests in the original spawn threads. Every one still has a
//! meaningful single-threaded core, so all 4 are adapted here (none are
//! race-only): the safety invariant these tests check — a node retired
//! while a guard pinned before that retire is still alive must not be
//! freed — holds regardless of whether the pinning thread and the retiring
//! thread are the same thread or not, so it's fully provable sequentially.
//! What's lost is cross-thread *visibility* of that protection (does the
//! reclaimer correctly see another thread's era from that thread's own
//! memory), which only a real second thread can exercise; each adapted
//! test's doc comment says so explicitly.
//!
//! Not ported (race-only): none.

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
use std::sync::atomic::{AtomicBool, Ordering};

/// Node for testing with embedded RetiredNode.
///
/// `freed` stays `Arc<AtomicBool>` rather than `Rc<Cell<bool>>` purely by
/// convention with the original — `Atomic<T>` (unlike `Atom<T>`) has no
/// `Send + Sync` bound on `T`, and `retire()`'s own safety docs sanction
/// `Rc`-style thread-affine payloads for single-threaded reclamation
/// (`kovan::retire`'s "Cross-thread drop" note). Nothing here is shared
/// across threads either way.
#[repr(C)]
struct TestNode {
    retired: RetiredNode,
    value: usize,
    freed: Arc<AtomicBool>,
}

impl TestNode {
    fn new(value: usize, freed: Arc<AtomicBool>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
            freed,
        }))
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.freed.store(true, Ordering::Release);
    }
}

/// Sequential adaptation of `correctness::test_no_premature_free`. The
/// original held a guard on one thread while a second thread retired the
/// node underneath it, proving cross-thread visibility of the guard's
/// protection. This version pins a guard, retires the same node from the
/// SAME thread while that guard is still alive, and checks the exact safety
/// invariant that always holds regardless of threading: while a guard
/// pinned before a retire is alive, the retired node must not be freed. It
/// does not exercise cross-thread visibility of that protection; the
/// threaded original remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn guard_pinned_before_retire_prevents_premature_free_single_threaded() {
    let freed = Arc::new(AtomicBool::new(false));
    let atomic = Atomic::new(TestNode::new(42, freed.clone()));

    // Pin BEFORE the retire below.
    let guard = pin();
    let ptr = atomic.load(Ordering::Acquire, &guard);
    assert_eq!(unsafe { ptr.as_ref() }.unwrap().value, 42);

    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    assert!(!old.is_null());
    unsafe { retire(old.as_raw()) };

    // Drive enough retires to cross several batch/epoch boundaries.
    for i in 0..2000usize {
        let dummy = TestNode::new(i, Arc::new(AtomicBool::new(false)));
        unsafe { retire(dummy) };
    }
    kovan::flush();

    // Safety invariant: `guard`, pinned before the retire, is still alive.
    assert!(!freed.load(Ordering::Acquire), "node freed prematurely!");
    assert_eq!(unsafe { ptr.as_ref() }.unwrap().value, 42);

    drop(guard);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_eventual_reclamation() {
    // Test that all retired nodes are eventually reclaimed. We do this by
    // retiring many nodes and verifying the system doesn't crash and that
    // memory is bounded (implicit through successful completion). This test
    // was already single-threaded in the original.

    const NUM_NODES: usize = 10000;
    let atomic = Atomic::new(std::ptr::null_mut::<TestNode>());

    // Allocate and retire many nodes rapidly
    for i in 0..NUM_NODES {
        let freed = Arc::new(AtomicBool::new(false));
        let node = Box::into_raw(Box::new(TestNode {
            retired: RetiredNode::new(),
            value: i,
            freed,
        }));

        let guard = pin();
        let old = atomic.swap(
            unsafe { kovan::Shared::from_raw(node) },
            Ordering::Release,
            &guard,
        );

        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }

        // Periodically create guards to trigger reclamation
        if i % 100 == 0 {
            for _ in 0..10 {
                let _guard = pin();
            }
        }
    }

    // Retire final node
    {
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
    }

    // Create many guards to trigger reclamation
    for _ in 0..1000 {
        let _guard = pin();
    }

    // If we got here without crashing or OOM, reclamation is working
    println!(
        "Eventual reclamation test: retired {} nodes successfully",
        NUM_NODES
    );
}

/// Sequential adaptation of `correctness::test_concurrent_access`. The
/// original ran 4 reader threads and 4 writer threads concurrently against
/// the same `Atomic`. This performs the same total read/write operation mix
/// from one thread, so it validates the load/swap/retire state machine, NOT
/// concurrent reader/writer safety. The threaded original remains the real
/// coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_access_sequence_single_threaded() {
    const ITERATIONS: usize = 4000; // matches original's NUM_THREADS/2 * ITERATIONS scale

    let atomic = Atomic::new(TestNode::new(0, Arc::new(AtomicBool::new(false))));

    for i in 0..ITERATIONS {
        // "reader" half
        {
            let guard = pin();
            let ptr = atomic.load(Ordering::Acquire, &guard);
            if let Some(node) = unsafe { ptr.as_ref() } {
                let _ = node.value;
            }
        }
        // "writer" half
        {
            let new_node = TestNode::new(i, Arc::new(AtomicBool::new(false)));
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
    }

    // Cleanup final node
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

/// Adaptation of `correctness::test_guard_drop_triggers_reclamation`. The
/// original ran the retire loop inside a spawned thread purely to isolate
/// it, then joined before checking reclamation — the spawn itself added no
/// concurrency (nothing else touched the atomic while that thread ran), so
/// this inlines the same sequential logic directly with no adaptation
/// needed beyond removing the redundant thread.
#[test]
#[cfg_attr(miri, ignore)]
fn test_guard_drop_triggers_reclamation() {
    const NUM_RETIRES: usize = 1000;
    let atomic = Atomic::new(std::ptr::null_mut::<TestNode>());

    for i in 0..NUM_RETIRES {
        let node = TestNode::new(i, Arc::new(AtomicBool::new(false)));
        let guard = pin();
        let old = atomic.swap(
            unsafe { kovan::Shared::from_raw(node) },
            Ordering::Release,
            &guard,
        );

        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }
    }

    // Final cleanup
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

    // Create guards to trigger reclamation
    for _ in 0..500 {
        let _guard = pin();
    }

    println!(
        "Guard drop triggers reclamation test: PASS - {} nodes retired successfully",
        NUM_RETIRES
    );
}
