//! Single-threaded, wasm-capable adaptation of `tests/reclaim.rs`.
//!
//! 7 of the original's 9 tests port or adapt cleanly: 4 were already
//! single-threaded (no `thread::spawn` at all — `test_guard_protects_from_reclamation`,
//! `test_pin_unpin_rapid`, `test_multiple_guards_sequential`,
//! `test_reentrant_destructor_flush`) and are ported verbatim; 3 threaded
//! ones (`test_retire_eventually_frees`, `test_concurrent_retire`,
//! `test_reentrant_destructor_concurrent`) are adapted to run the same total
//! amount of work sequentially, since the property they check (retired
//! nodes are eventually freed, re-entrant destructors don't crash or
//! double-drop) doesn't depend on multiple threads to be true.
//!
//! Not ported (race-only):
//! - `test_reentrant_destructor_thread_exit` — exercises
//!   `Handle::cleanup()` / `drain_free_list` on a real OS thread's exit
//!   (the TLS-destructor path). There is no analogous event on wasm's
//!   single persistent thread: a thread that never exits never triggers
//!   `cleanup()`, so nothing meaningful survives without an actual thread
//!   to spawn and join.

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

use kovan::{Atom, AtomOption, Atomic, RetiredNode, pin, retire};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Bare retired node — not wrapped by `Atom<T>`, so no `Send + Sync` bound
/// applies and a thread-affine `Rc<Cell<_>>` counter is sound (see
/// `kovan::retire`'s "Cross-thread drop" note: sound for single-threaded
/// reclamation, which every test in this file is).
#[repr(C)]
struct CountedNode {
    retired: RetiredNode,
    drop_count: Rc<Cell<usize>>,
}

impl Drop for CountedNode {
    fn drop(&mut self) {
        self.drop_count.set(self.drop_count.get() + 1);
    }
}

/// Sequential adaptation of `reclaim::test_retire_eventually_frees`. The
/// original split 2048 retires across 4 threads specifically to force
/// multiple epoch transitions (the file's own comment: single-threaded
/// retire only advances the epoch every EPOCH_FREQ = 128 retires). This
/// performs the same total number of retires from one thread instead —
/// still far more than 128, so the epoch still advances repeatedly — and
/// checks the liveness property (some nodes get freed), not exact timing.
/// The threaded original remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn retire_eventually_frees_single_threaded() {
    let drops = Rc::new(Cell::new(0usize));

    for _ in 0..2048 {
        let node = Box::into_raw(Box::new(CountedNode {
            retired: RetiredNode::new(),
            drop_count: drops.clone(),
        }));
        let _guard = pin();
        unsafe { retire(node) };
    }
    kovan::flush();

    assert!(drops.get() > 0, "expected some nodes to be freed");
}

#[test]
fn test_guard_protects_from_reclamation() {
    let drops = Rc::new(Cell::new(0usize));
    let atomic = Atomic::new(Box::into_raw(Box::new(CountedNode {
        retired: RetiredNode::new(),
        drop_count: drops.clone(),
    })));

    let guard = pin();
    let ptr = atomic.load(Ordering::Acquire, &guard);
    unsafe { retire(ptr.as_raw()) };

    // While guard is held, node should not be freed (within same epoch)
    assert_eq!(drops.get(), 0);

    drop(guard);
}

/// Sequential adaptation of `reclaim::test_concurrent_retire`. The original
/// split 1600 retires across 8 threads then did 512 more retires to flush
/// pending batches. This performs the same total number of retires from one
/// thread and checks the same liveness property (some nodes freed, no
/// double free), not concurrent-retire safety. The threaded original
/// remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn retire_under_load_sequence_single_threaded() {
    let drops = Rc::new(Cell::new(0usize));

    for _ in 0..1600 {
        let node = Box::into_raw(Box::new(CountedNode {
            retired: RetiredNode::new(),
            drop_count: drops.clone(),
        }));
        let _guard = pin();
        unsafe { retire(node) };
    }

    // Do more work to flush pending batches
    for _ in 0..512 {
        let node = Box::into_raw(Box::new(CountedNode {
            retired: RetiredNode::new(),
            drop_count: drops.clone(),
        }));
        let _guard = pin();
        unsafe { retire(node) };
    }

    // At least some should have been freed
    assert!(drops.get() > 0);
}

#[test]
fn test_pin_unpin_rapid() {
    // Rapidly pin and unpin without retiring anything
    for _ in 0..10_000 {
        let _guard = pin();
    }
}

#[test]
fn test_multiple_guards_sequential() {
    let atomic: Atomic<u64> = Atomic::new(Box::into_raw(Box::new(42u64)));

    for _ in 0..100 {
        let guard = pin();
        let ptr = atomic.load(Ordering::Acquire, &guard);
        assert_eq!(unsafe { *ptr.as_raw() }, 42);
    }

    // Cleanup
    let guard = pin();
    let ptr = atomic.load(Ordering::Acquire, &guard);
    unsafe { drop(Box::from_raw(ptr.as_raw())) };
}

// ============================================================================
// Re-entrancy regression tests
//
// These test the scenario that caused the misaligned pointer dereference:
// destructors calling pin()/flush() during free_batch_list when pin_count is 0
// (cleanup / flush paths). Without the fix, the free_list Cell retains stale
// pointers to already-freed nodes, causing use-after-free / double-free.
//
// `drop_count: Arc<AtomicUsize>` stays `Arc` here (not `Rc<Cell<_>>`):
// `ReentrantDrop` is stored inside `Atom<T>`/`AtomOption<T>`, which require
// `T: Send + Sync` unconditionally — that's an API bound, not a concurrency
// choice made by these tests.
// ============================================================================

/// A type whose destructor re-enters kovan by dropping an inner Atom.
/// This triggers: destructor -> Atom::drop -> flush() -> free_batch_list.
struct ReentrantDrop {
    _inner: Option<Atom<u64>>,
    drop_count: Arc<AtomicUsize>,
}

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        // The Atom::drop here calls flush(), which is the re-entrant path.
        // Before the fix, this caused use-after-free in the free_list Cell.
        self._inner.take();
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

/// Regression test: Atom<T> where T's destructor drops another Atom.
///
/// Exercises the flush() -> free_batch_list -> destructor -> Atom::drop ->
/// flush() re-entrancy path that caused misaligned pointer dereference.
#[test]
#[cfg_attr(miri, ignore)]
fn test_reentrant_destructor_flush() {
    let drops = Arc::new(AtomicUsize::new(0));

    for _ in 0..10 {
        let d = drops.clone();
        let outer = Atom::new(ReentrantDrop {
            _inner: Some(Atom::new(42u64)),
            drop_count: d,
        });

        // Store several times to build up retired nodes
        for i in 0..200 {
            outer.store(ReentrantDrop {
                _inner: Some(Atom::new(i)),
                drop_count: drops.clone(),
            });
        }
        // Dropping outer triggers: Atom::drop -> flush -> free_batch_list
        // -> ReentrantDrop::drop -> Atom::drop -> flush (re-entrant!)
        drop(outer);
    }

    assert!(drops.load(Ordering::SeqCst) > 0, "destructors must run");
}

/// Sequential adaptation of `reclaim::test_reentrant_destructor_concurrent`.
/// The original ran 4 threads each doing 200 stores against a shared `Atom`
/// whose value's destructor re-enters kovan. This performs the same total
/// number of stores (4 * 200 = 800) from one thread — it still exercises
/// the re-entrant destructor path (`Atom::drop` -> `flush()` ->
/// `free_batch_list` -> `ReentrantDrop::drop` -> `Atom::drop` -> `flush()`
/// again) under sustained load, just not concurrent contention on the same
/// `Atom`. The threaded original remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)]
fn reentrant_destructor_repeated_stores_single_threaded() {
    let drops = Arc::new(AtomicUsize::new(0));
    let shared = Atom::new(ReentrantDrop {
        _inner: Some(Atom::new(0u64)),
        drop_count: drops.clone(),
    });

    for i in 0..800 {
        shared.store(ReentrantDrop {
            _inner: Some(Atom::new(i as u64)),
            drop_count: drops.clone(),
        });
    }

    drop(shared);

    assert!(drops.load(Ordering::SeqCst) > 0, "destructors must run");
}

/// Regression test: AtomOption::take with re-entrant destructors.
///
/// Exercises the Removed<T>::drop -> DeferDrop -> retire -> enqueue_node ->
/// try_retire -> free_batch_list -> destructor -> pin() path.
#[test]
#[cfg_attr(miri, ignore)]
fn test_reentrant_destructor_atom_option_take() {
    let drops = Arc::new(AtomicUsize::new(0));

    for _ in 0..20 {
        let opt = AtomOption::some(ReentrantDrop {
            _inner: Some(Atom::new(99u64)),
            drop_count: drops.clone(),
        });

        // Build up retired nodes
        for i in 0..100 {
            opt.store_some(ReentrantDrop {
                _inner: Some(Atom::new(i)),
                drop_count: drops.clone(),
            });
        }

        // take() returns Removed<T>, whose Drop defers T's destructor.
        // When the Removed is dropped, DeferDrop -> retire -> enqueue_node
        // -> eventually free_batch_list -> ReentrantDrop::drop -> Atom::drop
        let taken = opt.take();
        drop(taken);
        drop(opt);
    }

    assert!(drops.load(Ordering::SeqCst) > 0, "destructors must run");
}
