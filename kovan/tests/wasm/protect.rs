//! Single-threaded, wasm-capable adaptation of `tests/protect.rs`.
//!
//! Both tests in the original exist to prove era tracking works *across
//! threads*: thread A pins, loads a pointer, then loads a second pointer
//! born after another thread advanced the epoch and retired things — proving
//! `load()` updates A's recorded era so the second pointer isn't freed out
//! from under it. The mechanism itself (does a second `load()` on an
//! already-pinned guard update this thread's recorded era so a
//! later-born node stays protected) is provable on one thread: single-
//! threaded `retire()` advances the epoch too (every EPOCH_FREQ = 128
//! retires — see `tests/reclaim.rs`'s header note), so both tests are
//! adapted to run that whole sequence on one thread. What's lost is
//! cross-thread *visibility* of the era update (does the reclaimer on
//! thread B correctly observe thread A's era store) — that can only be
//! shown with a real second thread. Each adapted test's doc comment says
//! so explicitly.
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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Node with embedded RetiredNode for proper reclamation.
///
/// Counters are `Arc<Atomic*>`, never `Rc<Cell<_>>`.
///
/// `retire()`'s "Cross-thread drop" note (`kovan::guard`) says a retired
/// node's destructor runs on whichever thread later traverses the slot its
/// batch landed in -- typically NOT the retiring thread -- and that
/// thread-affine payloads (`Rc`, lock guards) are sound only when reclamation
/// is single-threaded.
///
/// Each test function here IS single-threaded, but that is not the relevant
/// scope: kovan's reclaimer is process-global and libtest runs test functions
/// on several threads at once, so a batch retired by one test thread is freed
/// by another. An `Rc` refcount touched from two threads corrupts the heap
/// (observed as `tcache_thread_shutdown(): unaligned tcache chunk detected`).
/// `Arc<Atomic*>` is what the pristine `tests/` files use, for this reason.
#[repr(C)]
struct EraTestNode {
    retired: RetiredNode,
    value: u64,
    freed: Arc<AtomicBool>,
}

impl EraTestNode {
    fn new(value: u64, freed: Arc<AtomicBool>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
            freed,
        }))
    }
}

impl Drop for EraTestNode {
    fn drop(&mut self) {
        self.freed.store(true, Ordering::SeqCst);
    }
}

/// A node used only to force epoch advancement via retire().
#[repr(C)]
struct DummyNode {
    retired: RetiredNode,
}

impl DummyNode {
    fn new() -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
        }))
    }
}

/// Advance the global epoch by retiring many dummy nodes on this thread.
///
/// Single-threaded retire still advances the epoch every EPOCH_FREQ (128)
/// retires (see `tests/reclaim.rs`'s header note); this walks that many
/// multiples sequentially instead of splitting the work across threads.
fn advance_epoch_by(n: usize) {
    for _ in 0..(n * 128) {
        let _guard = pin();
        unsafe { retire(DummyNode::new()) };
    }
}

/// Sequential adaptation of `protect::test_protect_prevents_uaf_across_epochs`.
/// See the file header for what survives and what doesn't.
#[test]
#[cfg_attr(miri, ignore)]
fn era_updates_on_load_prevent_premature_free_single_threaded() {
    let freed_a = Arc::new(AtomicBool::new(false));
    let freed_b = Arc::new(AtomicBool::new(false));
    let shared = Atomic::new(EraTestNode::new(1, freed_a.clone()));

    // Pin BEFORE the epoch advances and BEFORE the later-born node is
    // retired.
    let guard = pin();
    let ptr1 = shared.load(Ordering::Acquire, &guard);
    assert_eq!(unsafe { ptr1.as_ref() }.unwrap().value, 1);

    // Advance the epoch significantly.
    advance_epoch_by(4);

    // Store a new node, born at the now-advanced epoch, and retire the old one.
    let new_ptr = EraTestNode::new(2, freed_b.clone());
    let old = shared.swap(
        unsafe { kovan::Shared::from_raw(new_ptr) },
        Ordering::AcqRel,
        &guard,
    );
    if !old.is_null() {
        unsafe { retire(old.as_raw()) };
    }

    // Second load, with the SAME guard: must update our era to cover the
    // later-born node so it isn't reclaimed while we still hold it.
    let ptr2 = shared.load(Ordering::Acquire, &guard);
    assert_eq!(
        unsafe { ptr2.as_ref() }.unwrap().value,
        2,
        "should see the new node"
    );

    // Drive more retires/epoch churn while still holding `guard`.
    for _ in 0..2000u64 {
        let dummy = DummyNode::new();
        let _g = pin();
        unsafe { retire(dummy) };
    }
    kovan::flush();

    // Safety: the node loaded via ptr2, born after our pin, must not be
    // freed while `guard` (pinned before it was born) is still alive.
    assert!(
        !freed_b.load(Ordering::SeqCst),
        "node born in later epoch was freed while guard was held! (UAF)"
    );

    drop(guard);

    // Cleanup
    let cleanup_guard = pin();
    let old = shared.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::AcqRel,
        &cleanup_guard,
    );
    if !old.is_null() {
        unsafe { retire(old.as_raw()) };
    }
    kovan::flush();
}

#[repr(C)]
struct CountedNode {
    retired: RetiredNode,
    value: usize,
    drop_count: Arc<AtomicUsize>,
}

impl Drop for CountedNode {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

/// Sequential adaptation of `protect::test_era_updates_across_many_loads`.
/// See the file header for what survives and what doesn't.
#[test]
#[cfg_attr(miri, ignore)]
fn era_tracking_many_loads_sequential() {
    let drops = Arc::new(AtomicUsize::new(0));
    let shared: Atomic<CountedNode> = Atomic::null();

    let mut loads = 0u64;
    for i in 0..2000i64 {
        let node = Box::into_raw(Box::new(CountedNode {
            retired: RetiredNode::new(),
            value: i as usize,
            drop_count: drops.clone(),
        }));
        let guard = pin();
        let old = shared.swap(
            unsafe { kovan::Shared::from_raw(node) },
            Ordering::AcqRel,
            &guard,
        );
        if !old.is_null() {
            unsafe { retire(old.as_raw()) };
        }

        let ptr = shared.load(Ordering::Acquire, &guard);
        if let Some(n) = unsafe { ptr.as_ref() } {
            let _ = std::hint::black_box(n.value);
        }
        loads += 1;
    }

    assert!(loads > 0, "readers should have done some loads");
    kovan::flush();
    assert!(
        drops.load(Ordering::SeqCst) > 0,
        "some nodes should be freed"
    );
}
