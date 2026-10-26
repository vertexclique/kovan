//! Single-threaded, wasm-capable adaptation of `tests/recycle.rs`.
//!
//! 2 of the original's 3 tests are adapted; 1 is not ported.
//!
//! Not ported (race-only):
//! - `concurrent_recycling_never_exposes_a_recycled_value` -- readers and
//!   writers racing on one published node, checking that a node a live guard
//!   can see is never recycled. With one thread there is no second guard to
//!   protect anything from, so nothing of the property survives without real
//!   threads. The threaded original, and `tests/shuttle_recycle.rs`, remain
//!   the real coverage on native.

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

use kovan::{Atomic, Reclaimable, RetiredNode, Shared, flush, pin, retire_reclaimable};
use std::collections::HashSet;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

// kovan's reclamation state is process-global and libtest runs the cases of a
// file on several threads, so a node one case retires can be freed, or held
// back, by another. Taking turns keeps the allocation counts exact.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Gives back what a test leaked for `'static`, once every thread that used
/// it is joined and every node that points at it is freed: LeakSanitizer
/// checks the suite.
fn unleak<T>(leaked: &'static T) {
    // SAFETY: made by `Box::leak` in this test, and nothing uses it any more.
    drop(unsafe { Box::from_raw(leaked as *const T as *mut T) });
}

/// Counts its own drops, so a missed or doubled in-place drop shows up.
struct Tracked(&'static Pool);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct Ptr(*mut Slot);

// SAFETY: a pooled node is reachable only through the pool.
unsafe impl Send for Ptr {}

/// The per-structure free list, plus the counters the assertions read.
struct Pool {
    free: Mutex<Vec<Ptr>>,
    allocs: AtomicUsize,
    reuses: AtomicUsize,
    drops: AtomicUsize,
}

impl Pool {
    fn leak() -> &'static Pool {
        Box::leak(Box::new(Pool {
            free: Mutex::new(Vec::new()),
            allocs: AtomicUsize::new(0),
            reuses: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        }))
    }

    fn pooled(&self) -> usize {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    fn allocs(&self) -> usize {
        self.allocs.load(Ordering::Relaxed)
    }

    /// A node holding `seq`: recycled if the pool has one, allocated if not.
    fn acquire(&'static self, seq: u64) -> *mut Slot {
        let recycled = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .map(|p| p.0);
        match recycled {
            Some(node) => {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                // SAFETY: the node left the pool, so this thread owns it. The
                // header is re-armed (fresh birth epoch) before the node can
                // be published, as `Reclaimable` documents.
                unsafe {
                    ptr::write(&raw mut (*node).retired, RetiredNode::new());
                    (*node).seq = seq;
                    (*node).tracked.write(Tracked(self));
                }
                node
            }
            None => {
                self.allocs.fetch_add(1, Ordering::Relaxed);
                Box::into_raw(Box::new(Slot {
                    retired: RetiredNode::new(),
                    seq,
                    tracked: MaybeUninit::new(Tracked(self)),
                    pool: self,
                }))
            }
        }
    }
}

#[repr(C)]
struct Slot {
    retired: RetiredNode,
    seq: u64,
    tracked: MaybeUninit<Tracked>,
    pool: &'static Pool,
}

// SAFETY: `RetiredNode` is the first field of a `#[repr(C)]` struct.
unsafe impl Reclaimable for Slot {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }

    unsafe fn dealloc(node: *mut Self) {
        // SAFETY: kovan never touches the node again and no guard reaches it,
        // so it is ours to drop and hand to the pool.
        unsafe {
            let pool = (*node).pool;
            ptr::drop_in_place((*node).tracked.as_mut_ptr());
            pool.free
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(Ptr(node));
        }
    }
}

/// Publish a fresh node holding `seq`, retire what it replaced.
fn publish(pool: &'static Pool, slot: &Atomic<Slot>, seq: u64) {
    let node = pool.acquire(seq);
    let guard = pin();
    // SAFETY: `node` is unpublished and valid; `old` is unlinked by the swap.
    let old = slot.swap(unsafe { Shared::from_raw(node) }, Ordering::AcqRel, &guard);
    if !old.is_null() {
        // SAFETY: retired once, never touched again by this thread.
        unsafe { retire_reclaimable(old.as_raw()) };
    }
}

/// Sequential adaptation of `recycle::retired_nodes_return_to_the_pool_and_are_reused`.
/// The original is single-threaded already; this one avoids `Instant`, which
/// panics on wasm32-unknown-unknown, so the drain is a fixed number of flushes
/// instead of a deadline.
#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn retired_nodes_return_to_the_pool_and_are_reused_single_threaded() {
    const WARMUP: u64 = 2_048;
    const STEADY: u64 = 20_000;

    let _serial = serial();
    let pool = Pool::leak();
    let slot = Atomic::<Slot>::null();

    for seq in 0..WARMUP {
        publish(pool, &slot, seq);
        if seq % 64 == 63 {
            flush();
        }
    }
    flush();
    let warm = pool.allocs();
    assert!(warm > 0 && pool.pooled() > 0, "warm-up returned no node");

    for seq in WARMUP..WARMUP + STEADY {
        publish(pool, &slot, seq);
        if seq % 64 == 63 {
            flush();
        }
    }
    let grown = pool.allocs() - warm;
    assert_eq!(
        grown, 0,
        "allocations grew by {grown} over {STEADY} publishes after warm-up ({warm} at warm-up)"
    );
    let reuses = pool.reuses.load(Ordering::Relaxed);
    assert!(
        reuses as u64 >= STEADY,
        "only {reuses} of {STEADY} steady-state publishes reused a node"
    );

    // The published node holds the last value written.
    {
        let guard = pin();
        let shared = slot.load(Ordering::Acquire, &guard);
        // SAFETY: loaded under `guard`, which is live.
        assert_eq!(unsafe { shared.deref() }.seq, WARMUP + STEADY - 1);
    }

    // Retire the node still published and drain: the books must balance.
    {
        let guard = pin();
        let last = slot.swap(
            unsafe { Shared::from_raw(ptr::null_mut()) },
            Ordering::AcqRel,
            &guard,
        );
        unsafe { retire_reclaimable(last.as_raw()) };
    }
    for _ in 0..16 {
        flush();
    }
    let allocs = pool.allocs();
    assert_eq!(pool.pooled(), allocs, "a retired node never came back");
    assert_eq!(
        pool.drops.load(Ordering::Relaxed),
        allocs + reuses,
        "every value must be dropped in place exactly once"
    );
    let distinct: HashSet<usize> = pool
        .free
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .map(|p| p.0 as usize)
        .collect();
    assert_eq!(distinct.len(), allocs, "a node was pooled twice");
    for Ptr(node) in pool
        .free
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .drain(..)
    {
        // SAFETY: allocated by `acquire` with `Box`, owned by the pool.
        // `tracked` is a `MaybeUninit`, so the drop below skips it.
        drop(unsafe { Box::from_raw(node) });
    }
    unleak(pool);
}

/// A type that keeps the default `dealloc` frees its `Box`, as `retire` does.
#[repr(C)]
struct Plain {
    retired: RetiredNode,
    drops: &'static AtomicUsize,
}

impl Drop for Plain {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

// SAFETY: `RetiredNode` is the first field of a `#[repr(C)]` struct.
unsafe impl Reclaimable for Plain {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }
}

/// Sequential adaptation of `recycle::default_dealloc_frees_the_box`, with a
/// fixed number of flushes in place of the original's deadline.
#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn default_dealloc_frees_the_box_single_threaded() {
    const NODES: usize = 1_000;

    let _serial = serial();
    let drops: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
    for _ in 0..NODES {
        let node = Box::into_raw(Box::new(Plain {
            retired: RetiredNode::new(),
            drops,
        }));
        let _guard = pin();
        // SAFETY: never published, retired once.
        unsafe { retire_reclaimable(node) };
    }
    for _ in 0..16 {
        flush();
    }
    assert_eq!(drops.load(Ordering::Relaxed), NODES);
    unleak(drops);
}
