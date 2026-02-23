use super::node::StmNode;
use alloc::boxed::Box;
use kovan::Atomic;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Heap-allocated interior of a [`TVar`].
///
/// Separated from the `TVar` wrapper so that transactions can clone an `Arc`
/// pointing here, extending the lifetime of the version_lock and data fields
/// independently of the outer `TVar` handle.
pub(crate) struct TVarInner<T: Send + Sync + 'static> {
    /// The version lock word.
    /// Bit 0: Locked flag.
    /// Bits 1..63: Version number.
    pub(crate) version_lock: AtomicU64,

    /// The atomic pointer managed by Kovan.
    /// Accessing this requires a `Guard`.
    pub(crate) data: Atomic<StmNode<T>>,
}

impl<T: Send + Sync + 'static> TVarInner<T> {
    fn new(val: T) -> Arc<Self> {
        let node = Box::new(StmNode::new(val));
        let ptr = Box::into_raw(node);
        Arc::new(Self {
            version_lock: AtomicU64::new(0),
            data: Atomic::new(ptr),
        })
    }

    /// Helper to decompose the lock word.
    /// Returns (is_locked, version).
    #[inline]
    pub(crate) fn load_version_lock(&self) -> (bool, u64) {
        let val = self.version_lock.load(Ordering::Acquire);
        (val & 1 == 1, val & !1)
    }
}

/// Retire the heap-allocated `StmNode` when the last Arc clone
/// pointing to this `TVarInner` is dropped.  `retire()` defers the actual
/// free until all concurrent Kovan guards have been released, so readers
/// that loaded the pointer while holding a pin remain safe.
impl<T: Send + Sync + 'static> Drop for TVarInner<T> {
    fn drop(&mut self) {
        let guard = kovan::pin();
        let node = self.data.load(Ordering::Acquire, &guard);
        if !node.is_null() {
            // SAFETY: StmNode is #[repr(C)] with RetiredNode at offset 0,
            // satisfying kovan::retire's layout requirement.
            unsafe { kovan::retire(node.as_raw()) };
        }
    }
}

/// A Transactional Variable.
///
/// Holds a reference-counted pointer to versioned data.
/// Concurrent access is safe and managed by the STM.
///
/// The inner state is stored behind an `Arc<TVarInner<T>>` so that
/// transactions can cheaply extend the lifetime of the `version_lock` and
/// `data` fields by cloning the Arc into the read/write sets — even after
/// the `TVar` handle itself is dropped.
pub struct TVar<T: Send + Sync + 'static>(pub(crate) Arc<TVarInner<T>>);

impl<T: Send + Sync + 'static> TVar<T> {
    /// Create a new TVar.
    pub fn new(val: T) -> Self {
        TVar(TVarInner::new(val))
    }
}

// Ensure TVar is safe to share across threads
unsafe impl<T: Send + Sync> Send for TVar<T> {}
unsafe impl<T: Send + Sync> Sync for TVar<T> {}
