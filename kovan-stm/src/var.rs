use super::node::StmNode;
use alloc::boxed::Box;
use kovan::Atomic;
use std::sync::atomic::{AtomicU64, Ordering};

/// A Transactional Variable.
///
/// Holds a reference to a versioned piece of data.
/// Concurrent access is safe and managed by the STM.
pub struct TVar<T> {
    /// The version lock word.
    /// Bit 0: Locked flag.
    /// Bits 1..63: Version number.
    pub(crate) version_lock: AtomicU64,

    /// The atomic pointer managed by Kovan.
    /// Accessing this requires a `Guard`.
    pub(crate) data: Atomic<StmNode<T>>,
}

impl<T: Send + Sync + 'static> TVar<T> {
    /// Create a new TVar.
    pub fn new(val: T) -> Self {
        let node = Box::new(StmNode::new(val));
        let ptr = Box::into_raw(node);
        Self {
            version_lock: AtomicU64::new(0),
            data: Atomic::new(ptr),
        }
    }

    /// Helper to decompose the lock word.
    /// Returns (is_locked, version).
    #[inline]
    pub(crate) fn load_version_lock(&self) -> (bool, u64) {
        let val = self.version_lock.load(Ordering::Acquire);
        (val & 1 == 1, val & !1)
    }
}

// Ensure TVar is safe to share across threads
unsafe impl<T: Send + Sync> Send for TVar<T> {}
unsafe impl<T: Send + Sync> Sync for TVar<T> {}
