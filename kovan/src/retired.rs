//! Retired node structures for ASMR batch-based memory reclamation.
//!
//! Node layout for the ASMR algorithm:
//! - `next`: AtomicPtr — list link in slot (exchanged with INVPTR during traverse)
//! - `batch_link`: AtomicPtr — points to refs-node; on refs-node: RNODE(batch_first)
//! - `refs_or_next`: union — atomic refs counter on refs-node, batch_next on others
//! - `birth_epoch`: epoch at which this node was allocated (on refs-node: min of batch)
//! - `destructor`: type-erased destructor for deallocation

use core::sync::atomic::{AtomicPtr, AtomicUsize};

/// Type-erased destructor function
pub(crate) type DestructorFn = unsafe fn(*mut RetiredNode);

/// Sentinel value meaning "slot inactive / node already traversed"
pub(crate) const INVPTR: usize = !0x0000_0000_0000_0000_usize;

/// REFC_PROTECT bias for reference counting (1 << 63)
pub(crate) const REFC_PROTECT: usize = 1_usize << 63;

/// Mark a pointer as an RNODE (XOR with 1)
#[inline]
pub(crate) fn rnode_mark(ptr: *mut RetiredNode) -> *mut RetiredNode {
    (ptr as usize ^ 1) as *mut RetiredNode
}

/// Check if a pointer is an RNODE marker
#[inline]
pub(crate) fn is_rnode(ptr: *const RetiredNode) -> bool {
    (ptr as usize) & 1 != 0
}

/// Get the actual pointer from an RNODE-marked pointer
#[inline]
pub(crate) fn rnode_unmask(ptr: *mut RetiredNode) -> *mut RetiredNode {
    (ptr as usize ^ 1) as *mut RetiredNode
}

/// Node structure embedded in user's data structure.
///
/// Users must embed this at the start of their node type to enable retirement.
///
/// Layout for ASMR:
/// - `next`: atomic next pointer in slot's retirement list; exchanged with INVPTR during traverse
/// - `batch_link`: points to refs-node for non-refs nodes; on refs-node: RNODE(batch_first)
/// - `refs_or_next`: union — atomic refs counter on refs-node, batch_next pointer on others
/// - `birth_epoch`: allocation epoch; on refs-node: minimum birth epoch of entire batch
/// - `destructor`: type-erased destructor for deallocation
#[repr(C, align(8))]
pub struct RetiredNode {
    /// Next node in slot's retirement list (atomic — exchanged during traverse).
    /// Also used as slot pointer during try_retire preparation phase,
    /// and as free-list link after refs reach 0.
    pub(crate) next: AtomicPtr<RetiredNode>,

    /// Points to the refs-node for non-refs nodes.
    /// On the refs-node: set to RNODE(batch_first) after batch finalization.
    /// Atomic because helpers may read it concurrently.
    pub(crate) batch_link: AtomicPtr<RetiredNode>,

    /// Union: atomic refs counter (on refs-node) | batch_next pointer (others).
    /// On refs-node: initialized to REFC_PROTECT (1<<63).
    /// On list nodes: batch_next pointer for walking the batch during free.
    pub(crate) refs_or_next: AtomicUsize,

    /// Birth epoch of this node's allocation.
    /// On the refs-node: minimum birth epoch across the entire batch.
    pub(crate) birth_epoch: u64,

    /// Type-erased destructor — set during retire().
    /// Used by free_list to deallocate each node in a batch.
    pub(crate) destructor: Option<DestructorFn>,
}

impl RetiredNode {
    /// Create a new RetiredNode with the current global epoch as birth_epoch.
    ///
    /// The birth_epoch must be set at allocation time (not retirement time)
    /// so that threads pinned before this allocation can be correctly identified
    /// as not needing protection for this node.
    pub fn new() -> Self {
        Self {
            next: AtomicPtr::new(core::ptr::null_mut()),
            batch_link: AtomicPtr::new(core::ptr::null_mut()),
            refs_or_next: AtomicUsize::new(0),
            birth_epoch: crate::slot::global().get_epoch(),
            destructor: None,
        }
    }

    /// Read the batch_next pointer (non-atomic, for batch construction and freeing)
    #[inline]
    pub(crate) fn batch_next(&self) -> *mut RetiredNode {
        self.refs_or_next
            .load(core::sync::atomic::Ordering::Relaxed) as *mut RetiredNode
    }

    /// Set the batch_next pointer (non-atomic, for batch construction)
    #[inline]
    pub(crate) fn set_batch_next(&self, next: *mut RetiredNode) {
        self.refs_or_next
            .store(next as usize, core::sync::atomic::Ordering::Relaxed);
    }

    /// Store (tid, slot_index) packed into the `next` field during try_retire scan phase.
    /// This avoids pointer arithmetic that would violate Stacked Borrows under Miri.
    #[inline]
    pub(crate) fn set_slot_info(&self, tid: usize, index: usize) {
        let packed = (tid << 16) | index;
        self.next.store(
            packed as *mut RetiredNode,
            core::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Read the (tid, slot_index) packed into `next` during try_retire insert phase.
    #[inline]
    pub(crate) fn get_slot_info(&self) -> (usize, usize) {
        let packed = self.next.load(core::sync::atomic::Ordering::Relaxed) as usize;
        (packed >> 16, packed & 0xFFFF)
    }
}

impl Default for RetiredNode {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: RetiredNode contains only raw pointers and atomics which are Send
unsafe impl Send for RetiredNode {}
// SAFETY: RetiredNode synchronization handled by atomic operations
unsafe impl Sync for RetiredNode {}
