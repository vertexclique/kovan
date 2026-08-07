//! Retired node structures for ASMR batch-based memory reclamation.
//!
//! Node layout for the ASMR algorithm:
//! - `next`: AtomicPtr — list link in slot (exchanged with INVPTR during traverse)
//! - `batch_link`: AtomicPtr — points to refs-node; on refs-node: RNODE(batch_first)
//! - `refs_or_next`: union — atomic refs counter on refs-node, batch_next on others
//! - `birth_epoch`: epoch at which this node was allocated (on refs-node: min of batch)
//! - `destructor`: type-erased destructor for deallocation

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicPtr, AtomicUsize};

/// Type-erased destructor function
pub(crate) type DestructorFn = unsafe fn(*mut RetiredNode);

/// Sentinel value meaning "slot inactive / node already traversed".
///
/// All-ones, so it is never a valid allocation address at any pointer width.
pub(crate) const INVPTR: usize = !0_usize;

/// REFC_PROTECT bias for reference counting: the top bit of a `usize`.
///
/// `1 << 63` on a 64-bit target, `1 << 31` on a 32-bit one. The value is only
/// ever used as a sign-bit bias (see `guard.rs`, `REFC_PROTECT.wrapping_neg()`),
/// so the remaining `usize::BITS - 1` bits are the usable reference count.
/// Batches are bounded by `RETIRE_FREQ` (64), so 31 bits is ample headroom.
pub(crate) const REFC_PROTECT: usize = 1_usize << (usize::BITS - 1);

/// Width of the `slot_index` field in the packed `(tid, slot_index)` value
/// stored by [`RetiredNode::set_slot_info`].
///
/// The `tid` field gets the remaining `usize::BITS - SLOT_INFO_INDEX_BITS`
/// bits: 48 on a 64-bit target, 16 on a 32-bit one. `slot.rs` carries the
/// static assertions that both fields are wide enough at every supported
/// pointer width.
pub(crate) const SLOT_INFO_INDEX_BITS: u32 = 16;

/// Mask selecting the `slot_index` field of a packed slot-info value.
pub(crate) const SLOT_INFO_INDEX_MASK: usize = (1_usize << SLOT_INFO_INDEX_BITS) - 1;

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
///
/// `birth_epoch` and `destructor` use `UnsafeCell` for interior mutability: these fields
/// are written during `retire()` while other threads may hold guard-protected `&T` references
/// to the outer struct (whose layout starts with this RetiredNode). Without `UnsafeCell`,
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
    /// UnsafeCell: written during retire() while readers may hold &T to the outer struct.
    pub(crate) birth_epoch: UnsafeCell<u64>,

    /// Type-erased destructor — set during retire().
    /// Used by free_list to deallocate each node in a batch.
    /// UnsafeCell: written during retire() while readers may hold &T to the outer struct.
    pub(crate) destructor: UnsafeCell<Option<DestructorFn>>,
}

impl RetiredNode {
    /// Create a new RetiredNode with the current global epoch as birth_epoch.
    ///
    /// The birth_epoch must be set at allocation time (not retirement time)
    /// so that threads pinned before this allocation can be correctly identified
    /// as not needing protection for this node.
    ///
    /// The epoch comes from a thread-local cache (refreshed at every `pin()`)
    /// rather than a direct read of the global counter, avoiding a contended
    /// atomic load on every allocation. The cached value is always ≤ the true
    /// global epoch, so it can only defer reclamation more conservatively,
    /// never prematurely. See `guard::current_birth_epoch`.
    pub fn new() -> Self {
        Self {
            next: AtomicPtr::new(core::ptr::null_mut()),
            batch_link: AtomicPtr::new(core::ptr::null_mut()),
            refs_or_next: AtomicUsize::new(0),
            birth_epoch: UnsafeCell::new(crate::guard::current_birth_epoch()),
            destructor: UnsafeCell::new(None),
        }
    }

    /// Read birth_epoch.
    ///
    /// # Ordering Justification
    ///
    /// This is a non-atomic read of an `UnsafeCell`. It is safe because:
    /// - Writes happen during batch construction (thread-local, no concurrent access).
    /// - Reads happen after publication via `try_retire`, which establishes a
    ///   happens-before chain: `batch_link.store(SeqCst)` -> `slot.exchange(AcqRel)` ->
    ///   `slot.exchange(AcqRel)` -> `birth_epoch` read (same thread).
    /// - The value is never modified after batch finalization.
    #[inline]
    pub(crate) fn birth_epoch(&self) -> u64 {
        unsafe { *self.birth_epoch.get() }
    }

    /// Write birth_epoch (thread-local only, during batch construction).
    #[inline]
    pub(crate) fn set_birth_epoch(&self, epoch: u64) {
        unsafe { *self.birth_epoch.get() = epoch }
    }

    /// Read destructor
    #[inline]
    pub(crate) fn destructor(&self) -> Option<DestructorFn> {
        unsafe { *self.destructor.get() }
    }

    /// Write destructor
    #[inline]
    pub(crate) fn set_destructor(&self, d: Option<DestructorFn>) {
        unsafe { *self.destructor.get() = d }
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
    ///
    /// `slot_index` occupies the low [`SLOT_INFO_INDEX_BITS`] bits and `tid`
    /// the rest. The static assertions in `slot.rs` guarantee both fit at
    /// every supported pointer width.
    #[inline]
    pub(crate) fn set_slot_info(&self, tid: usize, index: usize) {
        debug_assert!(index <= SLOT_INFO_INDEX_MASK, "slot index overflows field");
        debug_assert!(
            tid <= usize::MAX >> SLOT_INFO_INDEX_BITS,
            "tid overflows field"
        );
        let packed = (tid << SLOT_INFO_INDEX_BITS) | index;
        self.next.store(
            packed as *mut RetiredNode,
            core::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Read the (tid, slot_index) packed into `next` during try_retire insert phase.
    #[inline]
    pub(crate) fn get_slot_info(&self) -> (usize, usize) {
        let packed = self.next.load(core::sync::atomic::Ordering::Relaxed) as usize;
        (
            packed >> SLOT_INFO_INDEX_BITS,
            packed & SLOT_INFO_INDEX_MASK,
        )
    }
}

impl Default for RetiredNode {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: RetiredNode contains only raw pointers, atomics, and UnsafeCell fields
// whose access is synchronized by the SMR protocol (retire happens-before reclamation).
unsafe impl Send for RetiredNode {}
unsafe impl Sync for RetiredNode {}
