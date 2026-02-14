//! Retired node structures for batch-based memory reclamation
//!
//! Implements the Hyaline node layout with a union of refs/batch_next.
//! The refs-node (first pushed, tail of batch_next chain) carries the
//! atomic reference counter. All other nodes use the same field as
//! a batch_next pointer.

use core::sync::atomic::AtomicIsize;

/// Type-erased destructor function
pub(crate) type DestructorFn = unsafe fn(*mut RetiredNode);

/// Node structure embedded in user's data structure
///
/// Users must embed this at the start of their node type to enable retirement.
///
/// Layout matches the reference Hyaline lfsmr_node:
/// - `smr_next`: next in slot's retirement list (also reused for free list)
/// - `batch_link`: points to the refs-node (batch tail) for all non-tail nodes;
///   on the refs-node itself, points to the batch front (for freeing)
/// - `refs_or_next`: union — atomic refs counter on the refs-node,
///   batch_next pointer on other nodes. Last node pushed (tail) has this = 0,
///   serving as both null batch_next and initial refs = 0.
/// - `destructor`: type-erased destructor, only meaningful on the refs-node
#[repr(C, align(8))]
pub struct RetiredNode {
    /// Next node in slot's retirement list (reused as free-list link)
    pub(crate) smr_next: *mut RetiredNode,

    /// Points to the refs-node (batch tail) for non-tail nodes.
    /// On the refs-node: points to the batch front (for __lfsmr_free).
    pub(crate) batch_link: *mut RetiredNode,

    /// Union: atomic refs counter (on refs-node) | batch_next pointer (others).
    /// Initialized to 0 on the refs-node (tail), which means both
    /// "no next in batch" and "refs starts at 0".
    pub(crate) refs_or_next: AtomicIsize,

    /// Type-erased destructor — only set on the refs-node (batch tail).
    /// Used by `free_batch_list` to deallocate all nodes in the batch.
    pub(crate) destructor: Option<DestructorFn>,

    /// Birth era for robustness
    #[cfg(feature = "robust")]
    pub(crate) birth_era: u64,
}

impl RetiredNode {
    /// Create a new RetiredNode with null pointers
    pub const fn new() -> Self {
        Self {
            smr_next: core::ptr::null_mut(),
            batch_link: core::ptr::null_mut(),
            refs_or_next: AtomicIsize::new(0),
            destructor: None,
            #[cfg(feature = "robust")]
            birth_era: 0,
        }
    }

    /// Read the batch_next pointer (non-atomic, for batch construction and freeing)
    #[inline]
    pub(crate) fn batch_next(&self) -> *mut RetiredNode {
        // During batch construction and freeing, refs_or_next holds a batch_next pointer.
        // Relaxed is sufficient: single-threaded during construction, and all
        // synchronization is established by the CAS operations during distribution.
        self.refs_or_next
            .load(core::sync::atomic::Ordering::Relaxed) as usize as *mut RetiredNode
    }

    /// Set the batch_next pointer (non-atomic, for batch construction)
    #[inline]
    pub(crate) fn set_batch_next(&self, next: *mut RetiredNode) {
        self.refs_or_next.store(
            next as usize as isize,
            core::sync::atomic::Ordering::Relaxed,
        );
    }
}

impl Default for RetiredNode {
    fn default() -> Self {
        Self::new()
    }
}

impl RetiredNode {
    /// Create a new RetiredNode with birth era
    #[cfg(feature = "robust")]
    pub fn new_with_era(era: u64) -> Self {
        Self {
            smr_next: core::ptr::null_mut(),
            batch_link: core::ptr::null_mut(),
            refs_or_next: AtomicIsize::new(0),
            destructor: None,
            birth_era: era,
        }
    }
}

// SAFETY: RetiredNode contains only raw pointers and atomics which are Send
unsafe impl Send for RetiredNode {}
// SAFETY: RetiredNode synchronization handled by atomic operations
unsafe impl Sync for RetiredNode {}
