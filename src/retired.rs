//! Retired node structures for batch-based memory reclamation

use core::sync::atomic::AtomicIsize;

/// Node structure embedded in user's data structure
///
/// Users must embed this at the start of their node type to enable retirement.
#[repr(C)]
pub struct RetiredNode {
    /// Next node in slot's retirement list
    pub(crate) smr_next: *mut RetiredNode,
    
    /// Next node in batch (for deallocation)
    pub(crate) batch_next: *mut RetiredNode,
    
    /// Pointer to batch's reference counter
    pub(crate) nref_ptr: *mut NRefNode,
}

impl RetiredNode {
    /// Create a new RetiredNode with null pointers
    pub const fn new() -> Self {
        Self {
            smr_next: core::ptr::null_mut(),
            batch_next: core::ptr::null_mut(),
            nref_ptr: core::ptr::null_mut(),
        }
    }
}

// SAFETY: RetiredNode contains only raw pointers which are Send
unsafe impl Send for RetiredNode {}
// SAFETY: RetiredNode synchronization handled by atomic operations
unsafe impl Sync for RetiredNode {}

/// Type-erased destructor function
pub(crate) type DestructorFn = unsafe fn(*mut RetiredNode);

/// Reference counter node shared by all nodes in a batch
#[repr(C)]
pub(crate) struct NRefNode {
    /// Atomic reference counter
    ///
    /// Tracks: ADDEND × k + Σ(threads_entered - threads_left)
    /// When reaches 0, all threads that could see batch have left
    pub(crate) nref: AtomicIsize,
    
    /// Pointer to first node in batch (for deallocation)
    pub(crate) batch_first: *mut RetiredNode,
    
    /// Type-erased destructor for the batch
    pub(crate) destructor: DestructorFn,
}

impl NRefNode {
    /// Create a new NRefNode with destructor
    pub(crate) fn new(batch_first: *mut RetiredNode, destructor: DestructorFn) -> Self {
        Self {
            nref: AtomicIsize::new(0),
            batch_first,
            destructor,
        }
    }
}

/// Internal tracking structure for retired nodes
pub(crate) struct Retired {
    pub(crate) ptr: *mut RetiredNode,
    pub(crate) nref_ptr: *mut NRefNode,
}

// SAFETY: Retired contains only raw pointers which are Send
unsafe impl Send for Retired {}
