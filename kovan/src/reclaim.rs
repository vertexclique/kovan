//! Memory reclamation: adjust_refs, traverse, and deferred batch freeing.
//!
//! Matches the reference Hyaline (lfsmr) implementation:
//! - `adjust_refs`: follows batch_link to refs-node, fetch_add, deferred free
//! - `traverse_and_decrement`: do-while from next to end (inclusive), deferred free
//! - `free_batch_list`: walks deferred free list, frees all nodes per batch

use crate::retired::RetiredNode;
use core::sync::atomic::Ordering;

/// Trait for types that can be reclaimed
///
/// Types implementing this trait can be safely retired and reclaimed
/// by the memory reclamation system.
/// # Safety
/// Implementors must ensure that `reclaim` is safe to call when the object is no longer reachable.
pub unsafe trait Reclaimable: Sized {
    /// Get a reference to the embedded RetiredNode
    fn retired_node(&self) -> &RetiredNode;

    /// Get a mutable reference to the embedded RetiredNode
    fn retired_node_mut(&mut self) -> &mut RetiredNode;

    /// Deallocate this node
    ///
    /// # Safety
    ///
    /// This must only be called once, when the node is no longer accessible
    unsafe fn dealloc(ptr: *mut Self) {
        // Default implementation: drop via Box
        // SAFETY: Caller guarantees ptr is valid and this is called once
        unsafe {
            drop(alloc::boxed::Box::from_raw(ptr));
        }
    }
}

/// Adjust reference count of a batch (matches `__lfsmr_adjust_refs`).
///
/// Follows `node_ptr -> batch_link` to find the refs-node, then atomically
/// adds `delta` to the refs counter. If refs reaches 0 (old value was `-delta`),
/// adds the refs-node to the deferred free list.
///
/// # Safety
///
/// - `node_ptr` must point to a valid RetiredNode with valid `batch_link`
/// - `free_list` must be a valid pointer to a free-list head
pub(crate) unsafe fn adjust_refs(node_ptr: usize, delta: isize, free_list: &mut *mut RetiredNode) {
    if node_ptr == 0 {
        return;
    }

    let node = unsafe { &*(node_ptr as *const RetiredNode) };
    let refs_node_ptr = node.batch_link;
    let refs_node = unsafe { &*refs_node_ptr };

    // Atomic add; if old value was -delta, refs just reached 0
    let old = refs_node.refs_or_next.fetch_add(delta, Ordering::AcqRel);
    if old == -delta {
        // Batch is ready for freeing — add refs-node to deferred free list.
        // SAFETY: refs reached 0 means no thread can access any batch node.
        // Reuse smr_next for the free-list chain.
        unsafe {
            (*refs_node_ptr).smr_next = *free_list;
        }
        *free_list = refs_node_ptr;
    }
}

/// Traverse retirement list and decrement references (matches `__lfsmr_traverse`).
///
/// Do-while loop from `start` to `end` (inclusive). For each node, follows
/// `batch_link` to the refs-node and decrements refs by 1. If refs reaches 0,
/// adds the refs-node to the deferred free list.
///
/// # Safety
///
/// - `start` and `end` must be valid RetiredNode pointers or 0
/// - Nodes in the range must have valid `batch_link` pointers
/// - `free_list` must be a valid pointer to a free-list head
#[allow(unused_variables)]
pub(crate) unsafe fn traverse_and_decrement(
    start: usize,
    end: usize,
    slot: usize,
    free_list: &mut *mut RetiredNode,
) {
    let mut next = start;
    #[cfg(feature = "robust")]
    let mut count = 0usize;

    // do-while: process curr, then check if curr == end
    loop {
        let curr = next;
        if curr == 0 {
            break;
        }

        let node = unsafe { &*(curr as *const RetiredNode) };
        // Save next BEFORE any potential modification
        next = node.smr_next as usize;

        // Follow batch_link to refs-node
        let refs_node_ptr = node.batch_link;
        let refs_node = unsafe { &*refs_node_ptr };

        // Atomic decrement
        let old = refs_node.refs_or_next.fetch_sub(1, Ordering::AcqRel);
        #[cfg(feature = "robust")]
        {
            count += 1;
        }

        // If old was 1, refs just reached 0 — batch ready for freeing
        if old == 1 {
            // SAFETY: refs reached 0, no thread can access any batch node
            unsafe {
                (*refs_node_ptr).smr_next = *free_list;
            }
            *free_list = refs_node_ptr;
        }

        // Inclusive end: stop after processing `end`
        if curr == end {
            break;
        }
    }

    // Decrement ack counter for robustness
    #[cfg(feature = "robust")]
    if count > 0 {
        let global = crate::slot::global();
        global
            .slot(slot)
            .ack_counter
            .fetch_sub(count as isize, Ordering::Relaxed);
    }
}

/// Free all batches in the deferred free list (matches `__lfsmr_free`).
///
/// Each entry in the list is a refs-node (batch tail). Its `batch_link`
/// points to the batch front. We walk the batch_next chain from front
/// to tail, calling the type-erased destructor on each node.
///
/// # Safety
///
/// - All refs-nodes in the list must have refs == 0
/// - The destructor on each refs-node must be valid
/// - Each node must be a valid allocation that can be freed by the destructor
pub(crate) unsafe fn free_batch_list(mut list: *mut RetiredNode) {
    while !list.is_null() {
        let refs_node = unsafe { &*list };
        // batch_link on refs-node points to batch front
        let front = refs_node.batch_link;
        // smr_next is reused for free-list chain — save before freeing
        list = refs_node.smr_next;

        // Walk the batch_next chain: front → ... → tail (batch_next = null)
        // Each node carries its own destructor (set during retire).
        let mut curr = front;
        while !curr.is_null() {
            let node = unsafe { &*curr };
            // Read batch_next and destructor BEFORE freeing the node
            let next = node.batch_next();
            let destructor = node.destructor;
            if let Some(d) = destructor {
                unsafe {
                    d(curr);
                }
            }
            curr = next;
        }
    }
}
