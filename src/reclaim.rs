//! Memory reclamation trait and implementation

use crate::retired::{NRefNode, RetiredNode};
use alloc::boxed::Box;

/// Trait for types that can be reclaimed
///
/// Types implementing this trait can be safely retired and reclaimed
/// by the memory reclamation system.
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
            drop(Box::from_raw(ptr));
        }
    }
}


/// Adjust reference count of a batch
///
/// # Safety
///
/// - node_ptr must point to valid RetiredNode
/// - The batch must still be valid
pub(crate) unsafe fn adjust_refs(node_ptr: usize, delta: isize) {
    if node_ptr == 0 {
        return;
    }

    let node = unsafe { &*(node_ptr as *const RetiredNode) };
    
    // Check for null nref_ptr
    if node.nref_ptr.is_null() {
        return;
    }
    
    let nref_node = unsafe { &*node.nref_ptr };

    // Handle increment vs decrement separately to avoid race conditions
    if delta < 0 {
        // Decrement: use fetch_sub and check if we're the one who brought it to zero
        let prev = nref_node.nref.fetch_sub(delta.abs(), core::sync::atomic::Ordering::AcqRel);
        
        // Only free if we were the one who brought it from delta.abs() to 0
        if prev == delta.abs() {
            // SAFETY: NRef reached 0, so all threads that could see batch have left
            unsafe {
                let nref_node = &*node.nref_ptr;
                
                // Call the type-erased destructor to free all nodes in batch
                let destructor = nref_node.destructor;
                let mut curr = nref_node.batch_first;
                
                while !curr.is_null() {
                    let next = (*curr).batch_next;
                    destructor(curr);
                    curr = next;
                }
                
                // Free the NRefNode itself
                drop(Box::from_raw(node.nref_ptr));
            }
        }
    } else {
        // Increment: just add, no free needed
        nref_node.nref.fetch_add(delta, core::sync::atomic::Ordering::AcqRel);
    }
}

/// Traverse retirement list and decrement references
///
/// # Safety
///
/// - start must point to valid RetiredNode or be null
/// - All nodes in list must still be valid
pub(crate) unsafe fn traverse_and_decrement(start: usize, slot: usize) {
    let mut curr = start as *mut RetiredNode;
    let mut count = 0usize;

    while !curr.is_null() {
        let node = unsafe { &*curr };
        
        // Check for null nref_ptr before dereferencing
        if !node.nref_ptr.is_null() {
            let nref_node = unsafe { &*node.nref_ptr };

            // Atomic decrement
            let prev_nref = nref_node.nref.fetch_sub(1, core::sync::atomic::Ordering::AcqRel);
            count += 1;

            // If reaches zero, free entire batch
            if prev_nref == 1 {
                // SAFETY: NRef reached 0, all threads have left
                unsafe {
                    let nref_node = &*node.nref_ptr;
                    
                    // Call the type-erased destructor to free all nodes in batch
                    let destructor = nref_node.destructor;
                    let mut curr = nref_node.batch_first;
                    
                    while !curr.is_null() {
                        let next = (*curr).batch_next;
                        destructor(curr);
                        curr = next;
                    }
                    
                    // Free the NRefNode itself
                    drop(Box::from_raw(node.nref_ptr as *mut NRefNode));
                }
            }
        }

        // Move to next node in list
        curr = node.smr_next;
    }
    
    // Decrement ack counter for robustness
    #[cfg(feature = "robust")]
    if count > 0 {
        let global = crate::slot::global();
        global.slot(slot).ack_counter.fetch_sub(count as isize, core::sync::atomic::Ordering::Relaxed);
    }
}
