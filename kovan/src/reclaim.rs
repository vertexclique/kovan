//! Memory reclamation: Crystalline (WFR) traverse and free_list.
//!
//! - `traverse`: exchange-based list walk with INVPTR sentinel
//! - `traverse_cache`: cached traverse with periodic free_list drain
//! - `free_list`: walks batch chain and calls per-node destructors

use crate::retired::{INVPTR, RetiredNode, is_rnode, rnode_unmask};
use core::sync::atomic::Ordering;

/// Maximum cached free-list entries before draining
const MAX_CACHE: usize = 12;

/// Trait for types that can be reclaimed by the wait-free memory
/// reclamation system.
///
/// # Safety
///
/// Implementors must guarantee:
/// - [`RetiredNode`] is the **first field** at offset 0.
/// - The type is `#[repr(C)]` to ensure deterministic field layout.
/// - `dealloc` is safe to call when the object is no longer reachable
///   by any thread (i.e. all guards that could reference it have been dropped).
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
        unsafe {
            drop(alloc::boxed::Box::from_raw(ptr));
        }
    }
}

/// Get the refs-node for a given node.
/// If the node's batch_link has the RNODE bit set, the node itself is the refs-node.
/// Otherwise, batch_link points to the refs-node.
#[inline]
pub(crate) unsafe fn get_refs_node(node: *mut RetiredNode) -> *mut RetiredNode {
    let refs = unsafe { (*node).batch_link.load(Ordering::Acquire) };
    if is_rnode(refs) { node } else { refs }
}

/// Traverse a slot's retirement list, decrementing refs for each node.
///
/// Walks the list following `next` pointers. For each node:
/// - Exchanges `next` with INVPTR (prevents double-traverse by concurrent helpers)
/// - If RNODE: terminal refs-node, fetch_sub(1) on refs
/// - Otherwise: follow batch_link to refs-node, fetch_sub(1)
/// - If refs reaches 0: add refs-node to free list
///
/// # Wait-free bound: O(T) where T = number of active threads
///
/// Each slot receives at most one node per `try_retire()` call. Between two
/// `do_update()` calls on the same slot, at most T `try_retire()`
/// calls can insert nodes (one per thread). The list length — and thus the
/// number of loop iterations — is bounded by T.
///
/// # Safety
///
/// `next` must be a valid RetiredNode pointer (not null, not INVPTR)
/// or a valid RNODE-marked pointer.
pub(crate) unsafe fn traverse(free_list: &mut *mut RetiredNode, mut next: *mut RetiredNode) {
    loop {
        let curr = next;
        if curr.is_null() {
            break;
        }
        if is_rnode(curr) {
            // Terminal refs-node case
            let refs = rnode_unmask(curr);
            let old = unsafe { (*refs).refs_or_next.fetch_sub(1, Ordering::AcqRel) };
            if old == 1 {
                unsafe {
                    (*refs).next.store(*free_list, Ordering::Relaxed);
                }
                *free_list = refs;
            }
            break;
        }
        // Swap next with INVPTR to claim this node
        next = unsafe {
            (*curr)
                .next
                .swap(INVPTR as *mut RetiredNode, Ordering::AcqRel)
        };
        // Follow batch_link to refs-node and decrement.
        // Ordering: Relaxed is sufficient because the happens-before chain is
        // established through the slot exchange (AcqRel) that delivered current slot
        // to this thread. The batch_link was written (SeqCst) before the slot
        // insertion, which happens-before current thread's slot extraction. shrug.
        let refs = unsafe { (*curr).batch_link.load(Ordering::Relaxed) };
        let old = unsafe { (*refs).refs_or_next.fetch_sub(1, Ordering::AcqRel) };
        if old == 1 {
            unsafe {
                (*refs).next.store(*free_list, Ordering::Relaxed);
            }
            *free_list = refs;
        }
    }
}

/// Traverse with caching: accumulates free-list entries and periodically drains.
///
/// # Safety
///
/// Same as `traverse`.
pub(crate) unsafe fn traverse_cache(
    free_list: &mut *mut RetiredNode,
    list_count: &mut usize,
    next: *mut RetiredNode,
) {
    if !next.is_null() {
        if *list_count >= MAX_CACHE {
            unsafe { free_batch_list(*free_list) };
            *free_list = core::ptr::null_mut();
            *list_count = 0;
        }
        unsafe { traverse(free_list, next) };
        *list_count += 1;
    }
}

/// Free all batches in the deferred free list.
///
/// Each entry in the list is a refs-node. Its `batch_link` is RNODE(batch_front).
/// We unmask to get batch_front, then walk the batch_next chain from front,
/// calling the type-erased destructor on each node.
///
/// # Safety
///
/// All refs-nodes in the list must have refs == 0.
pub(crate) unsafe fn free_batch_list(mut list: *mut RetiredNode) {
    while !list.is_null() {
        let refs_node = list;
        // batch_link on refs-node is RNODE(batch_front)
        let batch_link = unsafe { (*refs_node).batch_link.load(Ordering::Relaxed) };
        let front = rnode_unmask(batch_link);
        // next is reused for free-list chain — save before freeing
        list = unsafe { (*refs_node).next.load(Ordering::Relaxed) };

        // Walk the batch_next chain: front -> ... -> refs_node (batch_next = null)
        let mut curr = front;
        while !curr.is_null() {
            let node = unsafe { &*curr };
            let next = node.batch_next();
            let destructor = node.destructor();
            if let Some(d) = destructor {
                // If the destructor panics, continue freeing remaining nodes
                // to prevent memory leaks. The panic payload is discarded.
                // On no_std, panicking destructors will leak remaining nodes.
                #[cfg(feature = "std")]
                {
                    let _result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        unsafe { d(curr) };
                    }));
                }
                #[cfg(not(feature = "std"))]
                {
                    unsafe { d(curr) };
                }
            }
            curr = next;
        }
    }
}
