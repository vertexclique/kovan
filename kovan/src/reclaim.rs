//! Memory reclamation: Crystalline (WFR) traverse and free_list.
//!
//! - `traverse`: exchange-based list walk with INVPTR sentinel
//! - `free_list`: walks batch chain and calls per-node destructors

use crate::retired::{INVPTR, RetiredNode, is_rnode, rnode_unmask};
use core::sync::atomic::Ordering;

/// Traversals whose zero-count batches the free-list cache holds before it
/// is freed (see `Handle::traverse_into_cache`).
pub(crate) const MAX_CACHE: usize = 12;

/// Trait for types that can be reclaimed by the wait-free memory
/// reclamation system.
///
/// Retire a `Reclaimable` node with [`retire_reclaimable`](crate::retire_reclaimable):
/// once no thread can reach it, kovan calls [`dealloc`](Reclaimable::dealloc)
/// on it. The default `dealloc` frees the node's `Box`; overriding it lets a
/// structure recycle retired nodes (see [Recycling nodes](#recycling-nodes)).
/// [`retire`](crate::retire) ignores `dealloc` and always frees the `Box`.
///
/// # Safety
///
/// Implementors must guarantee:
/// - [`RetiredNode`] is the **first field** at offset 0.
/// - The type is `#[repr(C)]` to ensure deterministic field layout.
/// - `dealloc` is safe to call when the object is no longer reachable
///   by any thread (i.e. all guards that could reference it have been dropped).
///
/// # Recycling nodes
///
/// A structure that publishes one node per update (a ring of slots, each
/// overwritten on every send) can keep its retired nodes in a free list and
/// reuse them, so the steady state allocates nothing. Override `dealloc` to
/// drop the node's value in place and push the node on that list, retire
/// with [`retire_reclaimable`](crate::retire_reclaimable), and take nodes
/// back from the list when publishing.
///
/// ## What kovan guarantees
///
/// - `dealloc` runs at most once per `retire_reclaimable` call, and only
///   after the caller unlinked the node and every guard that could have
///   loaded it was dropped: a batch is freed only once every reservation
///   that covered it has moved past it. It runs exactly once unless the
///   process ends first, or the node is retired during teardown with no
///   thread handle left to take it: that node is leaked and never reaches
///   `dealloc`.
/// - Once `dealloc` is entered, kovan never reads or writes the node again.
///   Every field kovan needs from it (the next node of the batch, the
///   destructor) is read before `dealloc` runs. `dealloc` owns the whole
///   allocation, the embedded [`RetiredNode`] included, from its first
///   instruction, and may hand it to another thread at once.
/// - A pointer loaded under a guard keeps its incarnation for as long as the
///   guard lives: the address cannot be recycled meanwhile. A compare-exchange
///   on a pointer loaded and compared under one guard is therefore free of
///   ABA; a pointer kept across guards is not.
///
/// ## What `dealloc` must do
///
/// - Drop the value in place. kovan drops nothing of a recycled node.
/// - Push the node on a free list that is safe to use from any thread.
///   `dealloc` runs on whichever thread frees the batch (the retiring thread,
///   a reader, an exiting thread), from inside any kovan call that
///   reclaims: `retire`, `pin`, `flush`, a guard drop, thread exit.
/// - Not panic. A panic is caught and discarded under `std` and the node is
///   leaked; without `std` it unwinds out of kovan's reclamation code and
///   leaks the rest of the batch.
/// - Keep the free list's critical sections to the push and the pop. Never
///   hold its lock across a call into kovan: that call may run `dealloc` on
///   the same thread, which would then wait on a lock it already holds.
/// - Rely on a pool that outlives every node pointing to it: the free list
///   must stay valid until the last such node was passed to `dealloc`. A
///   thread's reservation releases the nodes it holds when its outermost
///   guard drops, and a thread's partial batch is submitted by its flush or
///   exit, so an owner that can drop the pool must first have every thread
///   drop its guards and flush or exit.
///
/// ## Before publishing a recycled node again
///
/// After taking a node from the free list, and before storing it in any
/// [`Atomic`](crate::Atomic) or otherwise making it reachable, re-initialise
/// the embedded header:
///
/// ```text
/// ptr::write(&raw mut (*node).retired, RetiredNode::new());
/// ```
///
/// This is a write to a node you own: no other thread can reach it until you
/// publish it.
///
/// The write is required. Retiring a node reads its `birth_epoch`: a batch
/// takes the minimum over its nodes, and the node that heads a batch is
/// lowered to that minimum in place. `dealloc` owns the whole allocation, the
/// embedded `RetiredNode` included, so a pool that keeps its own free-list
/// link in the header, or anything else that writes the header between
/// `dealloc` and the next publication, can leave `birth_epoch` holding an
/// arbitrary value. If that value is later than the epoch the node is
/// published at, kovan can free a batch while a reader still holds one of its
/// nodes: the reclamation model's `RC_mut_stale_link_birth` configuration
/// finds exactly that use after free, with a pool link written over the
/// `birth_epoch` word.
///
/// `RetiredNode::new()` stamps the epoch of a fresh allocation from the
/// thread's cached epoch. It is read before the node is stored, so it is
/// never later than the epoch a reader publishes before it loads the node,
/// the invariant kovan's protection rests on. A reader of an earlier
/// incarnation cannot be confused by the new one: that incarnation's
/// `dealloc` ran only after the reader's guard dropped.
///
/// ```
/// use kovan::{Atomic, Reclaimable, RetiredNode, Shared, pin, retire_reclaimable};
/// use std::mem::MaybeUninit;
/// use std::ptr;
/// use std::sync::{Mutex, PoisonError};
/// use std::sync::atomic::Ordering;
///
/// /// Nodes whose value was dropped, waiting to be reused.
/// struct Pool(Mutex<Vec<*mut Node>>);
///
/// // SAFETY: a pooled node is reachable only through the pool.
/// unsafe impl Send for Pool {}
/// unsafe impl Sync for Pool {}
///
/// impl Pool {
///     fn take(&self) -> Option<*mut Node> {
///         self.0.lock().unwrap_or_else(PoisonError::into_inner).pop()
///     }
///
///     fn put(&self, node: *mut Node) {
///         self.0.lock().unwrap_or_else(PoisonError::into_inner).push(node);
///     }
/// }
///
/// static POOL: Pool = Pool(Mutex::new(Vec::new()));
///
/// #[repr(C)]
/// struct Node {
///     retired: RetiredNode, // first field, as `Reclaimable` requires
///     value: MaybeUninit<String>, // initialised while published
///     pool: &'static Pool,
/// }
///
/// impl Node {
///     /// A node holding `value`: recycled if the pool has one.
///     fn new(value: String) -> *mut Node {
///         match POOL.take() {
///             Some(node) => unsafe {
///                 // Re-arm the header before the node can be published.
///                 ptr::write(&raw mut (*node).retired, RetiredNode::new());
///                 (*node).value.write(value);
///                 node
///             },
///             None => Box::into_raw(Box::new(Node {
///                 retired: RetiredNode::new(),
///                 value: MaybeUninit::new(value),
///                 pool: &POOL,
///             })),
///         }
///     }
/// }
///
/// // SAFETY: `RetiredNode` is the first field of a `#[repr(C)]` struct.
/// unsafe impl Reclaimable for Node {
///     fn retired_node(&self) -> &RetiredNode {
///         &self.retired
///     }
///
///     fn retired_node_mut(&mut self) -> &mut RetiredNode {
///         &mut self.retired
///     }
///
///     unsafe fn dealloc(node: *mut Self) {
///         // SAFETY: no guard reaches the node and kovan never touches it
///         // again, so it is ours to drop and to hand to the pool.
///         unsafe {
///             ptr::drop_in_place((*node).value.as_mut_ptr());
///             (*node).pool.put(node);
///         }
///     }
/// }
///
/// let slot = Atomic::new(Node::new("first".to_string()));
/// for i in 0..4 {
///     let guard = pin();
///     let new = unsafe { Shared::from_raw(Node::new(format!("v{i}"))) };
///     let old = slot.swap(new, Ordering::AcqRel, &guard);
///     // SAFETY: `old` is unlinked from `slot` and retired once.
///     unsafe { retire_reclaimable(old.as_raw()) };
/// }
/// kovan::flush();
///
/// // Nothing else was pinned, so the retired nodes are back in the pool and
/// // the next node is one of them.
/// assert!(!POOL.0.lock().unwrap().is_empty());
/// let reused = Node::new("again".to_string());
/// assert_eq!(unsafe { (*reused).value.assume_init_ref() }, "again");
/// ```
pub unsafe trait Reclaimable: Sized {
    /// Get a reference to the embedded RetiredNode
    fn retired_node(&self) -> &RetiredNode;

    /// Get a mutable reference to the embedded RetiredNode
    fn retired_node_mut(&mut self) -> &mut RetiredNode;

    /// Deallocate this node: free it, or recycle it into a free list.
    ///
    /// Called by kovan, once, for a node retired with
    /// [`retire_reclaimable`](crate::retire_reclaimable), on any thread. The
    /// default drops the node's `Box`. An override must uphold the contract
    /// in [Recycling nodes](Reclaimable#recycling-nodes): it must not panic,
    /// and kovan does not touch the node after this is entered.
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
/// # Progress: wait-free; bound proportional to retirement history
///
/// The list is captured atomically by the caller's exchange, so its length
/// is fixed before traversal begins and the loop terminates in finitely
/// many steps: an insert into the slot after the capture starts a new list,
/// and an entry whose inserter has not linked the rest behind it yet ends
/// the walk (its `next` is still null; that inserter walks the rest). The
/// length is NOT bounded by the thread count: each
/// `try_retire()` call system-wide may insert one node into this slot
/// while the slot is active, and a slot is traversed when its owner leaves
/// it (the outermost guard's drop, the exit) or enters it (the outermost
/// `pin()`, which takes what a retire exchanged in after the leave). A
/// thread therefore owes a traversal proportional to the batches retired
/// during one of its critical sections, never to an idle period: an
/// inactive slot takes no node.
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
        // to this thread. The batch_link was written before the slot insertion
        // (sequenced before the inserting exchange's release), which
        // happens-before current thread's slot extraction.
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
    // One pass over the batches of the list, and over each batch's nodes:
    // the list is the caller's, taken out of any cell others reach.
    while !list.is_null() {
        let refs_node = list;
        // batch_link on refs-node is RNODE(batch_front)
        let batch_link = unsafe { (*refs_node).batch_link.load(Ordering::Relaxed) };
        let front = rnode_unmask(batch_link);
        // next is reused for free-list chain: save before freeing
        list = unsafe { (*refs_node).next.load(Ordering::Relaxed) };

        // Walk the batch_next chain: front -> ... -> refs_node (batch_next = null)
        let mut curr = front;
        while !curr.is_null() {
            let node = unsafe { &*curr };
            // Everything kovan needs from the node is read here, before its
            // destructor runs. The destructor owns the allocation from its
            // first instruction (a recycling `Reclaimable::dealloc` hands
            // it to another thread at once), so nothing below may touch
            // `curr`'s memory again: only the locals read here.
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
