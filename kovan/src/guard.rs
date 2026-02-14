//! Guard and Handle for critical section management
//!
//! Implements the Hyaline enter/leave/retire protocol matching the reference
//! lfsmr CAS2 implementation. Key differences from the previous version:
//! - Leave uses a CAS loop (zeroes slot when last thread out)
//! - Traverse direction: from current_head->next to handle_ptr (inclusive)
//! - Batch size = num_slots + 1 (refs-node never inserted into a slot)
//! - No separate NRefNode allocation — refs embedded in batch tail node
//! - Deferred freeing via local free list

use crate::retired::RetiredNode;
use crate::slot::{GlobalState, global};
use alloc::boxed::Box;
use core::cell::Cell;

/// RAII guard representing an active critical section
///
/// While a Guard exists, the thread is considered "active" in its slot,
/// and any `Shared<'g, T>` pointers are guaranteed to remain valid.
pub struct Guard {
    slot: usize,
    handle_ptr: usize,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let global = global();
        let slot = global.slot(self.slot);
        let addend = global.addend();
        let mut free_list: *mut RetiredNode = core::ptr::null_mut();

        // === Leave: CAS loop (matches __lfsmr_leave for CAS2) ===
        //
        // Atomically decrement ref count. If refs reaches 0, zero the entire
        // slot (both refs and pointer) to detach the retirement list.

        let mut curr: usize;
        let mut next: usize = 0;

        loop {
            let (refs, ptr) = slot.head.load();
            curr = ptr;

            if curr != self.handle_ptr && curr != 0 {
                // List changed since we entered — read next for traverse
                let node = unsafe { &*(curr as *const RetiredNode) };
                next = node.smr_next as usize;
            }

            let new_refs = refs - 1;
            let (cas_new_refs, cas_new_ptr) = if new_refs == 0 {
                (0u64, 0usize) // Zero everything when last thread out
            } else {
                (new_refs, ptr) // Keep pointer, just decrement refs
            };

            match slot
                .head
                .compare_exchange(refs, ptr, cas_new_refs, cas_new_ptr)
            {
                Ok(_) => {
                    // CAS succeeded. If we zeroed and old list was non-empty,
                    // give this slot's addend contribution to the head batch.
                    let start = ptr; // pointer from the old value
                    if new_refs == 0 && start != 0 {
                        unsafe {
                            crate::reclaim::adjust_refs(start, addend, &mut free_list);
                        }
                    }
                    break;
                }
                Err(_) => continue, // Retry
            }
        }

        // === Traverse: walk new nodes added during our critical section ===
        //
        // If the list changed (curr != handle_ptr), traverse from
        // curr->next to handle_ptr (inclusive), decrementing each
        // node's batch refs by 1.

        if curr != self.handle_ptr && curr != 0 {
            unsafe {
                crate::reclaim::traverse_and_decrement(
                    next,
                    self.handle_ptr,
                    self.slot,
                    &mut free_list,
                );
            }
        }

        // === Free any batches whose refs reached 0 ===
        if !free_list.is_null() {
            unsafe {
                crate::reclaim::free_batch_list(free_list);
            }
        }
    }
}

/// Thread-local handle for batch accumulation
///
/// Batch construction matches the reference `lfsmr_retire`:
/// - First node pushed becomes `batch_last` (tail = refs-node, batch_next=0, refs=0)
/// - Subsequent nodes get `batch_link = batch_last`
/// - On finalize: `batch_last.batch_link = batch_first` (for freeing)
/// - Threshold: num_slots + 1 (refs-node + one node per slot)
struct Handle {
    global: Cell<Option<&'static GlobalState>>,
    slot: Cell<usize>,
    /// Front of batch chain (most recently pushed)
    batch_first: Cell<*mut RetiredNode>,
    /// Tail of batch chain (first pushed = refs-node, carries atomic refs)
    batch_last: Cell<*mut RetiredNode>,
    /// Number of nodes in current batch
    batch_count: Cell<usize>,
    pin_count: Cell<usize>,
}

impl Handle {
    const fn new() -> Self {
        Self {
            global: Cell::new(None),
            slot: Cell::new(0),
            batch_first: Cell::new(core::ptr::null_mut()),
            batch_last: Cell::new(core::ptr::null_mut()),
            batch_count: Cell::new(0),
            pin_count: Cell::new(0),
        }
    }

    /// Get cached global state reference
    #[inline]
    fn global(&self) -> &'static GlobalState {
        match self.global.get() {
            Some(g) => g,
            None => {
                let g = global();
                self.global.set(Some(g));
                g
            }
        }
    }

    /// Select a slot, avoiding stalled ones if robust feature enabled
    fn select_slot(&self) -> usize {
        use core::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        let global = self.global();
        let start_slot = COUNTER.fetch_add(1, Ordering::Relaxed) % global.num_slots();

        #[cfg(feature = "robust")]
        {
            // Try to find non-stalled slot
            let mut slot = start_slot;
            for _ in 0..global.num_slots() {
                let slot_ref = global.slot(slot);
                let ack = slot_ref.ack_counter.load(Ordering::Relaxed);
                if ack <= crate::robust::STALL_THRESHOLD {
                    return slot;
                }
                slot = (slot + 1) % global.num_slots();
            }
        }

        start_slot
    }

    fn pin(&self) -> Guard {
        let pin_count = self.pin_count.get();
        if pin_count & 127 == 0 {
            let slot = self.select_slot();
            self.slot.set(slot);
        }
        self.pin_count.set(pin_count.wrapping_add(1));

        let slot = self.slot.get();
        let global = self.global();

        #[cfg(feature = "robust")]
        {
            if pin_count & 63 == 0 {
                let current_era = crate::robust::current_era();
                global
                    .slot(slot)
                    .access_era
                    .store(current_era, core::sync::atomic::Ordering::Relaxed);
            }
        }

        // Atomic: increment ref count, get list snapshot
        let (_, handle_ptr) = global.slot(slot).head.fetch_add_ref();

        Guard { slot, handle_ptr }
    }

    /// Retire a node into the thread-local batch (matches `lfsmr_retire`).
    ///
    /// Batch construction:
    /// - First node pushed → `batch_last` (tail, refs-node, batch_next=0/refs=0)
    /// - Subsequent nodes → `batch_link = batch_last`, `batch_next = batch_first`
    /// - When batch reaches threshold (num_slots + 1): finalize and flush
    fn retire<T>(&self, ptr: *mut T)
    where
        T: 'static,
    {
        let node_ptr = ptr as *mut RetiredNode;
        let node = unsafe { &mut *node_ptr };

        // Set per-node destructor (matches reference: node->callback = smr->callback)
        // Each node carries its own destructor because different types may be
        // mixed in the same batch (e.g. AtomNode<T> and DeallocNode<T>).
        unsafe fn destructor<T>(ptr: *mut RetiredNode) {
            let typed_ptr = ptr as *mut T;
            // SAFETY: Caller guarantees ptr is valid and this is called once
            unsafe {
                drop(Box::from_raw(typed_ptr));
            }
        }
        node.destructor = Some(destructor::<T>);

        let first = self.batch_first.get();
        if first.is_null() {
            // First node in batch → becomes batch_last (tail / refs-node)
            self.batch_last.set(node_ptr);
        } else {
            // Subsequent nodes: batch_link = batch_last (refs-node)
            node.batch_link = self.batch_last.get();
        }

        // Implicitly initializes refs to 0 for first node (batch_next = null = 0)
        node.set_batch_next(first);
        self.batch_first.set(node_ptr);

        let count = self.batch_count.get() + 1;
        self.batch_count.set(count);

        let global = self.global();
        if count >= global.num_slots() + 1 {
            // Finalize: set tail's batch_link to front (for free_batch_list)
            let last = self.batch_last.get();
            unsafe {
                (*last).batch_link = self.batch_first.get();
            }

            self.flush_batch();

            // Reset batch state
            self.batch_first.set(core::ptr::null_mut());
            self.batch_last.set(core::ptr::null_mut());
            self.batch_count.set(0);
        }
    }

    /// Distribute batch nodes across all slots (matches `__lfsmr_retire`).
    ///
    /// Iterates through all k slots. For each active slot, CAS-inserts the
    /// current batch node as the new head. For empty slots, accumulates
    /// the addend adjustment. At the end, applies accumulated adjustments
    /// to the batch's refs-node via the first node.
    fn flush_batch(&self) {
        let global = self.global();
        let num_slots = global.num_slots();
        let addend = global.addend();

        let mut curr = self.batch_first.get();
        let mut adjs: isize = 0;
        let mut do_adjs = false;
        let mut free_list: *mut RetiredNode = core::ptr::null_mut();

        let first = self.batch_first.get();

        for i in 0..num_slots {
            let node = unsafe { &*curr };
            let slot = global.slot(i);

            // CAS loop to insert node into this slot
            loop {
                let (refs, list_ptr) = slot.head.load();

                if refs == 0 {
                    // Slot is empty — accumulate addend, don't advance curr
                    do_adjs = true;
                    adjs = adjs.wrapping_add(addend);
                    break;
                }

                // Link node into slot's retirement list
                unsafe {
                    (*curr).smr_next = list_ptr as *mut RetiredNode;
                }

                // CAS: keep refs, change pointer to our node
                match slot
                    .head
                    .compare_exchange(refs, list_ptr, refs, curr as usize)
                {
                    Ok(_) => {
                        // Increment ack counter for robustness
                        #[cfg(feature = "robust")]
                        {
                            slot.ack_counter.fetch_add(refs as isize, Ordering::Relaxed);
                        }

                        // Adjust predecessor's batch refs
                        if list_ptr != 0 {
                            unsafe {
                                crate::reclaim::adjust_refs(
                                    list_ptr,
                                    addend.wrapping_add(refs as isize),
                                    &mut free_list,
                                );
                            }
                        }

                        // Advance to next batch node
                        curr = node.batch_next();
                        break;
                    }
                    Err(_) => continue, // Retry CAS
                }
            }
        }

        // Apply accumulated adjustments for empty slots to our batch
        if do_adjs {
            unsafe {
                crate::reclaim::adjust_refs(first as usize, adjs, &mut free_list);
            }
        }

        // Free any batches whose refs reached 0 during this retire
        if !free_list.is_null() {
            unsafe {
                crate::reclaim::free_batch_list(free_list);
            }
        }
    }
}

// Use nightly thread_local for better performance when available
#[cfg(feature = "nightly")]
#[thread_local]
static HANDLE: Handle = Handle::new();

// Fall back to stable thread_local! macro
#[cfg(not(feature = "nightly"))]
thread_local! {
    static HANDLE: Handle = const { Handle::new() };
}

/// Enter a critical section
///
/// Returns a `Guard` that represents the active critical section.
/// While the guard exists, any `Shared<'g, T>` pointers loaded are
/// guaranteed to remain valid.
///
/// # Examples
///
/// ```rust
/// use kovan::pin;
///
/// let guard = pin();
/// // Access lock-free data structures safely
/// drop(guard);
/// ```
#[inline]
pub fn pin() -> Guard {
    #[cfg(feature = "nightly")]
    {
        HANDLE.pin()
    }
    #[cfg(not(feature = "nightly"))]
    {
        HANDLE.with(|handle| handle.pin())
    }
}

/// Retire a node for later reclamation
///
/// The node will be added to the local batch and eventually distributed
/// across all active slots for safe reclamation.
///
/// # Safety
///
/// The pointer must point to a valid allocation that will not be
/// accessed after this call (except through the reclamation system).
#[inline]
pub fn retire<T: 'static>(ptr: *mut T) {
    #[cfg(feature = "nightly")]
    {
        HANDLE.retire(ptr)
    }
    #[cfg(not(feature = "nightly"))]
    {
        HANDLE.with(|handle| handle.retire(ptr))
    }
}
