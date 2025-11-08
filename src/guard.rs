//! Guard and Handle for critical section management

use crate::retired::{NRefNode, Retired, RetiredNode};
use crate::slot::{calculate_adjustment, global};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::sync::atomic::Ordering;

/// Batch size threshold for flushing
///
/// Must be ≥ k+1 where k is number of slots (64)
const BATCH_SIZE: usize = 65;

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

        // Phase 1: Decrement reference count
        let (new_refs, current_list) = slot.head.fetch_sub_ref();

        // Phase 2a: If last thread out and list non-empty, adjust predecessor
        if new_refs == 0 && current_list != 0 {
            let addend = calculate_adjustment(global.slot_order());
            // SAFETY: current_list is valid from slot head
            unsafe {
                adjust_refs(current_list, addend);
            }
        }

        // Phase 2b: Traverse from entry point and decrement references
        if self.handle_ptr != 0 {
            // SAFETY: handle_ptr is valid from slot head at entry time
            unsafe {
                traverse_and_decrement(self.handle_ptr, self.slot);
            }
        }
    }
}

/// Thread-local handle for batch accumulation
struct Handle {
    slot: Cell<usize>,
    batch: RefCell<Vec<Retired>>,
    batch_nref: Cell<*mut NRefNode>,
}

impl Handle {
    fn new() -> Self {
        // Adaptive slot selection (avoid stalled slots in robust mode)
        let slot = Self::select_slot();

        Self {
            slot: Cell::new(slot),
            batch: RefCell::new(Vec::with_capacity(BATCH_SIZE)),
            batch_nref: Cell::new(core::ptr::null_mut()),
        }
    }
    
    /// Select a slot, avoiding stalled ones if robust feature enabled
    fn select_slot() -> usize {
        use core::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        
        let global = global();
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
        let slot = self.slot.get();
        let global = global();

        // Touch era for robustness
        #[cfg(feature = "robust")]
        {
            let current_era = crate::robust::current_era();
            global.slot(slot).access_era.store(current_era, core::sync::atomic::Ordering::Relaxed);
        }

        // Atomic: increment ref count, get list snapshot
        let (_, handle_ptr) = global.slot(slot).head.fetch_add_ref();

        Guard { slot, handle_ptr }
    }

    fn retire<T>(&self, ptr: *mut T) 
    where
        T: 'static,
    {
        let retired_ptr = ptr as *mut RetiredNode;
        let mut batch = self.batch.borrow_mut();

        // Initialize batch NRefNode on first retire
        if batch.is_empty() {
            // Create type-erased destructor
            unsafe fn destructor<T>(ptr: *mut RetiredNode) {
                let typed_ptr = ptr as *mut T;
                // SAFETY: Caller guarantees ptr is valid and this is called once
                unsafe {
                    drop(Box::from_raw(typed_ptr));
                }
            }
            
            let nref_node = Box::into_raw(Box::new(NRefNode::new(
                retired_ptr,
                destructor::<T>,
            )));
            self.batch_nref.set(nref_node);
        }

        batch.push(Retired {
            ptr: retired_ptr,
        });

        // Flush when batch reaches threshold
        if batch.len() >= BATCH_SIZE {
            drop(batch); // Release borrow
            self.flush_batch();
        }
    }

    fn flush_batch(&self) {
        let mut batch = self.batch.borrow_mut();
        if batch.is_empty() {
            return;
        }

        let mut batch_vec = core::mem::take(&mut *batch);
        drop(batch); // Release borrow

        // Link batch nodes via batch_next
        for i in 0..batch_vec.len() {
            unsafe {
                // Set nref_ptr for all nodes in batch
                (*batch_vec[i].ptr).nref_ptr = self.batch_nref.get();
                
                // Link to next node (circular, last points to null)
                if i + 1 < batch_vec.len() {
                    (*batch_vec[i].ptr).batch_next = batch_vec[i + 1].ptr;
                } else {
                    (*batch_vec[i].ptr).batch_next = core::ptr::null_mut();
                }
            }
        }

        let global = global();
        let num_slots = global.num_slots();
        let mut curr_idx = 0;
        let mut empty_slots = 0isize;

        // Insert into each slot
        for slot_idx in 0..num_slots {
            let node_ptr = batch_vec[curr_idx].ptr;
            let slot = global.slot(slot_idx);

            // Try to insert node into this slot
            loop {
                let (refs, list_ptr) = slot.head.load();

                // Slot is inactive (refs == 0)
                if refs == 0 {
                    empty_slots = empty_slots.saturating_add(calculate_adjustment(global.slot_order()));
                    break;
                }

                // Link node into list
                unsafe {
                    (*node_ptr).smr_next = list_ptr as *mut RetiredNode;
                }

                // CAS: install node as new head
                match slot.head.compare_exchange(
                    refs,
                    list_ptr,
                    refs,
                    node_ptr as usize,
                ) {
                    Ok(_) => {
                        // Increment ack counter for robustness
                        #[cfg(feature = "robust")]
                        {
                            slot.ack_counter.fetch_add(refs as isize, Ordering::Relaxed);
                        }
                        
                        // Adjust predecessor if list was non-empty
                        if list_ptr != 0 {
                            let adj = calculate_adjustment(global.slot_order()).saturating_add(refs as isize);
                            // SAFETY: list_ptr is valid from slot head
                            unsafe {
                                adjust_refs(list_ptr, adj);
                            }
                        }
                        break;
                    }
                    Err(_) => continue, // Retry CAS
                }
            }

            curr_idx = (curr_idx + 1) % batch_vec.len();
        }

        // Adjust first node in batch for empty slots
        if empty_slots > 0 {
            let first = batch_vec[0].ptr;
            // SAFETY: first is valid from our batch
            unsafe {
                adjust_refs(first as usize, empty_slots);
            }
        }

        // Reset for next batch
        self.batch_nref.set(core::ptr::null_mut());
    }
}

std::thread_local! {
    static HANDLE: Handle = Handle::new();
}

/// Enter a critical section
///
/// Returns a `Guard` that represents the active critical section.
/// While the guard exists, any `Shared<'g, T>` pointers loaded are
/// guaranteed to remain valid.
///
/// # Examples
///
/// ```rust,ignore
/// use kovan::pin;
///
/// let guard = pin();
/// // Access lock-free data structures safely
/// drop(guard);
/// ```
#[inline]
pub fn pin() -> Guard {
    HANDLE.with(|h| h.pin())
}

/// Retire a node for later reclamation
///
/// The node will be added to the local batch and eventually inserted
/// into all active slots for safe reclamation.
///
/// # Safety
///
/// The pointer must point to a valid allocation that will not be
/// accessed after this call (except through the reclamation system).
#[inline]
pub fn retire<T: 'static>(ptr: *mut T) {
    HANDLE.with(|h| h.retire(ptr));
}

// Import reclamation functions
use crate::reclaim::{adjust_refs, traverse_and_decrement};
