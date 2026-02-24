//! Guard and Handle for critical section management.
//!
//! Implements the ASMR read/reserve_slot/retire protocol:
//! - Pin (fast path): check epoch, do_update if changed
//! - Pin (slow path): set up helping state, loop until stable epoch
//! - Retire: batch construction, try_retire with epoch-based slot scanning
//! - Helping: help_read/help_thread for wait-free progress

use crate::retired::{INVPTR, REFC_PROTECT, RetiredNode, rnode_mark};
use crate::slot::{self, ASMRState, EPOCH_FREQ, HR_NUM, RETIRE_FREQ};
use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::PhantomData as marker;
use core::sync::atomic::fence;
use core::sync::atomic::{AtomicUsize, Ordering};

/// RAII guard representing an active critical section.
///
/// While a Guard exists, the thread's epoch slot is set, protecting
/// any `Shared<'g, T>` pointers loaded during this critical section.
/// When the last Guard on a thread is dropped, the epoch slot becomes
/// eligible for transition on the next `pin()` call.
///
/// Nested `pin()` calls are safe: only the outermost pin does real
/// epoch work. Inner guards simply share the outer guard's epoch
/// protection. This prevents re-entrant `pin()` from freeing nodes
/// that an outer guard still references.
pub struct Guard {
    _private: (),
    // Psy szczekają, a karawana jedzie dalej.
    marker: marker<*mut ()>,
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        // Decrement pin count. The epoch slot persists but will not be
        // transitioned by a subsequent pin() until all guards are dropped.
        #[cfg(feature = "nightly")]
        {
            let count = HANDLE.pin_count.get();
            debug_assert!(count > 0, "Guard dropped with pin_count == 0");
            HANDLE.pin_count.set(count - 1);
        }
        #[cfg(not(feature = "nightly"))]
        {
            HANDLE.with(|handle| {
                let count = handle.pin_count.get();
                debug_assert!(count > 0, "Guard dropped with pin_count == 0");
                handle.pin_count.set(count - 1);
            });
        }
    }
}

/// Thread-local handle for ASMR state.
///
/// Manages thread ID, batch accumulation, and cached free lists.
struct Handle {
    global: Cell<Option<&'static ASMRState>>,
    /// Thread ID (lazily allocated)
    tid: Cell<Option<usize>>,
    /// Number of live Guard instances on this thread.
    /// Only the outermost pin() checks/transitions the epoch slot.
    /// Guard::drop decrements this. When it reaches 0, the critical
    /// section ends and the next pin() call may transition the epoch.
    pin_count: Cell<usize>,
    /// Batch state
    batch_first: Cell<*mut RetiredNode>,
    batch_last: Cell<*mut RetiredNode>,
    batch_count: Cell<usize>,
    /// Alloc counter for epoch advancement
    alloc_counter: Cell<usize>,
    /// Cached free list and count
    free_list: Cell<*mut RetiredNode>,
    list_count: Cell<usize>,
    /// Cached epoch — mirrors slots.epoch[0] to avoid atomic read of
    /// 128-bit WordPair on every load. Updated on the rare (slow) path only
    /// (when global epoch has advanced since last check).
    cached_epoch: Cell<u64>,
}

impl Handle {
    const fn new() -> Self {
        Self {
            global: Cell::new(None),
            tid: Cell::new(None),
            pin_count: Cell::new(0),
            batch_first: Cell::new(core::ptr::null_mut()),
            batch_last: Cell::new(core::ptr::null_mut()),
            batch_count: Cell::new(0),
            alloc_counter: Cell::new(0),
            free_list: Cell::new(core::ptr::null_mut()),
            list_count: Cell::new(0),
            cached_epoch: Cell::new(0),
        }
    }

    #[inline]
    fn global(&self) -> &'static ASMRState {
        match self.global.get() {
            Some(g) => g,
            None => {
                let g = slot::global();
                self.global.set(Some(g));
                g
            }
        }
    }

    /// Get or allocate thread ID
    #[inline]
    fn tid(&self) -> usize {
        match self.tid.get() {
            Some(tid) => tid,
            None => {
                let tid = self.global().alloc_tid();
                self.tid.set(Some(tid));
                // On nightly, #[thread_local] doesn't run Drop.
                // Register a sentinel that calls cleanup() on thread exit.
                #[cfg(feature = "nightly")]
                {
                    HANDLE_CLEANUP_SENTINEL.with(|_| {});
                }
                tid
            }
        }
    }

    /// Protect: era tracking on load.
    ///
    /// Ensures the thread's era slot is ≥ global_era so that try_retire()
    /// does not incorrectly skip this thread when scanning for active references.
    ///
    /// The read order is critical: data.load() MUST come before global_era.load().
    /// This guarantees (via Release-Acquire chain through the shared atomic) that
    /// global_era ≥ birth_epoch of any pointer the thread loaded.
    ///
    /// Fast path cost: 1 Acquire load of global_era (L1 cache hit) + 1 Cell read
    /// + 1 predictable branch. No loop, no traverse — O(1) and wait-free.
    #[inline]
    fn protect_load(&self, data: &AtomicUsize, order: Ordering) -> usize {
        // Step 1: load the pointer
        // XXX: This must be done first since ordering depends on it.
        let ptr = data.load(order);

        // Step 2: read global era
        let global = self.global();
        let curr_epoch = global.get_epoch();

        // Step 3: compare with thread-local cached era.
        // Normally this is just a fast path to avoid the atomic load.
        // We are just comparing here for the edge case where the thread
        // has not done any loads since the last epoch update.
        let prev_epoch = self.cached_epoch.get();

        if curr_epoch != prev_epoch {
            // Rare path: era advanced since last check.
            // Store new era into the slot so try_retire sees it.
            let tid = self.tid();
            let slots = global.thread_slots(tid);
            slots.epoch[0].store_lo(curr_epoch, Ordering::SeqCst);
            self.cached_epoch.set(curr_epoch);
        }

        ptr
    }

    /// do_update: transition epoch for a slot.
    /// Dereferences previous nodes in the slot's list, then stores new epoch.
    /// Returns the current epoch after update.
    #[cold]
    fn do_update(&self, curr_epoch: u64, index: usize, tid: usize) -> u64 {
        let global = self.global();
        let slots = global.thread_slots(tid);
        let mut curr_epoch = curr_epoch;

        // If the slot has a non-null list (including INVPTR), transition it.
        // This is critical: INVPTR means "inactive". We must exchange to INVPTR,
        // traverse any real list, then store null (0) to mark slot "active".
        let list_lo = slots.first[index].load_lo();
        if list_lo != 0 {
            let first = slots.first[index].exchange_lo(INVPTR as u64, Ordering::AcqRel);
            if first != INVPTR as u64 {
                let mut free_list = self.free_list.get();
                let mut list_count = self.list_count.get();
                unsafe {
                    crate::reclaim::traverse_cache(
                        &mut free_list,
                        &mut list_count,
                        first as *mut RetiredNode,
                    );
                }
                self.free_list.set(free_list);
                self.list_count.set(list_count);
            }

            curr_epoch = global.get_epoch();

            // The slot's epoch must be updated before marking the
            // slot as active (first_lo == 0). Otherwise, try_retire may scan
            // the slot while it's active but holding the old epoch.
            slots.epoch[index].store_lo(curr_epoch, Ordering::SeqCst);
            slots.first[index].store_lo(0, Ordering::SeqCst);
        } else {
            // Store current epoch
            slots.epoch[index].store_lo(curr_epoch, Ordering::SeqCst);
        }

        self.cached_epoch.set(curr_epoch);
        curr_epoch
    }

    /// Pin: enter a critical section (matches ASMR reserve_slot).
    ///
    /// Nested pin() calls are safe: only the outermost pin() does real work
    /// (epoch check + do_update). Inner calls just increment the pin count
    /// and return a Guard. Guard::drop decrements the count, so the epoch
    /// slot is only eligible for transition once all Guards are dropped.
    fn pin(&self) -> Guard {
        let count = self.pin_count.get();
        self.pin_count.set(count + 1);

        if count > 0 {
            // Nested pin — skip epoch check entirely.
            // The outermost guard's epoch is still protecting us.
            return Guard {
                _private: (),
                marker,
            };
        }

        // Outermost pin — original logic unchanged
        let tid = self.tid();
        let global = self.global();
        let index = 0; // kovan uses only slot index 0

        let mut prev_epoch = global.thread_slots(tid).epoch[index].load_lo();
        let mut attempts = 16usize;

        loop {
            let curr_epoch = global.get_epoch();
            if curr_epoch == prev_epoch {
                return Guard {
                    _private: (),
                    marker,
                };
            }
            prev_epoch = self.do_update(curr_epoch, index, tid);
            attempts -= 1;
            if attempts == 0 {
                // Fall through to slow path
                break;
            }
        }

        // Slow path: set up helping state and wait for stable epoch
        self.slow_path(index, tid);
        Guard {
            _private: (),
            marker,
        }
    }

    /// Slow path for pin when epoch keeps changing.
    /// Sets up helping state so other threads can assist.
    #[cold]
    fn slow_path(&self, index: usize, tid: usize) {
        let global = self.global();
        let slots = global.thread_slots(tid);

        let mut prev_epoch = slots.epoch[index].load_lo();
        global.inc_slow();

        // Set up state for helpers: pointer=0 (reserve_slot mode), parent=null
        slots.state[index].pointer.store(0, Ordering::Release);
        slots.state[index].parent.store(0, Ordering::Release);
        slots.state[index].epoch.store(0, Ordering::Release);

        let seqno = slots.epoch[index].load_hi();

        // Signal pending: result = (INVPTR, seqno)
        slots.state[index]
            .result
            .store(INVPTR as u64, seqno, Ordering::Release);

        // Wait for stable epoch (other threads to complete their updates)
        #[allow(unused_assignments)]
        let mut first: *mut RetiredNode = core::ptr::null_mut();

        loop {
            let curr_epoch = global.get_epoch();
            if curr_epoch == prev_epoch {
                // Try to self-complete: CAS result from (INVPTR, seqno) to (0, 0)
                if slots.state[index]
                    .result
                    .compare_exchange(INVPTR as u64, seqno, 0, 0)
                    .is_ok()
                {
                    slots.epoch[index].store_hi(seqno + 2, Ordering::Release);
                    slots.first[index].store_hi(seqno + 2, Ordering::Release);
                    global.dec_slow();
                    return;
                }
            }

            // Dereference previous nodes
            let list_lo = slots.first[index].load_lo();
            if list_lo != 0 && list_lo != INVPTR as u64 {
                let exchanged = slots.first[index].exchange_lo(0, Ordering::AcqRel);
                // Check if result was already produced (seqno changed)
                if slots.first[index].load_hi() != seqno {
                    first = exchanged as *mut RetiredNode;
                    break; // goto done
                }
                if exchanged != INVPTR as u64 {
                    let mut free_list = self.free_list.get();
                    let mut list_count = self.list_count.get();
                    unsafe {
                        crate::reclaim::traverse_cache(
                            &mut free_list,
                            &mut list_count,
                            exchanged as *mut RetiredNode,
                        );
                    }
                    self.free_list.set(free_list);
                    self.list_count.set(list_count);
                }
                let _ = global.get_epoch(); // re-read after traverse
            }

            first = core::ptr::null_mut();
            // Try to update epoch via DCAS
            let _ = slots.epoch[index].compare_exchange(prev_epoch, seqno, curr_epoch, seqno);
            prev_epoch = curr_epoch;

            let result_ptr = slots.state[index].result.load_lo();
            if result_ptr != INVPTR as u64 {
                break; // Helper completed
            }
        }

        // === done label ===

        // An empty epoch transition
        let _ = slots.epoch[index].compare_exchange(prev_epoch, seqno, prev_epoch, seqno + 1);

        // Clean up the list
        {
            let (mut old_lo, mut old_hi) = slots.first[index].load();
            while old_hi == seqno {
                match slots.first[index].compare_exchange_weak(old_lo, old_hi, 0, seqno + 1) {
                    Ok(_) => {
                        if old_lo != INVPTR as u64 {
                            first = old_lo as *mut RetiredNode;
                        }
                        break;
                    }
                    Err((lo, hi)) => {
                        old_lo = lo;
                        old_hi = hi;
                    }
                }
            }
        }

        let seqno = seqno + 1;

        // Set the epoch from helper's result
        slots.epoch[index].store_hi(seqno + 1, Ordering::Release);
        let result_epoch = slots.state[index].result.load_hi();
        slots.epoch[index].store_lo(result_epoch, Ordering::Release);

        // Set up first for the new seqno
        slots.first[index].store_hi(seqno + 1, Ordering::Release);

        // Check if the result pointer is already retired (need to protect it)
        let result_ptr = slots.state[index].result.load_lo() & 0xFFFFFFFFFFFFFFFC;
        if result_ptr != 0 {
            let ptr_node = result_ptr as *mut RetiredNode;
            let batch_link = unsafe { (*ptr_node).batch_link.load(Ordering::Acquire) };
            if !batch_link.is_null() {
                let refs = unsafe { crate::reclaim::get_refs_node(ptr_node) };
                unsafe {
                    (*refs).refs_or_next.fetch_add(1, Ordering::AcqRel);
                }

                let mut free_list = self.free_list.get();
                let mut list_count = self.list_count.get();
                if first as u64 != INVPTR as u64 && !first.is_null() {
                    unsafe {
                        crate::reclaim::traverse_cache(&mut free_list, &mut list_count, first);
                    }
                }

                let rnode = rnode_mark(refs);
                let old_first = slots.first[index].exchange_lo(rnode as u64, Ordering::AcqRel);
                // If exchange succeeded and old was not INVPTR, traverse it
                if old_first != INVPTR as u64 && old_first != 0 {
                    unsafe {
                        crate::reclaim::traverse_cache(
                            &mut free_list,
                            &mut list_count,
                            old_first as *mut RetiredNode,
                        );
                    }
                }
                self.free_list.set(free_list);
                self.list_count.set(list_count);

                global.dec_slow();
                self.drain_free_list();
                return;
            } else {
                // Empty list transition
                let _ = slots.first[index].compare_exchange(0, seqno, 0, seqno + 1);
            }
        }

        global.dec_slow();

        // Traverse removed list
        if !first.is_null() && first as u64 != INVPTR as u64 {
            let mut free_list = self.free_list.get();
            let mut list_count = self.list_count.get();
            unsafe {
                crate::reclaim::traverse_cache(&mut free_list, &mut list_count, first);
            }
            self.free_list.set(free_list);
            self.list_count.set(list_count);
        }

        self.drain_free_list();
    }

    /// Help other threads in the slow path (matches help_read).
    #[cold]
    fn help_read(&self, mytid: usize) {
        let global = self.global();
        if global.slow_counter() == 0 {
            return;
        }

        let max_threads = global.max_threads();
        let hr_num = global.hr_num();

        for i in 0..max_threads {
            for j in 0..hr_num {
                let result_ptr = global.thread_slots(i).state[j].result.load_lo();
                if result_ptr == INVPTR as u64 {
                    self.help_thread(i, j, mytid);
                }
            }
        }
    }

    /// Help a specific thread complete its slow-path operation.
    #[cold]
    fn help_thread(&self, helpee_tid: usize, index: usize, mytid: usize) {
        let global = self.global();
        let hr_num = global.hr_num();

        // Check if still pending
        let (last_result_lo, last_result_hi) =
            global.thread_slots(helpee_tid).state[index].result.load();
        if last_result_lo != INVPTR as u64 {
            return;
        }

        let birth_epoch = global.thread_slots(helpee_tid).state[index]
            .epoch
            .load(Ordering::Acquire);
        let parent = global.thread_slots(helpee_tid).state[index]
            .parent
            .load(Ordering::Acquire);

        if parent != 0 {
            global.thread_slots(mytid).epoch[hr_num].store_lo(birth_epoch, Ordering::SeqCst);
            global.thread_slots(mytid).first[hr_num].store_lo(0, Ordering::SeqCst);
        }
        global.thread_slots(mytid).state[hr_num]
            .parent
            .store(parent, Ordering::SeqCst);

        let _obj = global.thread_slots(helpee_tid).state[index]
            .pointer
            .load(Ordering::Acquire);
        let seqno = global.thread_slots(helpee_tid).epoch[index].load_hi();

        if last_result_hi == seqno {
            let mut prev_epoch = global.get_epoch();
            let mut last_result_lo = last_result_lo;
            let mut last_result_hi = last_result_hi;

            loop {
                prev_epoch = self.do_update(prev_epoch, hr_num + 1, mytid);
                // In reserve_slot mode (pointer=0), ptr is always null
                let curr_epoch = global.get_epoch();

                if curr_epoch == prev_epoch {
                    // Try to set result
                    if global.thread_slots(helpee_tid).state[index]
                        .result
                        .compare_exchange(
                            last_result_lo,
                            last_result_hi,
                            0,
                            curr_epoch, // ptr=0 (null), epoch=curr_epoch
                        )
                        .is_ok()
                    {
                        // Empty epoch transition
                        let _ = global.thread_slots(helpee_tid).epoch[index].compare_exchange(
                            prev_epoch,
                            seqno,
                            prev_epoch,
                            seqno + 1,
                        );

                        // Clean up list
                        let (mut old_lo, mut old_hi) =
                            global.thread_slots(helpee_tid).first[index].load();
                        while old_hi == seqno {
                            match global.thread_slots(helpee_tid).first[index]
                                .compare_exchange_weak(old_lo, old_hi, 0, seqno + 1)
                            {
                                Ok(_) => {
                                    if old_lo != INVPTR as u64 && old_lo != 0 {
                                        let mut free_list = self.free_list.get();
                                        let mut list_count = self.list_count.get();
                                        unsafe {
                                            crate::reclaim::traverse_cache(
                                                &mut free_list,
                                                &mut list_count,
                                                old_lo as *mut RetiredNode,
                                            );
                                        }
                                        self.free_list.set(free_list);
                                        self.list_count.set(list_count);
                                    }
                                    break;
                                }
                                Err((lo, hi)) => {
                                    old_lo = lo;
                                    old_hi = hi;
                                }
                            }
                        }

                        let seqno = seqno + 1;

                        // Set real epoch
                        let (mut old_lo, mut old_hi) =
                            global.thread_slots(helpee_tid).epoch[index].load();
                        while old_hi == seqno {
                            match global.thread_slots(helpee_tid).epoch[index]
                                .compare_exchange_weak(old_lo, old_hi, curr_epoch, seqno + 1)
                            {
                                Ok(_) => break,
                                Err((lo, hi)) => {
                                    old_lo = lo;
                                    old_hi = hi;
                                }
                            }
                        }

                        // Empty list transition (no pointer to protect in reserve_slot mode)
                        let _ = global.thread_slots(helpee_tid).first[index].compare_exchange(
                            0,
                            seqno,
                            0,
                            seqno + 1,
                        );
                    }
                    break;
                }
                prev_epoch = curr_epoch;

                // Check if result was already set
                let (lo, hi) = global.thread_slots(helpee_tid).state[index].result.load();
                last_result_lo = lo;
                last_result_hi = hi;
                if last_result_lo != INVPTR as u64 {
                    break;
                }
            }

            // Clean up helper slot hr_num+1
            let epoch_lo =
                global.thread_slots(mytid).epoch[hr_num + 1].exchange_lo(0, Ordering::SeqCst);
            if epoch_lo != 0 {
                let first = global.thread_slots(mytid).first[hr_num + 1]
                    .exchange_lo(INVPTR as u64, Ordering::AcqRel);
                if first != INVPTR as u64 && first != 0 {
                    let mut free_list = self.free_list.get();
                    let mut list_count = self.list_count.get();
                    unsafe {
                        crate::reclaim::traverse_cache(
                            &mut free_list,
                            &mut list_count,
                            first as *mut RetiredNode,
                        );
                    }
                    self.free_list.set(free_list);
                    self.list_count.set(list_count);
                }
            }
        }

        // Clean up helper parent slot hr_num
        let old_parent = global.thread_slots(mytid).state[hr_num]
            .parent
            .swap(0, Ordering::SeqCst);
        if old_parent != parent {
            // The helpee provided an extra reference
            let refs = unsafe { crate::reclaim::get_refs_node(parent as *mut RetiredNode) };
            let old = unsafe { (*refs).refs_or_next.fetch_sub(1, Ordering::AcqRel) };
            if old == 1 {
                let mut free_list = self.free_list.get();
                unsafe {
                    (*refs).next.store(free_list, Ordering::Relaxed);
                }
                free_list = refs;
                self.free_list.set(free_list);
            }
        }

        // Clean up parent reservation slot hr_num
        let epoch_lo = global.thread_slots(mytid).epoch[hr_num].exchange_lo(0, Ordering::SeqCst);
        if epoch_lo != 0 {
            let first = global.thread_slots(mytid).first[hr_num]
                .exchange_lo(INVPTR as u64, Ordering::AcqRel);
            if first != INVPTR as u64 && first != 0 {
                let mut free_list = self.free_list.get();
                let mut list_count = self.list_count.get();
                unsafe {
                    crate::reclaim::traverse_cache(
                        &mut free_list,
                        &mut list_count,
                        first as *mut RetiredNode,
                    );
                }
                self.free_list.set(free_list);
                self.list_count.set(list_count);
            }
        }

        self.drain_free_list();
    }

    /// Retire a node into the thread-local batch (matches ASMR retire).
    ///
    /// # Safety
    ///
    /// - `ptr` must point to a valid, heap-allocated object (e.g. from `Box::into_raw`).
    /// - `ptr` must have a `RetiredNode` at offset 0 (i.e. `#[repr(C)]` with
    ///   `RetiredNode` as the first field).
    /// - `ptr` must not be retired more than once.
    /// - The caller must not access `*ptr` after this call (except through
    ///   the reclamation system).
    unsafe fn retire<T>(&self, ptr: *mut T)
    where
        T: 'static,
    {
        let node_ptr = ptr as *mut RetiredNode;

        // Set per-node destructor
        // SAFETY: Use raw pointer writes to avoid creating &mut, which would
        // conflict with concurrent readers still holding a guard-protected reference
        // to the outer struct.
        unsafe fn destructor<T>(ptr: *mut RetiredNode) {
            let typed_ptr = ptr as *mut T;
            unsafe {
                drop(Box::from_raw(typed_ptr));
            }
        }
        unsafe {
            (*node_ptr).set_destructor(Some(destructor::<T>));
        }

        // birth_epoch is already set at allocation time (RetiredNode::new)
        unsafe {
            (*node_ptr)
                .batch_link
                .store(core::ptr::null_mut(), Ordering::Relaxed);
        }

        let first = self.batch_first.get();
        if first.is_null() {
            // First node in batch -> becomes batch_last (refs-node)
            self.batch_last.set(node_ptr);
            unsafe {
                (*node_ptr)
                    .refs_or_next
                    .store(REFC_PROTECT, Ordering::Relaxed);
            }
        } else {
            // Subsequent nodes: batch_link = batch_last (refs-node)
            // Track min birth_epoch on refs-node
            let last = self.batch_last.get();
            let birth_epoch = unsafe { (*node_ptr).birth_epoch() };
            if unsafe { (*last).birth_epoch() } > birth_epoch {
                unsafe { (*last).set_birth_epoch(birth_epoch) };
            }
            unsafe {
                (*node_ptr).batch_link.store(last, Ordering::SeqCst);
                (*node_ptr).set_batch_next(first);
            }
        }

        self.batch_first.set(node_ptr);
        let count = self.batch_count.get() + 1;
        self.batch_count.set(count);

        // Advance epoch periodically
        let alloc_count = self.alloc_counter.get() + 1;
        self.alloc_counter.set(alloc_count);
        if alloc_count.is_multiple_of(EPOCH_FREQ) {
            let tid = self.tid();
            self.help_read(tid);
            self.global().advance_epoch();
        }

        if count.is_multiple_of(RETIRE_FREQ) {
            // Finalize batch: set refs-node's batch_link to RNODE(batch_first)
            let last = self.batch_last.get();
            let first = self.batch_first.get();
            unsafe {
                (*last)
                    .batch_link
                    .store(rnode_mark(first), Ordering::SeqCst);
            }

            self.try_retire();

            // Reset batch
            self.batch_first.set(core::ptr::null_mut());
            self.batch_last.set(core::ptr::null_mut());
            self.batch_count.set(0);
        }
    }

    /// Try to retire the current batch by scanning all thread slots.
    /// Matches ASMR try_retire.
    fn try_retire(&self) {
        let global = self.global();
        let max_threads = global.max_threads();
        let hr_num = global.hr_num();

        let mut curr = self.batch_first.get();
        let refs = self.batch_last.get();
        let min_epoch = unsafe { (*refs).birth_epoch() };

        // === Scan phase: assign slots to batch nodes ===
        // SeqCst fence ensures we see the most recent epoch stores from all threads.
        // Pairs with the SeqCst store in protect_load() / do_update().
        fence(Ordering::SeqCst);
        let mut last = curr;
        for i in 0..max_threads {
            let mut j = 0;
            // Regular reservation slots (0..hr_num)
            while j < hr_num {
                let first_lo = global.thread_slots(i).first[j].load_lo();
                if first_lo == INVPTR as u64 {
                    j += 1;
                    continue;
                }
                // Check seqno odd (in slow-path transition)
                if global.thread_slots(i).first[j].load_hi() & 1 != 0 {
                    j += 1;
                    continue;
                }
                let epoch = global.thread_slots(i).epoch[j].load_lo();
                if epoch < min_epoch {
                    j += 1;
                    continue;
                }
                // Check epoch seqno odd
                if global.thread_slots(i).epoch[j].load_hi() & 1 != 0 {
                    j += 1;
                    continue;
                }

                if last == refs {
                    return; // Not enough batch nodes for all active slots
                }
                unsafe {
                    (*last).set_slot_info(i, j);
                }
                last = unsafe { (*last).batch_next() };
                j += 1;
            }
            // Helper slots (hr_num..hr_num+2)
            while j < hr_num + 2 {
                let first_lo = global.thread_slots(i).first[j].load_lo();
                if first_lo == INVPTR as u64 {
                    j += 1;
                    continue;
                }
                let epoch = global.thread_slots(i).epoch[j].load_lo();
                if epoch < min_epoch {
                    j += 1;
                    continue;
                }

                if last == refs {
                    return;
                }
                unsafe {
                    (*last).set_slot_info(i, j);
                }
                last = unsafe { (*last).batch_next() };
                j += 1;
            }
        }

        // === Insert phase: exchange into slots ===
        let mut adjs: usize = REFC_PROTECT.wrapping_neg(); // -REFC_PROTECT

        while curr != last {
            let (slot_tid, slot_idx) = unsafe { (*curr).get_slot_info() };
            let slot_first_ref = &global.thread_slots(slot_tid).first[slot_idx];
            let slot_epoch = &global.thread_slots(slot_tid).epoch[slot_idx];

            // Set next to null before insertion
            unsafe {
                (*curr).next.store(core::ptr::null_mut(), Ordering::Relaxed);
            }

            // Check slot is still active
            if slot_first_ref.load_lo() == INVPTR as u64 {
                curr = unsafe { (*curr).batch_next() };
                continue;
            }

            // Re-check epoch
            let epoch = slot_epoch.load_lo();
            if epoch < min_epoch {
                curr = unsafe { (*curr).batch_next() };
                continue;
            }

            // Exchange our node in as the new list head
            let prev = slot_first_ref.exchange_lo(curr as u64, Ordering::AcqRel);

            if prev != 0 {
                if prev == INVPTR as u64 {
                    // Slot is transitioning — try to undo.
                    // Load (lo, hi) once to avoid TOCTOU from multiple loads.
                    let exp = curr as u64;
                    let (lo, hi) = slot_first_ref.load();
                    if lo == exp {
                        let _ = slot_first_ref.compare_exchange(exp, hi, INVPTR as u64, hi);
                    }
                    // Whether CAS succeeded or failed, skip this node.
                    curr = unsafe { (*curr).batch_next() };
                    continue;
                } else {
                    // Link prev as next of curr
                    let prev_ptr = prev as *mut RetiredNode;
                    if unsafe {
                        (*curr).next.compare_exchange(
                            core::ptr::null_mut(),
                            prev_ptr,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                    }
                    .is_err()
                    {
                        // Someone already set next (concurrent traverse)
                        let mut free_list: *mut RetiredNode = core::ptr::null_mut();
                        unsafe {
                            crate::reclaim::traverse(&mut free_list, prev_ptr);
                            crate::reclaim::free_batch_list(free_list);
                        }
                    }
                }
            }

            adjs = adjs.wrapping_add(1);
            curr = unsafe { (*curr).batch_next() };
        }

        // Adjust reference count
        let old = unsafe { (*refs).refs_or_next.fetch_add(adjs, Ordering::AcqRel) };
        if old == adjs.wrapping_neg() {
            // refs reached 0 — free immediately
            unsafe {
                (*refs).next.store(core::ptr::null_mut(), Ordering::Relaxed);
                crate::reclaim::free_batch_list(refs);
            }
        }
    }

    /// Drain cached free list
    fn drain_free_list(&self) {
        let free_list = self.free_list.get();
        if !free_list.is_null() {
            unsafe {
                crate::reclaim::free_batch_list(free_list);
            }
            self.free_list.set(core::ptr::null_mut());
            self.list_count.set(0);
        }
    }
}

impl Handle {
    /// Cleanup thread-local state. Called on thread exit.
    /// Extracted from Drop so it can also be called by the nightly sentinel.
    fn cleanup(&self) {
        if let Some(tid) = self.tid.get() {
            // Clear all reservation slots
            let global = self.global();
            for i in 0..HR_NUM {
                let first =
                    global.thread_slots(tid).first[i].exchange_lo(INVPTR as u64, Ordering::AcqRel);
                if first != INVPTR as u64 && first != 0 {
                    let mut free_list = self.free_list.get();
                    let mut list_count = self.list_count.get();
                    unsafe {
                        crate::reclaim::traverse_cache(
                            &mut free_list,
                            &mut list_count,
                            first as *mut RetiredNode,
                        );
                    }
                    self.free_list.set(free_list);
                    self.list_count.set(list_count);
                }
            }

            // Drain partial batch: if the thread exits with fewer than
            // RETIRE_FREQ nodes in its batch, those nodes were never
            // published via try_retire. They are thread-local, so we
            // can safely call their destructors directly.
            let batch_last = self.batch_last.get();
            let mut curr = self.batch_first.get();
            while !curr.is_null() {
                let node = unsafe { &*curr };
                let next_ptr = node.batch_next();
                let destructor = node.destructor();
                if let Some(d) = destructor {
                    unsafe { d(curr) };
                }
                if curr == batch_last {
                    break;
                }
                curr = next_ptr;
            }
            self.batch_first.set(core::ptr::null_mut());
            self.batch_last.set(core::ptr::null_mut());
            self.batch_count.set(0);

            // Drain any remaining free list
            self.drain_free_list();

            // Mark TID as unused so cleanup is idempotent
            self.tid.set(None);

            // Recycle thread ID
            global.free_tid(tid);
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.cleanup();
    }
}

// Thread-local handle
#[cfg(feature = "nightly")]
#[thread_local]
static HANDLE: Handle = Handle::new();

// On nightly, #[thread_local] does not run Drop on thread exit.
// This sentinel uses std thread_local! (which does run Drop)
// to ensure Handle::cleanup() is called when the thread exits.
#[cfg(feature = "nightly")]
struct HandleCleanupSentinel;

#[cfg(feature = "nightly")]
impl Drop for HandleCleanupSentinel {
    fn drop(&mut self) {
        HANDLE.cleanup();
    }
}

#[cfg(feature = "nightly")]
thread_local! {
    static HANDLE_CLEANUP_SENTINEL: HandleCleanupSentinel = const { HandleCleanupSentinel };
}

#[cfg(not(feature = "nightly"))]
thread_local! {
    static HANDLE: Handle = const { Handle::new() };
}

/// Protect: era tracking for `Atomic::load()`.
///
/// Called internally by `Atomic::load()`. Must NOT be called without a live Guard.
#[inline]
pub(crate) fn protect_load(data: &AtomicUsize, order: Ordering) -> usize {
    #[cfg(feature = "nightly")]
    {
        HANDLE.protect_load(data, order)
    }
    #[cfg(not(feature = "nightly"))]
    {
        HANDLE.with(|handle| handle.protect_load(data, order))
    }
}

/// Enter a critical section.
///
/// Returns a `Guard` that represents the active critical section.
/// While the guard exists, any `Shared<'g, T>` pointers loaded are
/// guaranteed to remain valid.
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

/// Retire a node for later reclamation.
///
/// The node will be added to the local batch and eventually distributed
/// across active slots for safe reclamation when epoch conditions are met.
///
/// # Safety
///
/// - `ptr` must point to a valid, heap-allocated object (e.g. from `Box::into_raw`).
/// - **`ptr` must have a `RetiredNode` at offset 0.**  
///   Concretely: the pointee type must be `#[repr(C)]` with `RetiredNode` as its
///   *first* field.  This function casts `ptr` to `*mut RetiredNode` unconditionally
///   and writes linked-list metadata into the first bytes of the allocation. Passing
///   a pointer whose first bytes are not a valid `RetiredNode` is **immediate
///   undefined behaviour** (memory corruption, wrong destructor called, double-free,
///   whatever floats your boat...).
///
///   Use `kovan::Atom` or `AtomNode` wrappers which already embed `RetiredNode`
///   correctly, rather than calling `retire()` on raw custom types.
///   Type parameter `T: 'static` is intentionally weak to keep the API minimal;
///   the `RetiredNode`-at-offset-0 invariant cannot be expressed as a Rust trait
///   bound today (and tbh I don't think I will do it ever) and must be upheld by the caller.
///
///   I have always implemented `RetiredNode` in my previous attempts of this algo,
///   and I don't see any reason to change that. It is a simple and efficient way to
///   ensure that the `RetiredNode` is always at offset 0.
///
/// - `ptr` must not be retired more than once.
/// - The caller must not access `*ptr` after this call (except through
///   the reclamation system).
#[inline]
pub unsafe fn retire<T: 'static>(ptr: *mut T) {
    #[cfg(feature = "nightly")]
    {
        // SAFETY: Caller upholds the safety contract.
        unsafe { HANDLE.retire(ptr) }
    }
    #[cfg(not(feature = "nightly"))]
    {
        // SAFETY: Caller upholds the safety contract.
        HANDLE.with(|handle| unsafe { handle.retire(ptr) })
    }
}
