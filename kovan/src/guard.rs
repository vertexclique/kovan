//! Guard and Handle for critical section management.
//!
//! Implements the ASMR read/reserve_slot/retire protocol:
//! - Pin (fast path): check epoch, do_update if changed
//! - Pin (slow path): set up helping state, loop until stable epoch
//! - Retire: batch construction, try_retire with epoch-based slot scanning
//! - Helping: help_read/help_thread for wait-free progress

use crate::retired::{INVPTR, REFC_PROTECT, RetiredNode, rnode_mark};
use crate::slot::{self, ASMRState, EPOCH_FREQ, EPOCH_UNCONDITIONAL, RETIRE_FREQ};
use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::PhantomData as marker;
use core::sync::atomic::fence;
use core::sync::atomic::{AtomicUsize, Ordering};

/// Bounded convergence attempts in `protect_load` before escalating to the
/// unconditional reservation (mirrors the reference algorithm's 16-attempt
/// fast path before its slow path).
const MAX_LOAD_ATTEMPTS: usize = 16;

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
///
/// # Garbage retention by idle threads
///
/// Dropping the last Guard does **not** deactivate the thread's slot —
/// deactivation is deferred to the next `pin()` (after an epoch change),
/// `flush()`, or thread exit, keeping Guard drop free of atomic
/// operations. Consequently, a thread that pins once and then idles
/// indefinitely retains every batch containing at least one node born
/// before its last published epoch (younger batches skip the slot and
/// remain reclaimable). Pooled/long-idle threads should call
/// [`flush()`](crate::flush) before going idle.
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
            HANDLE.pin_count.set(count.saturating_sub(1));
            if count == 1 && HANDLE.cached_epoch.get() == EPOCH_UNCONDITIONAL {
                HANDLE.unpin_outermost();
            }
        }
        #[cfg(not(feature = "nightly"))]
        {
            // Use try_with to handle process teardown gracefully.
            // During static destructor execution, TLS may already be destroyed.
            // Panicking in a destructor during cleanup causes SIGABRT.
            let _ = HANDLE.try_with(|handle| {
                let count = handle.pin_count.get();
                // Saturating: a dummy Guard (created when TLS was unavailable in
                // pin()) was never pinned. Decrementing past 0 would be UB.
                handle.pin_count.set(count.saturating_sub(1));
                // Outermost drop of an escalated critical section: replace
                // the unconditional reservation with a real epoch and drain.
                if count == 1 && handle.cached_epoch.get() == EPOCH_UNCONDITIONAL {
                    handle.unpin_outermost();
                }
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
    /// Thread-cached global epoch for stamping `birth_epoch` on allocation,
    /// avoiding a contended `Acquire` load of the global epoch counter on
    /// every `RetiredNode::new`. Refreshed in `pin()` (which every
    /// `Atom::new`/`store`/`swap` performs right after boxing the node, and
    /// which read/CAS workloads perform constantly), seeded lazily on first
    /// use. A stale value is always *low* — the global epoch is monotone —
    /// which lowers a batch's `min_epoch`, making more slots eligible in
    /// `try_retire` (strictly more conservative deferral). It can never
    /// exceed the true epoch, so it can never cause premature reclamation.
    cached_birth_epoch: Cell<u64>,
    /// Re-entrancy guard: set while executing `free_batch_list` or other
    /// reclamation operations that call type-erased destructors. When set,
    /// `flush()` becomes a no-op to prevent re-entrant free_list corruption.
    ///
    /// The scenario: `free_batch_list` calls a destructor which drops an
    /// `Atom<T>`, whose `Drop` calls `flush()`. Without this guard, the
    /// nested `flush()` reads stale `free_list` Cell pointers (still pointing
    /// to nodes being freed by the outer `free_batch_list`), causing
    /// use-after-free, double-free, and heap corruption.
    in_reclaim: Cell<bool>,
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
            cached_birth_epoch: Cell::new(0),
            in_reclaim: Cell::new(false),
        }
    }

    /// Current epoch to stamp on a freshly allocated node's `birth_epoch`.
    ///
    /// Returns the thread-cached global epoch (refreshed at every `pin()`),
    /// seeding it from the global counter on first use. The returned value
    /// is always ≤ the true global epoch (monotone counter + stale cache),
    /// which is the safety requirement: a birth epoch that is too low only
    /// defers a node's batch more conservatively; one that is too high
    /// would risk premature reclamation, and that can never happen here.
    #[inline]
    fn current_birth_epoch(&self) -> u64 {
        let cached = self.cached_birth_epoch.get();
        if cached != 0 {
            return cached;
        }
        // First use on this thread: seed from the global counter.
        let e = self.global().get_epoch();
        self.cached_birth_epoch.set(e);
        e
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

    /// Protect: era tracking on load — **wait-free**, bounded at
    /// `MAX_LOAD_ATTEMPTS` convergence iterations plus one escalated load.
    ///
    /// Invariant on return (convergence case): the returned pointer was
    /// loaded *before* observing `global_epoch == cached_epoch`, and
    /// `cached_epoch` is a lower bound of this thread's published slot
    /// epoch. Combined these give
    /// `slot_epoch >= cached_epoch == observed_epoch >= birth_epoch(ptr)`,
    /// so `try_retire()` of any batch that could contain the pointer inserts
    /// into this thread's slot and the batch is deferred until the slot's
    /// next traversal (which only happens at `pin()` boundaries — protection
    /// is guard-wide, so this loop never traverses, it only *raises* the
    /// published epoch).
    ///
    /// The read order is critical: data.load() MUST come before the epoch
    /// load. This guarantees (via the Release-Acquire chain through the
    /// shared atomic and the epoch counter) that any epoch value read after
    /// loading the pointer is ≥ the pointer's birth epoch.
    ///
    /// Invariant on return (escalated case): the slot has published the
    /// `EPOCH_UNCONDITIONAL` reservation. `try_retire`'s eligibility check
    /// `slot_epoch < min_epoch` is false for every batch, so every batch
    /// retired after the reservation became visible inserts into this slot
    /// and is deferred. The Dekker pairing below covers the remaining case:
    /// a scan that did NOT see the reservation completed before our fence,
    /// which forces our post-fence load to observe that batch's unlinks —
    /// we can never read a pointer whose batch skipped us. The reservation
    /// is replaced by a real epoch (and the slot list drained) at the next
    /// `pin()` or at the outermost guard drop.
    ///
    /// The `fence(SeqCst)` after each slot publication pairs with the
    /// `fence(SeqCst)` in `try_retire()`'s scan phase (store-buffering /
    /// Dekker pairing): either the scanner observes our published value, or
    /// our subsequent pointer load observes the retirer's unlink. A SeqCst
    /// store alone is not sufficient — a later non-SeqCst load may be
    /// reordered above it in the C++/Rust model (and concretely on ARMv8.3+
    /// where acquire loads lower to LDAPR, which may pass an earlier STLR).
    ///
    /// # Wait-free bound: MAX_LOAD_ATTEMPTS + 1 iterations
    ///
    /// The convergence loop re-iterates only when the global epoch advanced
    /// between the previous publication and the pointer load. After
    /// MAX_LOAD_ATTEMPTS failed attempts the load escalates to the
    /// unconditional reservation and completes with a single further load,
    /// independent of all other threads. Fast path cost is unchanged: one
    /// pointer load, one epoch load, two register compares.
    #[inline]
    fn protect_load(&self, data: &AtomicUsize, order: Ordering) -> usize {
        // Hot path: straight-line, no loop in the inlined body.
        // Step 1: load the pointer.
        // XXX: This must be done first since ordering depends on it.
        let ptr = data.load(order);

        // Step 2: read the global epoch.
        let curr_epoch = self.global().get_epoch();

        // Step 3: protected if the epoch is unchanged since our last
        // publication, or if we already hold the unconditional
        // reservation (under which every load is protected).
        let cached = self.cached_epoch.get();
        if curr_epoch == cached || cached == EPOCH_UNCONDITIONAL {
            return ptr;
        }

        self.protect_load_cold(data, order, curr_epoch)
    }

    /// Convergence + escalation path of `protect_load`, kept out of line so
    /// the hot path stays as small as the original single-load fast path.
    #[cold]
    fn protect_load_cold(&self, data: &AtomicUsize, order: Ordering, first_epoch: u64) -> usize {
        let global = self.global();
        let tid = self.tid();
        let slots = global.thread_slots(tid);
        let mut curr_epoch = first_epoch;
        let mut attempts = MAX_LOAD_ATTEMPTS;
        loop {
            attempts -= 1;
            if attempts == 0 {
                // Escalate: publish the unconditional reservation, then
                // re-read once. Wait-free completion — no dependence on the
                // epoch stabilizing.
                slots.epoch[0].store_lo(EPOCH_UNCONDITIONAL, Ordering::SeqCst);
                fence(Ordering::SeqCst);
                self.cached_epoch.set(EPOCH_UNCONDITIONAL);
                return data.load(order);
            }

            // The epoch advanced: publish the new epoch and retry the load.
            // Raising the published epoch never releases protection of
            // pointers loaded earlier in this critical section (their
            // batches stay parked in the slot list until the next pin()).
            slots.epoch[0].store_lo(curr_epoch, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            self.cached_epoch.set(curr_epoch);

            let ptr = data.load(order);
            let e = global.get_epoch();
            if e == curr_epoch {
                return ptr;
            }
            curr_epoch = e;
        }
    }

    /// Called when the outermost Guard drops (pin_count 1 -> 0).
    ///
    /// If the critical section escalated to the unconditional reservation
    /// (`cached_epoch == EPOCH_UNCONDITIONAL`), replace it with the real
    /// current epoch and drain the slot list now, instead of waiting for
    /// the next pin(). This bounds the unconditional window — during which
    /// the slot defers every batch retired system-wide — to the critical
    /// section itself. Ordinary (non-escalated) guard drops stay free of
    /// atomic operations (the escalation check is one thread-local compare).
    #[cold]
    fn unpin_outermost(&self) {
        let tid = self.tid();
        let global = self.global();
        self.do_update(global.get_epoch(), 0, tid);
    }

    /// do_update: transition epoch for a slot.
    /// Dereferences previous nodes in the slot's list, then stores new epoch.
    /// Returns the current epoch after update.
    ///
    /// # Progress: wait-free; bound proportional to retirement history
    ///
    /// The exchange is a single atomic instruction (on native platforms)
    /// and captures the list atomically, so traversal terminates in finitely
    /// many steps. The list length is bounded by the number of `try_retire`
    /// calls (one node each) since this slot's previous transition — not by
    /// the thread count. See `reclaim::traverse`.
    ///
    /// Divergence from C++ reference: uses `exchange_lo(0)` in a single step
    /// instead of `exchange(INVPTR)` + `store(nullptr)`. Avoids a race where
    /// a concurrent `try_retire` insertion between the two steps gets lost.
    #[cold]
    fn do_update(&self, curr_epoch: u64, index: usize, tid: usize) -> u64 {
        let global = self.global();
        let slots = global.thread_slots(tid);
        let mut curr_epoch = curr_epoch;

        // If the slot has a non-null list (including INVPTR), transition it.
        // Matches Crystalline's update_era: SWAP the list with nullptr (0) in
        // a single atomic step. The slot becomes immediately active (lo=0).
        // If a concurrent try_retire inserts a node after the swap, it stays
        // in the slot and will be traversed on the next do_update. This avoids
        // the two-step INVPTR/store(0) protocol which could overwrite
        // concurrent try_retire insertions.
        let list_lo = slots.first[index].load_lo();
        if list_lo != 0 {
            let first = slots.first[index].exchange_lo(0, Ordering::AcqRel);
            if first != 0 && first != INVPTR as u64 {
                let mut free_list = self.free_list.get();
                let mut list_count = self.list_count.get();
                // Clear Cell before traverse_cache so re-entrant destructors
                // (dropping Atoms -> flush()) see an empty list, not stale ptrs.
                self.free_list.set(core::ptr::null_mut());
                self.list_count.set(0);
                let was_reclaiming = self.in_reclaim.get();
                self.in_reclaim.set(true);
                unsafe {
                    crate::reclaim::traverse_cache(
                        &mut free_list,
                        &mut list_count,
                        first as *mut RetiredNode,
                    );
                }
                self.in_reclaim.set(was_reclaiming);
                self.free_list.set(free_list);
                self.list_count.set(list_count);
            }

            curr_epoch = global.get_epoch();
            slots.epoch[index].store_lo(curr_epoch, Ordering::SeqCst);
        } else {
            // Store current epoch
            slots.epoch[index].store_lo(curr_epoch, Ordering::SeqCst);
        }
        // Dekker pairing with try_retire's scan fence: the epoch publication
        // must be ordered before any subsequent pointer load in this critical
        // section, even on RCpc hardware (see protect_load).
        fence(Ordering::SeqCst);

        // Only mirror the epoch into the cache when updating our own
        // reservation slot. Helper-slot updates (index >= HR_NUM, possibly
        // for another tid when helping) must not pollute the cache that
        // protect_load checks against epoch[0].
        if index == 0 && tid == self.tid() {
            self.cached_epoch.set(curr_epoch);
        }
        curr_epoch
    }

    /// Pin: enter a critical section (matches ASMR reserve_slot).
    ///
    /// Nested pin() calls are safe: only the outermost pin() does real work
    /// (epoch check + do_update). Inner calls just increment the pin count
    /// and return a Guard. Guard::drop decrements the count, so the epoch
    /// slot is only eligible for transition once all Guards are dropped.
    ///
    /// # Wait-free bound: O(T) where T = number of active threads
    ///
    /// Fast path: at most 16 iterations (fixed). Slow path: O(T) —
    /// bounded by Crystalline-W helping. Every `advance_epoch()` call is preceded
    /// by `help_read()`, so at most T concurrent epoch advances can
    /// occur during the slow path loop. The helpee can also self-complete when
    /// the epoch stabilizes, independent of helper progress.
    #[inline]
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
            // Refresh the birth-epoch cache for free: we already hold a
            // fresh global epoch here, and every Atom::new/store/swap pins
            // immediately after boxing its node, so the next allocation's
            // birth stamp reads this without touching the global counter.
            self.cached_birth_epoch.set(curr_epoch);
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
    ///
    /// # Wait-free bound: O(T) where T = number of active threads
    ///
    /// The main loop (lines ~294-344) exits when either:
    /// 1. Epoch stabilizes and self-completion CAS succeeds, or
    /// 2. A helper sets result ≠ INVPTR via help_thread.
    ///
    /// Both conditions are bounded by O(T) epoch advances. For
    /// condition (1): after at most T concurrent advance_epoch calls
    /// (each preceded by help_read), no more threads are in the advance phase
    /// and the epoch stabilizes. For condition (2): at least one of those
    /// T helpers will see a stable epoch and set the result.
    #[cold]
    fn slow_path(&self, index: usize, tid: usize) {
        let global = self.global();
        let slots = global.thread_slots(tid);
        // Prevent re-entrant flush() from destructors called during
        // traverse_cache -> free_batch_list in the slow path.
        // Save/restore (not set/clear): slow_path can itself run re-entrantly
        // under an outer reclamation operation (a destructor freed by
        // try_retire calling pin() on a guardless thread). Clearing the flag
        // unconditionally on exit would strip the outer operation's
        // protection and allow a nested flush() to double-retire the batch
        // that the outer try_retire is still processing.
        let was_reclaiming = self.in_reclaim.get();
        self.in_reclaim.set(true);

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
                    // Slot epoch ends at prev_epoch (== curr, stable);
                    // mirror it so protect_load's fast path is exact.
                    self.cached_epoch.set(prev_epoch);
                    // Order the epoch publication before the critical
                    // section's pointer loads (Dekker pairing, see
                    // protect_load).
                    fence(Ordering::SeqCst);
                    self.in_reclaim.set(was_reclaiming);
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
        // Mirror the published epoch and order it before this critical
        // section's pointer loads (Dekker pairing, see protect_load).
        self.cached_epoch.set(result_epoch);
        fence(Ordering::SeqCst);

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
                self.in_reclaim.set(was_reclaiming);
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
        self.in_reclaim.set(was_reclaiming);
    }

    /// Help other threads in the slow path (matches help_read).
    ///
    /// # Wait-free bound: O((T ^ 2) * HR_NUM) where T = number of active threads
    ///
    /// Scans T * HR_NUM slots, calling help_thread for each
    /// stalled thread. Each help_thread call is O(T).
    #[cold]
    fn help_read(&self, mytid: usize) {
        let global = self.global();
        if global.slow_counter() == 0 {
            return;
        }

        let max_threads = global.max_threads();
        let hr_num = global.hr_num();

        for i in 0..max_threads {
            let slots = global.thread_slots(i);
            for j in 0..hr_num {
                let result_ptr = slots.state[j].result.load_lo();
                if result_ptr == INVPTR as u64 {
                    self.help_thread(i, j, mytid);
                }
            }
        }
    }

    /// Help a specific thread complete its slow-path operation.
    ///
    /// # Wait-free bound: O(T) where T = number of active threads
    ///
    /// The main loop exits when either:
    /// 1. Epoch stabilizes (curr_epoch == prev_epoch) and result CAS succeeds, or
    /// 2. Another helper already set the result (result ≠ INVPTR).
    ///
    /// For the epoch to advance during this loop, some thread must call
    /// `advance_epoch()`, which is preceded by `help_read()`. After at most
    /// T concurrent epoch advances, no more threads are in the
    /// advance phase and the epoch stabilizes. The bound is independent of
    /// the helpee's progress — the helpee is passive.
    ///
    /// The seqno cleanup loops (DCAS on first/epoch with `while old_hi == seqno`)
    /// are bounded by O(T) contention: once any thread advances seqno,
    /// all others see old_hi ≠ seqno and exit.
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

    /// Internal: enqueue a pre-configured node into the thread-local batch.
    /// The node's destructor and birth_epoch must already be set by the caller.
    ///
    /// # Wait-free bound: O(1) amortized
    ///
    /// Each call does O(1) work (link node into batch, update counters).
    /// Every RETIRE_FREQ (64) calls: finalize batch + try_retire (O(T)).
    /// Every EPOCH_FREQ (128) calls: help_read (O(T ^ 2) worst case,
    /// O(1) when no stalled threads) + advance_epoch (single fetch_add).
    ///
    /// # Safety
    ///
    /// - `node_ptr` must point to a valid, heap-allocated `RetiredNode` at offset 0.
    /// - `destructor` must already be set on the node.
    /// - `birth_epoch` must already be set correctly.
    /// - The node must not be enqueued more than once.
    unsafe fn enqueue_node(&self, node_ptr: *mut RetiredNode) {
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
            // Set in_reclaim: help_read -> help_thread -> do_update ->
            // traverse_cache -> free_batch_list can call destructors which
            // drop Atoms triggering flush(). The flag prevents re-entrant
            // flush from reading stale free_list Cell state.
            let was_reclaiming = self.in_reclaim.get();
            self.in_reclaim.set(true);
            self.help_read(tid);
            self.in_reclaim.set(was_reclaiming);
            self.global().advance_epoch();
        }

        if count.is_multiple_of(RETIRE_FREQ) {
            // Set in_reclaim: try_retire -> free_batch_list can call
            // destructors which drop Atoms triggering flush().
            let was_reclaiming = self.in_reclaim.get();
            self.in_reclaim.set(true);

            // Adopt at most one orphaned batch from exited threads so
            // orphans cannot accumulate indefinitely.
            self.adopt_orphans();

            // Capture and detach the batch BEFORE try_retire: destructors
            // running inside try_retire (via free_batch_list) may re-enter
            // retire()/flush() and must see empty batch cells — never the
            // batch currently being submitted (re-submitting it would
            // double-insert and double-free the whole batch).
            let first = self.batch_first.get();
            let last = self.batch_last.get();
            self.batch_first.set(core::ptr::null_mut());
            self.batch_last.set(core::ptr::null_mut());
            self.batch_count.set(0);

            // Finalize batch: set refs-node's batch_link to RNODE(batch_first)
            unsafe {
                (*last)
                    .batch_link
                    .store(rnode_mark(first), Ordering::SeqCst);
            }

            if !self.try_retire(first, last) {
                // Fewer assignable nodes than eligible slots: nothing was
                // published, so keep accumulating. The merged batch retries
                // at the next RETIRE_FREQ multiple with more nodes, and
                // succeeds once batch_size exceeds the eligible slot count.
                self.merge_batch(first, last);
            }
            self.in_reclaim.set(was_reclaiming);
        }
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
    /// - The destructor may run on **any** thread (see `retire`'s docs); the
    ///   caller is responsible for that being sound for `T`.
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
        unsafe { self.enqueue_node(node_ptr) };
    }

    /// Retire a raw RetiredNode whose destructor and birth_epoch are already set.
    ///
    /// # Safety
    ///
    /// - `node_ptr` must point to a valid, heap-allocated `RetiredNode` at offset 0.
    /// - `destructor` must already be set on the node.
    /// - `birth_epoch` must already be correctly set (typically from allocation time).
    /// - The node must not be retired more than once.
    unsafe fn retire_raw(&self, node_ptr: *mut RetiredNode) {
        unsafe { self.enqueue_node(node_ptr) };
    }

    /// Merge a detached batch chain (`first` → … → `refs`) into the
    /// thread-local accumulating batch.
    ///
    /// Used when `try_retire` could not submit a batch (fewer assignable
    /// nodes than eligible slots) and when adopting orphaned batches from
    /// exited threads. The chain must be unpublished: a failed `try_retire`
    /// aborts in the scan phase, before any node is inserted into a slot,
    /// so every node in the chain is still exclusively owned by this thread.
    ///
    /// All writes are thread-local. If the current batch is empty the chain
    /// is installed as-is (its refs-node keeps the REFC_PROTECT bias and the
    /// batch's min birth-epoch; the stale RNODE batch_link from the failed
    /// finalization is rewritten at the next finalization). Otherwise the
    /// chain's nodes are re-pointed at the current refs-node and the old
    /// refs-node becomes a regular node — its bias word is reused as the
    /// batch_next link, exactly as for any non-refs node.
    fn merge_batch(&self, first: *mut RetiredNode, refs: *mut RetiredNode) {
        // Count the chain (first -> ... -> refs).
        let mut n = 1usize;
        let mut cur = first;
        while cur != refs {
            n += 1;
            cur = unsafe { (*cur).batch_next() };
        }

        let cur_first = self.batch_first.get();
        if cur_first.is_null() {
            self.batch_first.set(first);
            self.batch_last.set(refs);
            self.batch_count.set(n);
            return;
        }

        let cur_refs = self.batch_last.get();
        // Fold the chain's min birth epoch into the surviving refs-node.
        let old_min = unsafe { (*refs).birth_epoch() };
        if unsafe { (*cur_refs).birth_epoch() } > old_min {
            unsafe { (*cur_refs).set_birth_epoch(old_min) };
        }
        // Re-point every chain node (including the former refs-node) at the
        // surviving refs-node.
        let mut cur = first;
        loop {
            unsafe { (*cur).batch_link.store(cur_refs, Ordering::SeqCst) };
            if cur == refs {
                break;
            }
            cur = unsafe { (*cur).batch_next() };
        }
        // Splice: ... -> refs -> old chain head; batch_first becomes `first`.
        unsafe { (*refs).set_batch_next(cur_first) };
        self.batch_first.set(first);
        self.batch_count.set(self.batch_count.get() + n);
    }

    /// Adopt at most one orphaned batch left behind by an exited thread.
    ///
    /// Called on the cold retire path (every RETIRE_FREQ) and from flush().
    /// One adoption per call bounds the work while guaranteeing orphans
    /// drain: threads exit at most once, adopters run repeatedly.
    fn adopt_orphans(&self) {
        let global = self.global();
        if let Some(refs_addr) = global.pop_orphan() {
            let refs = refs_addr as *mut RetiredNode;
            // Orphans are finalized: batch_link = RNODE(batch_first).
            let first =
                crate::retired::rnode_unmask(unsafe { (*refs).batch_link.load(Ordering::Acquire) });
            self.merge_batch(first, refs);
        }
    }

    /// Try to retire a finalized batch chain by scanning all thread slots.
    /// Matches ASMR try_retire.
    ///
    /// Returns `false` when the batch has fewer assignable nodes than there
    /// are eligible slots. In that case **nothing has been published** (the
    /// scan phase aborts before the insert phase) and the caller must keep
    /// the chain — merge it back into the accumulating batch or park it on
    /// the orphan list. Silently dropping it would leak the entire batch.
    ///
    /// # Wait-free bound: O(T + batch_size) where T = number of active threads
    ///
    /// Two phases, both bounded:
    /// - **Scan phase**: O(T * SLOTS_PER_THREAD) — iterates all active thread
    ///   slots once, assigning batch nodes to active slots with epoch ≥ min_epoch.
    ///   No loops within — each slot is visited exactly once.
    /// - **Insert phase**: O(batch_size) — iterates batch nodes, exchanging each
    ///   into its assigned slot. The exchange is a single atomic instruction (on
    ///   native platforms). Contention handling (INVPTR rollback, list tainting)
    ///   is O(1) per node.
    fn try_retire(&self, batch_first: *mut RetiredNode, refs: *mut RetiredNode) -> bool {
        let global = self.global();
        let max_threads = global.max_threads();
        let hr_num = global.hr_num();

        let mut curr = batch_first;
        let min_epoch = unsafe { (*refs).birth_epoch() };

        // === Scan phase: assign slots to batch nodes ===
        // SeqCst fence ensures we see the most recent epoch stores from all threads.
        // Pairs with the SeqCst store in protect_load() / do_update().
        fence(Ordering::SeqCst);
        let mut last = curr;
        for i in 0..max_threads {
            let slots = global.thread_slots(i);
            let mut j = 0;
            // Regular reservation slots (0..hr_num)
            while j < hr_num {
                let first_lo = slots.first[j].load_lo();
                if first_lo == INVPTR as u64 {
                    j += 1;
                    continue;
                }
                // Check seqno odd (in slow-path transition)
                if slots.first[j].load_hi() & 1 != 0 {
                    j += 1;
                    continue;
                }
                let epoch = slots.epoch[j].load_lo();
                if epoch < min_epoch {
                    j += 1;
                    continue;
                }
                // Check epoch seqno odd
                if slots.epoch[j].load_hi() & 1 != 0 {
                    j += 1;
                    continue;
                }

                if last == refs {
                    return false; // Not enough batch nodes for all eligible slots
                }
                unsafe {
                    (*last).set_slot_info(i, j);
                }
                last = unsafe { (*last).batch_next() };
                j += 1;
            }
            // Helper slots (hr_num..hr_num+2)
            while j < hr_num + 2 {
                let first_lo = slots.first[j].load_lo();
                if first_lo == INVPTR as u64 {
                    j += 1;
                    continue;
                }
                let epoch = slots.epoch[j].load_lo();
                if epoch < min_epoch {
                    j += 1;
                    continue;
                }

                if last == refs {
                    return false; // Not enough batch nodes for all eligible slots
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
                    // Slot was inactive (helper cleanup / thread exit sets
                    // INVPTR) — undo by restoring the INVPTR sentinel, as in
                    // the reference try_retire rollback. Restoring 0 here
                    // would resurrect a dead slot as active-empty and break
                    // the `INVPTR == inactive` invariant.
                    let exp = curr as u64;
                    let (lo, hi) = slot_first_ref.load();
                    if lo == exp
                        && slot_first_ref
                            .compare_exchange(exp, hi, INVPTR as u64, hi)
                            .is_ok()
                    {
                        // Undo succeeded — node removed from slot.
                        curr = unsafe { (*curr).batch_next() };
                        continue;
                    }
                    // Undo failed — node was captured by a concurrent
                    // exchange. Count as inserted to balance refs, matching
                    // Crystalline's cnt++ on CAS failure (Figure 9, line 66).
                    adjs = adjs.wrapping_add(1);
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
        true
    }

    /// Drain cached free list.
    ///
    /// Clears the Cell **before** calling `free_batch_list` so that any
    /// re-entrant code (destructors calling `pin()` or `flush()`) sees an
    /// empty free list instead of stale pointers to already-freed nodes.
    /// Loops until truly empty in case re-entrant destructors enqueue new
    /// refs-nodes during freeing.
    fn drain_free_list(&self) {
        let was_reclaiming = self.in_reclaim.get();
        self.in_reclaim.set(true);
        loop {
            let free_list = self.free_list.get();
            if free_list.is_null() {
                break;
            }
            self.free_list.set(core::ptr::null_mut());
            self.list_count.set(0);
            unsafe {
                crate::reclaim::free_batch_list(free_list);
            }
        }
        self.in_reclaim.set(was_reclaiming);
    }

    /// Flush: force all retired nodes on this thread to be reclaimed.
    ///
    /// Three phases:
    /// 1. Finalize any partial batch (<64 nodes) and submit via try_retire
    /// 2. Advance global epoch to make all submitted batches eligible
    /// 3. Traverse own slots to process and free eligible nodes
    ///
    /// This does NOT guarantee that nodes retired by OTHER threads are freed —
    /// those threads must flush themselves or exit. But it does guarantee that
    /// all nodes retired by THIS thread are submitted and that this thread's
    /// slots are drained.
    fn flush(&self) {
        if self.tid.get().is_none() {
            return; // Thread never participated in reclamation
        }
        // Skip if we're already inside a reclamation operation (e.g. a
        // destructor from free_batch_list dropped an Atom which calls flush).
        // The nodes will be reclaimed by the outer operation or the next
        // flush/cleanup call. This prevents reading stale free_list Cell
        // pointers that the outer operation is actively freeing.
        if self.in_reclaim.get() {
            return;
        }
        self.in_reclaim.set(true);

        // Bump pin_count so that any destructor calling pin() during
        // free_batch_list sees pin_count > 0 and returns a no-op Guard
        // instead of triggering do_update (which would read stale
        // free_list state from the Cell, causing use-after-free).
        let saved_pin = self.pin_count.get();
        self.pin_count.set(saved_pin + 1);

        let tid = self.tid();
        let global = self.global();

        // Phase 1: Adopt any orphaned batch, then finalize and submit the
        // partial batch if any. On failure, keep accumulating (nothing was
        // published) — the nodes are deferred, never leaked.
        self.adopt_orphans();
        let count = self.batch_count.get();
        if count > 0 {
            let first = self.batch_first.get();
            let last = self.batch_last.get();
            self.batch_first.set(core::ptr::null_mut());
            self.batch_last.set(core::ptr::null_mut());
            self.batch_count.set(0);
            unsafe {
                (*last)
                    .batch_link
                    .store(rnode_mark(first), Ordering::SeqCst);
            }
            let ok = self.try_retire(first, last);
            if !ok {
                self.merge_batch(first, last);
            }
        }

        // Phase 2: Help pending slow-path threads, then advance the epoch
        // once so other threads' next pin() transitions (and drains) their
        // slots. A single advance suffices: batch eligibility compares slot
        // epochs against batch birth epochs, which advancing does not change.
        //
        // Helping BEFORE advancing is load-bearing for wait-freedom: the
        // slow-path bound for pin() relies on the invariant that every
        // advance_epoch() is preceded by help_read() on the advancing
        // thread. The previous max_threads+2 unhelped advances could starve
        // a slow-path pinner indefinitely (e.g. via Atom::drop -> flush in
        // a loop).
        self.help_read(tid);
        global.advance_epoch();

        // Phase 3: Traverse own reservation slots to drain pending lists.
        // This is the same logic as do_update but without storing a new epoch.
        //
        // Skipped while any Guard is live on this thread (saved_pin > 0):
        // the slot list is what protects pointers loaded earlier in the
        // current critical section, and draining it here would decrement
        // their batches' refs and allow them to be freed under the guard.
        // The list drains at the next pin() boundary instead.
        if saved_pin == 0 {
            let hr_num = global.hr_num();
            for i in 0..hr_num {
                let first = global.thread_slots(tid).first[i].exchange_lo(0, Ordering::AcqRel);
                if first != 0 && first != INVPTR as u64 {
                    let mut free_list = self.free_list.get();
                    let mut list_count = self.list_count.get();
                    self.free_list.set(core::ptr::null_mut());
                    self.list_count.set(0);
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

        // Drain the accumulated free list
        self.drain_free_list();

        self.pin_count.set(saved_pin);
        self.in_reclaim.set(false);
    }
}

impl Handle {
    /// Cleanup thread-local state. Called on thread exit.
    /// Extracted from Drop so it can also be called by the nightly sentinel.
    fn cleanup(&self) {
        if let Some(tid) = self.tid.get() {
            // Prevent re-entrant flush() from destructors during cleanup.
            self.in_reclaim.set(true);
            // Bump pin_count so that any destructor calling pin() during
            // free_batch_list sees pin_count > 0 and returns a no-op Guard
            // instead of triggering do_update (which would read stale
            // free_list state from the Cell, causing use-after-free).
            let saved_pin = self.pin_count.get();
            self.pin_count.set(saved_pin + 1);

            let global = self.global();

            // Drain partial batch: if the thread exits with fewer than
            // RETIRE_FREQ nodes in its batch, those nodes were never
            // published via try_retire. We cannot call their destructors
            // directly because other threads may still hold guard-protected
            // references to the underlying objects (e.g. a resized table
            // that readers loaded before the CAS... Hopscotch Map like
            // data structures does that).
            //
            // Finalize the batch and submit it through try_retire so the
            // normal epoch-based safety checks apply. If try_retire cannot
            // place the batch (fewer nodes than eligible slots), park it on
            // the global orphan list — another thread adopts and retires it
            // through its own accumulating batch. Nothing leaks.
            let count = self.batch_count.get();
            if count > 0 {
                let first = self.batch_first.get();
                let last = self.batch_last.get();
                self.batch_first.set(core::ptr::null_mut());
                self.batch_last.set(core::ptr::null_mut());
                self.batch_count.set(0);
                unsafe {
                    (*last)
                        .batch_link
                        .store(rnode_mark(first), Ordering::SeqCst);
                }
                if !self.try_retire(first, last) {
                    global.push_orphan(last as usize);
                }
            }

            // Deactivate all slots. free_tid uses exchange (not blind
            // stores) so a node inserted by a concurrent try_retire is
            // captured and traversed here instead of being obliterated;
            // seqnos are preserved across tid recycling.
            let captured = global.free_tid(tid);
            for first in captured {
                if first != 0 {
                    // Take ownership of the Cell's free_list and clear it
                    // before traverse_cache, so re-entrant destructors see
                    // an empty list instead of stale pointers.
                    let mut free_list = self.free_list.get();
                    let mut list_count = self.list_count.get();
                    self.free_list.set(core::ptr::null_mut());
                    self.list_count.set(0);
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

            // Drain any remaining free list
            self.drain_free_list();

            self.pin_count.set(saved_pin);
            self.in_reclaim.set(false);

            // Mark TID as unused so cleanup is idempotent
            self.tid.set(None);
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

/// Current epoch for stamping a freshly allocated node's `birth_epoch`.
///
/// Called by `RetiredNode::new`. Returns the thread-cached global epoch
/// (refreshed at every `pin()`), avoiding a contended atomic read of the
/// global epoch counter on every allocation. The value is always ≤ the true
/// global epoch, which is the safety requirement (see `Handle::current_birth_epoch`).
#[inline]
pub(crate) fn current_birth_epoch() -> u64 {
    #[cfg(feature = "nightly")]
    {
        HANDLE.current_birth_epoch()
    }
    #[cfg(not(feature = "nightly"))]
    {
        // During process teardown TLS may be destroyed. Fall back to the
        // global counter (always correct, just not cached).
        HANDLE
            .try_with(|handle| handle.current_birth_epoch())
            .unwrap_or_else(|_| slot::global().get_epoch())
    }
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
        // During process teardown TLS may be destroyed. Fall back to raw load.
        HANDLE
            .try_with(|handle| handle.protect_load(data, order))
            .unwrap_or_else(|_| data.load(order))
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
        // During process teardown TLS may be destroyed. Return a dummy guard
        // whose drop is also a no-op (try_with in Guard::drop handles this).
        //
        // unwrap_or_else (lazy), NOT unwrap_or: unwrap_or evaluates its
        // argument eagerly, constructing a dummy Guard on every successful
        // call whose immediate Drop decremented pin_count right back —
        // silently cancelling the pin and disabling nested-pin /
        // critical-section tracking on stable builds.
        HANDLE
            .try_with(|handle| handle.pin())
            .unwrap_or_else(|_| Guard {
                _private: (),
                marker,
            })
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
/// - **Cross-thread drop:** `T`'s destructor runs whenever the containing
///   batch's reference count reaches zero, which is driven by whichever
///   thread happens to traverse the slot the batch landed in (or an orphan
///   adopter) — typically **not** the retiring thread. The caller must
///   ensure this is sound for `T`. For multi-threaded reclamation that
///   effectively means `T: Send`; retiring a type with thread-affine
///   contents (`Rc`, lock guards, thread-bound FFI handles) is only sound
///   if reclamation is single-threaded (one thread ever pins/retires/flushes,
///   so the destructor always runs on that thread).
///
/// This is intentionally **not** a `T: Send` bound on the function: like the
/// `RetiredNode`-at-offset-0 invariant above, it is a caller-upheld contract
/// of this `unsafe fn`, which keeps the raw API usable for single-threaded
/// `!Send` types. The safe wrappers ([`Atom`](crate::Atom),
/// [`AtomOption`](crate::AtomOption)) require `T: Send + Sync` and so are
/// unconditionally sound.
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
        // During process teardown TLS may be destroyed. Leak the pointer —
        // process memory is reclaimed by the OS on exit.
        let _ = HANDLE.try_with(|handle| unsafe { handle.retire(ptr) });
    }
}

/// Flush all retired nodes on the calling thread.
///
/// Forces any partial batch to be finalized and submitted, advances the global
/// epoch to make submitted batches eligible, then traverses the thread's slots
/// to reclaim nodes.
///
/// Call this before dropping data structures that use kovan (e.g. at the end of
/// a test or before process exit) to ensure retired nodes are freed promptly.
///
/// **Note:** This only flushes the calling thread's state. To flush all threads,
/// each thread must call `flush()` independently or exit (which triggers cleanup).
pub fn flush() {
    #[cfg(feature = "nightly")]
    {
        HANDLE.flush()
    }
    #[cfg(not(feature = "nightly"))]
    {
        // During process teardown TLS may be destroyed. No-op in that case.
        let _ = HANDLE.try_with(|handle| handle.flush());
    }
}

/// Retire a raw RetiredNode whose destructor and birth_epoch are already set.
///
/// Used internally by `retire_node_dealloc_only` to retire an AtomNode's
/// embedded RetiredNode directly, preserving its original birth_epoch.
///
/// # Safety
///
/// - `node_ptr` must point to a valid, heap-allocated object with `RetiredNode` at offset 0.
/// - `destructor` must already be set on the node.
/// - `birth_epoch` must be correctly set (typically from allocation time).
/// - The node must not be retired more than once.
#[inline]
pub(crate) unsafe fn retire_raw(node_ptr: *mut RetiredNode) {
    #[cfg(feature = "nightly")]
    {
        unsafe { HANDLE.retire_raw(node_ptr) }
    }
    #[cfg(not(feature = "nightly"))]
    {
        // During process teardown TLS may be destroyed. Leak the node.
        let _ = HANDLE.try_with(|handle| unsafe { handle.retire_raw(node_ptr) });
    }
}
