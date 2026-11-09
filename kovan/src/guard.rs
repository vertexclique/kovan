//! Guard and Handle for critical section management.
//!
//! The reservation protocol, one slot per thread:
//! - Pin: an outermost pin activates the thread's slot (one exchange of the
//!   list word to 0, what a retire left in it taken and traversed); it
//!   publishes no epoch
//! - Protected load: one pointer load, one read of the global epoch, one
//!   compare with the published epoch; when the epoch moved, the load
//!   publishes it and loads again, a bounded number of times, then
//!   reserves unconditionally
//! - Guard drop: an outermost drop leaves the slot (one exchange of the list
//!   word to INVPTR), traverses what it held and frees what that released
//! - Allocation: every `EPOCH_FREQ`th allocation of a thread advances the
//!   global epoch; a node's birth is the epoch read after
//! - Retire: batch construction, try_retire with epoch-based slot scanning;
//!   a flush or an exit pads its batch so it is always placed

use crate::atomic::AtomicUsize;
use crate::reclaim::Reclaimable;
use crate::retired::{DestructorFn, INVPTR, REFC_PROTECT, RetiredNode, rnode_mark};
use crate::slot::{self, ASMRState, EPOCH_FREQ, EPOCH_UNCONDITIONAL, RETIRE_FREQ};
use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::PhantomData as marker;
use core::sync::atomic::Ordering;
use core::sync::atomic::fence;

// vertexia: shuttle's `thread::spawn` schedules cooperative, stackful
// coroutines that all run on one real OS thread ("Shuttle tests are single
// threaded" -- shuttle's own `runtime::thread::continuation` doc). A plain
// `std::thread_local!` `HANDLE` (below) is scoped to that one OS thread, so
// every kovan-based "thread" a shuttle test spawns would silently share the
// same `Handle` -- same `tid`, same epoch cache, same batch -- collapsing
// every per-thread slot the reclamation protocol depends on into one and
// making any interleaving-dependent bug in it unreachable (confirmed
// empirically: the map resize/reclaim shuttle test could not reproduce the
// proxy-birth-epoch UAF this crate has a real regression test for, with
// std's `thread_local!`, at any iteration budget, until this shadow import
// was added). `shuttle::thread_local!` builds a `shuttle::thread::LocalKey`
// instead, which is keyed per-task the way this algorithm needs. Only
// shadows the macro name used below; every other type stays untouched.
#[cfg(feature = "shuttle")]
use shuttle::thread_local;

/// Bounded convergence attempts in `protect_load` before escalating to the
/// unconditional reservation (mirrors the reference algorithm's 16-attempt
/// fast path before its slow path).
const MAX_LOAD_ATTEMPTS: usize = 16;

/// RAII guard representing an active critical section.
///
/// While a Guard exists, the thread's reservation slot is active,
/// protecting any `Shared<'g, T>` pointer loaded during this critical
/// section: a protected load publishes the epoch it reads when that moved
/// since the last publication. The outermost `pin()` activates the slot,
/// and the drop of the outermost Guard leaves it: the slot becomes
/// inactive, what retirers put in its list is traversed, and the batches
/// that released are freed.
///
/// Nested `pin()` calls are safe: only the outermost pin activates the
/// slot. Inner guards simply share the outer guard's reservation. This prevents re-entrant `pin()` from freeing nodes
/// that an outer guard still references, and the drop of an inner guard
/// leaves the slot active.
///
/// # A thread with no live guard holds nothing back
///
/// A thread with no live Guard has an inactive slot, which no batch is
/// placed in: a thread that pins once and then idles, or parks in a pool,
/// delays no reclamation, however many values other threads retire
/// meanwhile and whatever their birth epochs. Nothing has to be flushed
/// before idling; [`flush()`](crate::flush) is for what the thread itself
/// retired and has not submitted yet.
///
/// The destructors of the values a drop frees run inside the drop, with
/// the slot already inactive and no guard live: a destructor may pin
/// (an outermost pin, which enters the slot for its own section), load and
/// retire, as any code outside a critical section may.
///
/// # A pin after the thread's exit
///
/// A thread-local destructor that runs after kovan's own (the thread's exit
/// is over) may still pin: its guard protects what it loads as any other
/// does. Such a critical section claims a thread ID and a reservation for
/// itself, and when its last guard drops it leaves the protocol as an exit
/// does (submits what it retired, releases the ID). A guard of such a
/// section that is never dropped (`mem::forget`) keeps the reservation and
/// the ID for the rest of the process, since no exit runs to end it.
pub struct Guard {
    _private: (),
    // Psy szczekają, a karawana jedzie dalej.
    marker: marker<*mut ()>,
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        // Decrement the pin count; the outermost drop leaves the slot.
        #[cfg(feature = "nightly")]
        {
            HANDLE.unpin();
        }
        #[cfg(not(feature = "nightly"))]
        {
            // Through the handle; through the exiting handle while the exit
            // runs (a destructor it runs); after the exit, through the late
            // handle the pin made (see `late_pin`).
            if HANDLE.try_with(Handle::unpin).is_err() && on_exiting_handle(Handle::unpin).is_none()
            {
                late_unpin();
            }
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
    /// Number of live Guard instances on this thread. Only the outermost
    /// pin() activates the slot. Guard::drop decrements this; when it
    /// reaches 0 the critical section ends and the slot is left.
    pin_count: Cell<usize>,
    /// Batch state
    batch_first: Cell<*mut RetiredNode>,
    batch_last: Cell<*mut RetiredNode>,
    batch_count: Cell<usize>,
    /// Allocations counted for the epoch clock (see `alloc_birth_epoch`)
    alloc_counter: Cell<usize>,
    /// Cached free list: the zero-count batches a traversal released, freed
    /// by the drain that follows it
    free_list: Cell<*mut RetiredNode>,
    /// The epoch this thread's slot publishes, the `prev` of the algorithm's
    /// `protect`: the era word is written by this thread alone (a protected
    /// load that found the epoch moved, an escalation, the reset at the
    /// leave of an escalated section), each write mirrored here, so the
    /// load's one compare reads no atomic. 0 before the first publication,
    /// below every epoch.
    cached_epoch: Cell<u64>,
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
    /// Fillers this thread added to padded batches (see `try_retire`).
    #[cfg(test)]
    fillers: Cell<usize>,
    /// The handle runs a critical section that began after this thread's
    /// exit was over (a thread-local destructor that runs after kovan's):
    /// the section holds a reservation under an ID of its own, and its end
    /// (the outermost drop's `leave`) runs the exit every thread runs, since
    /// no exit runs for the thread any more. Elsewhere than nightly the
    /// section's handle is a box of its own, dropped after that
    /// (`late::late_unpin`), its exit already run.
    late: Cell<bool>,
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
            cached_epoch: Cell::new(0),
            in_reclaim: Cell::new(false),
            #[cfg(test)]
            fillers: Cell::new(0),
            late: Cell::new(false),
        }
    }

    /// [Alloc] The epoch clock and the birth stamp of a node this thread
    /// allocates: the thread's first allocation and every `EPOCH_FREQ`th
    /// after advance the global epoch (`alloc_cnt++ % FREQ == 0`), and the
    /// birth is the epoch read after that, so a node born after an advance
    /// carries an epoch no reservation published before it holds.
    #[inline]
    fn alloc_birth_epoch(&self) -> u64 {
        let count = self.alloc_counter.get();
        self.alloc_counter.set(count.wrapping_add(1));
        if count.is_multiple_of(EPOCH_FREQ) {
            slot::advance_epoch();
        }
        slot::epoch()
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
                let tid = self.claim_tid();
                // On nightly, #[thread_local] doesn't run Drop.
                // Register a sentinel that calls cleanup() on thread exit.
                // Once the sentinel is gone the exit is over and runs no
                // more: the ID is the late section's, which releases it.
                #[cfg(feature = "nightly")]
                if HANDLE_CLEANUP_SENTINEL.try_with(|_| {}).is_err() {
                    self.late.set(true);
                }
                tid
            }
        }
    }

    /// Claim a thread ID for this handle (`alloc_tid`), which its exit
    /// releases. The exit itself claims through this, not `tid()`: on
    /// nightly it runs inside the sentinel's destructor, which `tid()`
    /// would reach.
    fn claim_tid(&self) -> usize {
        let tid = self.global().alloc_tid();
        self.tid.set(Some(tid));
        tid
    }

    /// Protect: era tracking on load - **wait-free**, bounded at
    /// `MAX_LOAD_ATTEMPTS` convergence iterations plus one escalated load.
    ///
    /// Invariant on return (convergence case): the returned pointer was
    /// loaded *before* observing `global_epoch == cached_epoch`, and
    /// `cached_epoch` is this thread's published slot epoch. Combined these
    /// give
    /// `slot_epoch >= cached_epoch == observed_epoch >= birth_epoch(ptr)`,
    /// so `try_retire()` of any batch that could contain the pointer inserts
    /// into this thread's slot and the batch is deferred until the slot's
    /// next traversal, at the outermost guard's drop: protection is
    /// guard-wide, so this loop never traverses, it only *raises* the
    /// published epoch. A node born after the epoch this thread published
    /// (its allocator read or made a later global epoch before publishing
    /// it) is always met by a later global epoch here, by coherence of the
    /// epoch counter, so its load refreshes the published epoch to at least
    /// its birth before it returns.
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
    /// which forces our post-fence load to observe that batch's unlinks -
    /// we can never read a pointer whose batch skipped us. The reservation
    /// ends with the section: the outermost guard drop leaves the slot and
    /// resets the era word, and the next section's first protected load
    /// publishes the epoch it reads.
    ///
    /// Every publication (`publish`) is ordered before the loads that
    /// follow it and pairs with the `fence(SeqCst)` in `try_retire()`'s scan
    /// phase (store-buffering / Dekker pairing): either the scanner
    /// observes our published value, or our subsequent pointer load
    /// observes the retirer's unlink.
    ///
    /// # Wait-free bound: MAX_LOAD_ATTEMPTS + 1 iterations
    ///
    /// The convergence loop re-iterates only when the global epoch advanced
    /// between the previous publication and the pointer load. After
    /// MAX_LOAD_ATTEMPTS failed attempts the load escalates to the
    /// unconditional reservation and completes with a single further load,
    /// independent of all other threads. Fast path cost: one pointer load,
    /// one epoch load, one compare against the thread-local cache.
    #[inline]
    fn protect_load(&self, data: &AtomicUsize, order: Ordering) -> usize {
        // Hot path: straight-line, no loop in the inlined body.
        // Step 1: load the pointer.
        // XXX: This must be done first since ordering depends on it.
        let ptr = data.load(order);

        // Step 2: read the global epoch.
        let curr_epoch = slot::epoch();

        // Step 3: protected if the epoch is unchanged since our last
        // publication, or if we already hold the unconditional
        // reservation (under which every load is protected). One compare
        // covers both. `cached_epoch` is either an epoch this thread read
        // from the global counter before this load, so `curr_epoch >=
        // cached` by read-read coherence and `<=` means equal; or 0 (no
        // publication yet), below every epoch; or `EPOCH_UNCONDITIONAL`
        // (`u64::MAX`), above every epoch.
        if curr_epoch <= self.cached_epoch.get() {
            return ptr;
        }

        self.protect_load_cold(data, order, curr_epoch)
    }

    /// Convergence + escalation path of `protect_load`, kept out of line so
    /// the hot path stays as small as the original single-load fast path.
    #[cold]
    fn protect_load_cold(&self, data: &AtomicUsize, order: Ordering, first_epoch: u64) -> usize {
        let tid = self.tid();
        let mut curr_epoch = first_epoch;
        let mut attempts = MAX_LOAD_ATTEMPTS;
        // At most MAX_LOAD_ATTEMPTS passes: MAX_LOAD_ATTEMPTS - 1
        // publications, then the escalation, which returns.
        loop {
            attempts -= 1;
            if attempts == 0 {
                // Escalate, then re-read once. Wait-free completion, with no
                // dependence on the epoch stabilizing.
                self.escalate();
                return data.load(order);
            }

            // [Raise] The epoch advanced: publish the new epoch and retry
            // the load. Raising the published epoch never releases
            // protection of pointers loaded earlier in this critical section
            // (their batches stay in the slot list until the outermost drop).
            self.publish(tid, curr_epoch);

            let ptr = data.load(order);
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::LoadAttempt, tid);
            let e = slot::epoch();
            if e == curr_epoch {
                return ptr;
            }
            curr_epoch = e;
        }
    }

    /// [Escalate] Publish the unconditional reservation on this thread's
    /// slot: every load after it in the critical section is protected
    /// whatever the global epoch does. It ends with the section: the
    /// outermost guard drop leaves the slot and resets the era word, and
    /// the next section's first protected load publishes a real epoch.
    #[cold]
    fn escalate(&self) {
        self.publish(self.tid(), EPOCH_UNCONDITIONAL);
    }

    /// [Publish] Write `epoch` to this thread's era word, ordered before
    /// every load that follows (the algorithm's `rsrv.era = era`, a store
    /// every later load must come after). A SeqCst exchange: its acquire
    /// half keeps the loads after it, and on x86-64 the locked instruction
    /// is the full barrier the scan's fence pairs with; elsewhere an
    /// Acquire load may be RCpc, which no earlier write orders, so a SeqCst
    /// fence follows (`publish_fence`). Mirrored in `cached_epoch`.
    #[inline]
    fn publish(&self, tid: usize, epoch: u64) {
        self.global()
            .thread_slots(tid)
            .epoch
            .swap(epoch, Ordering::SeqCst);
        publish_fence();
        self.cached_epoch.set(epoch);
    }

    /// Guard drop: end a critical section.
    ///
    /// The outermost drop (count 1) stores 0 and leaves the slot (`leave`).
    /// A nested drop decrements, saturating: a guard that outlived its
    /// handle (kept by a thread-local destructor that runs after the exit)
    /// drops through another handle, whose count must not wrap.
    #[inline]
    fn unpin(&self) {
        let count = self.pin_count.get();
        if count == 1 {
            self.pin_count.set(0);
            self.leave();
        } else {
            self.pin_count.set(count.saturating_sub(1));
        }
    }

    /// The outermost drop (the count already 0): leave this thread's
    /// reservation, then free what that released.
    ///
    /// [Leave] One exchange of the slot's list word to INVPTR, on the
    /// thread's own line: the slot is inactive from this instant, so no
    /// batch retired from now on is placed in it and no retirer waits for
    /// this thread while it holds no guard. Never a plain store: an insert
    /// that landed between a load and the store would be lost, its batch
    /// counted for a node no traversal takes (a retire that finds INVPTR
    /// behind its exchange rolls its node back, or counts it when this
    /// exchange took it first: `try_retire`). [LeaveTraverse] What the
    /// exchange captured (the nodes every batch placed here while the slot
    /// was active) is traversed onto the free-list cache, with no
    /// destructor running yet. [LeaveDrain] Then the cache is drained with
    /// the slot inactive and the pin count at 0, so a destructor that pins
    /// enters the slot for a section of its own and leaves it at its end,
    /// and one that retires or flushes runs as code outside any section
    /// does. The drain takes the whole cache out of its cell before it frees
    /// anything, so a nested drain finds it empty (`drain_free_list`).
    ///
    /// [LeaveReset] A section that escalated leaves the era word at
    /// `EPOCH_UNCONDITIONAL`; the leave resets it to 0, the era a slot
    /// starts with, so the next section's first protected load publishes
    /// the epoch it reads (a slot at era 0 is eligible for no batch, as at
    /// the start). A plain store: the slot is inactive, and the next
    /// activation's exchange orders it before the slot is read again.
    ///
    /// A late section (see `late`) ends here as an exit does: no exit runs
    /// for its thread any more to submit what it retired and release its ID.
    ///
    /// # Progress: wait-free
    ///
    /// One exchange, one walk of the list it captured (fixed in length by
    /// the exchange, see `reclaim::traverse`), and the frees of the batches
    /// that walk released: the destructors run are this thread's own work,
    /// bounded by the values freed, never by other threads.
    #[inline]
    fn leave(&self) {
        if self.late.get() {
            self.late.set(false);
            self.cleanup();
            return;
        }
        let tid = self.tid();
        let first = self.leave_slot(tid);
        if first != 0 && first != INVPTR {
            unsafe { self.traverse_onto_cache(first as *mut RetiredNode) };
        }
        if !self.free_list.get().is_null() {
            self.drain_free_list();
        }
    }

    /// [Leave] The exchange that leaves this thread's reservation slot:
    /// returns the list word it displaced (0, INVPTR, or a list the caller
    /// traverses), and resets the era word after an escalated section
    /// (`LeaveReset`). Shared by the outermost drop and the exit.
    #[inline]
    fn leave_slot(&self, tid: usize) -> usize {
        let slots = self.global().thread_slots(tid);
        let first = slots.first.swap(INVPTR, Ordering::AcqRel);
        #[cfg(test)]
        crate::stall::at(crate::stall::Step::Left, tid);
        if self.cached_epoch.get() == EPOCH_UNCONDITIONAL {
            slots.epoch.store(0, Ordering::Relaxed);
            self.cached_epoch.set(0);
        }
        first
    }

    /// [Enter] Activate this thread's reservation slot at an outermost
    /// `pin()`: one exchange of the list word to 0, what it held traversed.
    ///
    /// The exchange, never a store: the slot left INVPTR at the last drop
    /// may hold a node a retire exchanged in since and has not rolled back
    /// yet; the exchange takes it, the retire's rollback then fails and
    /// counts the node, and the traversal here balances that count. A store
    /// would lose the node. No epoch is published: the slot keeps the era
    /// word of its last publication (0 before the first, or after an
    /// escalated section), and the first protected load that finds the
    /// global epoch moved publishes it (`protect_load`); a scan meanwhile
    /// treats the slot as reserving that era, which holds every batch it
    /// could need.
    ///
    /// A SeqCst exchange on x86-64 is the full barrier the scan's fence
    /// pairs with: a scan that did not see the slot active completed before
    /// it, so the loads of this section see that batch's unlinks (see
    /// `publish`). Elsewhere `publish_fence` follows.
    ///
    /// # When a thread traverses its own slot list
    ///
    /// Protection is guard-wide: one reservation covers every pointer the
    /// thread loaded since its outermost `pin()`. So the thread traverses
    /// its own list only where no guard of it is live: at the outermost
    /// `pin()` here (the count was 0 and the new section has loaded
    /// nothing), at the outermost drop (`leave`, the count already 0) and
    /// at thread exit. Nested pins never touch the slot, and `protect_load`
    /// only raises the published epoch. A traversal therefore never
    /// releases a pointer loaded earlier in a live section; a batch is
    /// freed only once its count reaches zero, that is once every slot that
    /// took one of its nodes (every reservation that covered it when it was
    /// retired) has been left or re-entered by its owner. The traversal only
    /// moves the released batches onto the cache (`traverse_onto_cache`):
    /// no destructor runs inside `pin()`; the drop of the guard drains them.
    ///
    /// # Progress: wait-free
    ///
    /// One exchange and one walk of the list it captured: the list of a
    /// slot left INVPTR holds at most one node per retire whose insert
    /// found INVPTR behind its exchange and has not rolled back yet.
    #[inline]
    fn enter(&self) {
        let tid = self.tid();
        let first = self
            .global()
            .thread_slots(tid)
            .first
            .swap(0, Ordering::SeqCst);
        publish_fence();
        if first != 0 && first != INVPTR {
            unsafe { self.traverse_onto_cache(first as *mut RetiredNode) };
        }
    }

    /// Pin: enter a critical section.
    ///
    /// Nested pin() calls are safe: only the outermost pin() does real work
    /// (the activation, `enter`). Inner calls just increment the pin count
    /// and return a Guard. Guard::drop decrements the count, and the
    /// outermost drop leaves the slot.
    ///
    /// The outermost pin always activates: the slot is inactive between
    /// sections, and the activation is what makes the slot eligible for the
    /// batches this section's loads must be protected from.
    ///
    /// # Wait-free bound: one exchange
    ///
    /// Plus the walk of what the exchange took (see `enter`), and at a
    /// thread's first pin the claim of a thread ID (`alloc_tid`).
    #[inline]
    fn pin(&self) -> Guard {
        let count = self.pin_count.get();
        self.pin_count.set(count + 1);

        // Nested pin: the outermost guard's reservation protects us.
        if count == 0 {
            self.enter();
        }
        Guard {
            _private: (),
            marker,
        }
    }

    /// Internal: enqueue a pre-configured node into the thread-local batch.
    /// The node's destructor and birth_epoch must already be set by the caller.
    ///
    /// # Wait-free bound: O(1) amortized
    ///
    /// Each call does O(1) work (link node into batch, update counters).
    /// Every RETIRE_FREQ (64) calls: finalize batch + try_retire (O(T)).
    ///
    /// # Safety
    ///
    /// - `node_ptr` must point to a valid, heap-allocated `RetiredNode` at offset 0.
    /// - `destructor` must already be set on the node.
    /// - `birth_epoch` must already be set correctly.
    /// - The node must not be enqueued more than once.
    unsafe fn enqueue_node(&self, node_ptr: *mut RetiredNode) {
        // Relaxed stores throughout: the batch is this thread's alone until
        // `try_retire` publishes it, and another thread reads these words
        // only after reaching the batch through a slot list, with an acquire
        // that synchronizes with the release half of the exchange that
        // inserted a node of it (or of an exchange whose release sequence
        // contains that one: later insertions are RMWs on the same word).
        // These stores are sequenced before that exchange, so they happen
        // before every such read.
        let first = self.batch_first.get();
        if first.is_null() {
            // On nightly a thread's exit (`cleanup`) runs only from the
            // sentinel, which a thread registers when it claims an ID. A
            // thread with no ID that starts a batch registers it here, so
            // its exit runs and submits the batch; without this the batch of
            // a thread that never pinned would never be submitted on nightly.
            #[cfg(feature = "nightly")]
            if self.tid.get().is_none() {
                let _ = HANDLE_CLEANUP_SENTINEL.try_with(|_| {});
            }
            // First node in batch -> becomes batch_last (refs-node)
            self.batch_last.set(node_ptr);
            unsafe {
                (*node_ptr)
                    .batch_link
                    .store(core::ptr::null_mut(), Ordering::Relaxed);
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
                (*node_ptr).batch_link.store(last, Ordering::Relaxed);
                (*node_ptr).set_batch_next(first);
            }
        }

        self.batch_first.set(node_ptr);
        let count = self.batch_count.get() + 1;
        self.batch_count.set(count);

        if count.is_multiple_of(RETIRE_FREQ) {
            // Set in_reclaim: try_retire -> free_batch_list can call
            // destructors which drop Atoms triggering flush().
            let was_reclaiming = self.in_reclaim.get();
            self.in_reclaim.set(true);

            // Detach the batch BEFORE try_retire (see `take_batch`).
            if let Some((first, last)) = self.take_batch() {
                if !self.try_retire(first, last, false) {
                    // Fewer assignable nodes than eligible slots: nothing was
                    // published, so keep accumulating. The merged batch
                    // retries at the next RETIRE_FREQ multiple with more
                    // nodes, and succeeds once batch_size exceeds the
                    // eligible slot count.
                    self.merge_batch(first, last);
                }
            }
            self.in_reclaim.set(was_reclaiming);
        }
    }

    /// Retire a node into the thread-local batch (matches ASMR retire),
    /// storing `destructor` on it: the reclamation walk that frees the
    /// node's batch calls it once, and nothing reads or writes the node
    /// once it has been entered (see `reclaim::free_batch_list`).
    ///
    /// # Safety
    ///
    /// - `node_ptr` must point to a valid, heap-allocated object with a
    ///   `RetiredNode` at offset 0 (a `#[repr(C)]` type with `RetiredNode` as
    ///   its first field).
    /// - `destructor` must be sound to call once on `node_ptr`, on any thread,
    ///   when the node is no longer reachable.
    /// - `node_ptr` must not be retired more than once.
    /// - The caller must not access `*node_ptr` after this call (except
    ///   through the reclamation system).
    #[inline]
    unsafe fn retire_with(&self, node_ptr: *mut RetiredNode, destructor: DestructorFn) {
        // SAFETY: Use raw pointer writes to avoid creating &mut, which would
        // conflict with concurrent readers still holding a guard-protected reference
        // to the outer struct.
        unsafe {
            (*node_ptr).set_destructor(Some(destructor));
        }

        // birth_epoch is already set at allocation time (RetiredNode::new)
        unsafe { self.enqueue_node(node_ptr) };
    }

    /// Detach the accumulating batch, if any, and finalize it: the
    /// refs-node's batch_link becomes RNODE(batch_first). Returns
    /// `(batch_first, refs)`.
    ///
    /// The batch cells are left empty before the caller submits: destructors
    /// running inside try_retire (via free_batch_list) may re-enter
    /// retire()/flush() and must see empty batch cells, never the batch
    /// being submitted (re-submitting it would double-insert and double-free
    /// the whole batch).
    #[inline]
    fn take_batch(&self) -> Option<(*mut RetiredNode, *mut RetiredNode)> {
        let first = self.batch_first.get();
        if first.is_null() {
            return None;
        }
        let last = self.batch_last.get();
        self.batch_first.set(core::ptr::null_mut());
        self.batch_last.set(core::ptr::null_mut());
        self.batch_count.set(0);
        unsafe {
            (*last)
                .batch_link
                .store(rnode_mark(first), Ordering::SeqCst);
        }
        Some((first, last))
    }

    /// Merge a detached batch chain (`first` → … → `refs`) into the
    /// thread-local accumulating batch.
    ///
    /// Used when a retire's `try_retire` could not submit a batch (fewer
    /// assignable nodes than eligible slots). The chain must be unpublished: a failed `try_retire`
    /// aborts in the scan phase, before any node is inserted into a slot,
    /// so every node in the chain is still exclusively owned by this thread.
    ///
    /// All writes are thread-local. If the current batch is empty the chain
    /// is installed as-is (its refs-node keeps the REFC_PROTECT bias and the
    /// batch's min birth-epoch; the stale RNODE batch_link from the failed
    /// finalization is rewritten at the next finalization). Otherwise the
    /// chain's nodes are re-pointed at the current refs-node and the old
    /// refs-node becomes a regular node - its bias word is reused as the
    /// batch_next link, exactly as for any non-refs node.
    fn merge_batch(&self, first: *mut RetiredNode, refs: *mut RetiredNode) {
        // Count the chain (first -> ... -> refs): this thread's own,
        // unpublished, so no other thread changes its length.
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
        // surviving refs-node. Relaxed, as in `enqueue_node`: the chain is
        // unpublished and its readers acquire it through a slot list.
        let mut cur = first;
        loop {
            unsafe { (*cur).batch_link.store(cur_refs, Ordering::Relaxed) };
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

    /// Try to retire a finalized batch chain by scanning all thread slots.
    /// Matches ASMR try_retire.
    ///
    /// Returns `false` when the batch has fewer assignable nodes than there
    /// are eligible slots and `pad` is false. In that case **nothing has
    /// been published** (the scan phase aborts before the insert phase) and
    /// the caller must keep the chain: merge it back into the accumulating
    /// batch. Silently dropping it would leak the entire batch.
    ///
    /// With `pad` (a flush or an exit, which must not keep the batch) the
    /// scan never runs out of nodes: for every eligible slot beyond the
    /// batch's nodes it adds a filler to the batch (`filler`), so the batch
    /// is always placed. A filler is a bare node with no value whose
    /// destructor frees it, freed with its batch; `free_batch_list` calls it
    /// as it calls every destructor, so a filler never runs a user's.
    ///
    /// # Wait-free bound: O(T + batch_size) where T = number of active threads
    ///
    /// Two phases, both bounded:
    /// - **Scan phase**: O(T) - iterates all thread slots once, assigning
    ///   batch nodes to active slots with epoch ≥ min_epoch.
    ///   No loops within - each slot is visited exactly once (one filler
    ///   allocated per eligible slot beyond the batch's nodes, with `pad`).
    /// - **Insert phase**: O(batch_size) - iterates batch nodes, exchanging each
    ///   into its assigned slot. The exchange is a single atomic instruction (on
    ///   native platforms). Contention handling (INVPTR rollback, list tainting)
    ///   is O(1) per node.
    fn try_retire(&self, batch_first: *mut RetiredNode, refs: *mut RetiredNode, pad: bool) -> bool {
        let global = self.global();
        let max_threads = global.max_threads();

        let mut curr = batch_first;
        let min_epoch = unsafe { (*refs).birth_epoch() };

        // === Scan phase: assign slots to batch nodes ===
        // SeqCst fence: pairs with the publications of the slots (an
        // activation, an epoch), see `publish`.
        fence(Ordering::SeqCst);
        let mut last = curr;
        // One pass over the T slots, each visited once.
        for i in 0..max_threads {
            let slots = global.thread_slots(i);
            if slots.first.load(Ordering::Acquire) == INVPTR {
                continue;
            }
            if slots.epoch.load(Ordering::Acquire) < min_epoch {
                continue;
            }
            if last == refs {
                if !pad {
                    return false; // Not enough batch nodes for all eligible slots
                }
                // [Pad] A filler takes this slot, linked ahead of the
                // batch's first node (the insert walks from `curr`).
                curr = self.filler(curr, refs, min_epoch);
                unsafe { (*curr).set_slot_info(i) };
                continue;
            }
            unsafe {
                (*last).set_slot_info(i);
            }
            last = unsafe { (*last).batch_next() };
        }
        if curr != batch_first {
            // [PadMark] Fillers head the batch now: the refs-node's mark
            // names the new first node, which `free_batch_list` walks from.
            // Relaxed as every write to the unpublished batch (see
            // `enqueue_node`): the inserting exchanges release it.
            unsafe {
                (*refs)
                    .batch_link
                    .store(rnode_mark(curr), Ordering::Relaxed)
            };
        }

        // === Insert phase: exchange into slots ===
        let mut adjs: usize = REFC_PROTECT.wrapping_neg(); // -REFC_PROTECT

        // One pass over the nodes the scan assigned. Per node: at most one
        // exchange, one rollback CAS and one link CAS, none retried; a
        // failed link walks the list the node displaced, which the slot's
        // owner captured together with the node: fixed in length.
        while curr != last {
            let slots = global.thread_slots(unsafe { (*curr).get_slot_info() });

            // Set next to null before insertion
            unsafe {
                (*curr).next.store(core::ptr::null_mut(), Ordering::Relaxed);
            }

            // Check slot is still active
            if slots.first.load(Ordering::Acquire) == INVPTR {
                curr = unsafe { (*curr).batch_next() };
                continue;
            }

            // Re-check epoch
            if slots.epoch.load(Ordering::Acquire) < min_epoch {
                curr = unsafe { (*curr).batch_next() };
                continue;
            }

            // Exchange our node in as the new list head
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::InsertReady, self.tid());
            let prev = slots.first.swap(curr as usize, Ordering::AcqRel);
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::InsertExchanged, self.tid());

            if prev != 0 {
                if prev == INVPTR {
                    // Slot was inactive (its thread left it) - undo by
                    // restoring the INVPTR sentinel, as in the reference
                    // try_retire rollback. Restoring 0 here would resurrect
                    // a dead slot as active-empty and break the `INVPTR ==
                    // inactive` invariant.
                    // [InsertRollback] A compare-exchange of the list word:
                    // the node still heads the list or it does not.
                    if slots
                        .first
                        .compare_exchange(
                            curr as usize,
                            INVPTR,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        // Undo succeeded - node removed from slot.
                        curr = unsafe { (*curr).batch_next() };
                        continue;
                    }
                    // Undo failed - node was captured by a concurrent
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
            // refs reached 0 - free immediately
            unsafe {
                (*refs).next.store(core::ptr::null_mut(), Ordering::Relaxed);
                crate::reclaim::free_batch_list(refs);
            }
        }
        true
    }

    /// A filler for a padded batch (see `try_retire`): a bare node, linked
    /// ahead of `first` as the batch's new first node, whose destructor
    /// frees it. Its birth is the batch's, which the insert never reads.
    /// A `Box` of its own, not a node of the magazine cache: a filler is
    /// kovan's own bookkeeping, rare (a flush or an exit short of nodes),
    /// and must not change what a cache class holds. Returns the filler,
    /// now the batch's first node.
    fn filler(
        &self,
        first: *mut RetiredNode,
        refs: *mut RetiredNode,
        birth: u64,
    ) -> *mut RetiredNode {
        let node = Box::into_raw(Box::new(RetiredNode {
            next: core::sync::atomic::AtomicPtr::new(core::ptr::null_mut()),
            batch_link: core::sync::atomic::AtomicPtr::new(refs),
            refs_or_next: core::sync::atomic::AtomicUsize::new(first as usize),
            birth_epoch: core::cell::UnsafeCell::new(birth),
            destructor: core::cell::UnsafeCell::new(Some(drop_filler)),
        }));
        #[cfg(test)]
        self.fillers.set(self.fillers.get() + 1);
        node
    }

    /// Traverse a captured slot list onto this thread's free-list cache and
    /// free nothing: no destructor runs. Step `Traverse`, the one every
    /// traversal takes: `reclaim::traverse` over the captured list, whose
    /// length was fixed by the exchange that captured it.
    ///
    /// # Safety
    ///
    /// `first` must be a slot list head captured by exchange (not null, not
    /// INVPTR), as `reclaim::traverse` requires.
    unsafe fn traverse_onto_cache(&self, first: *mut RetiredNode) {
        #[cfg(test)]
        crate::stall::at(crate::stall::Step::Traverse, self.tid());
        let mut free_list = self.free_list.get();
        unsafe { crate::reclaim::traverse(&mut free_list, first) };
        self.free_list.set(free_list);
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
        // Another pass only for what the destructors the previous pass ran
        // pushed onto the cache through kovan calls of their own (a section
        // of their own, whose pin is outermost): bounded by those
        // destructors, never by other threads.
        loop {
            let free_list = self.free_list.get();
            if free_list.is_null() {
                break;
            }
            self.free_list.set(core::ptr::null_mut());
            unsafe {
                crate::reclaim::free_batch_list(free_list);
            }
        }
        self.in_reclaim.set(was_reclaiming);
    }

    /// Runs `flush` when this thread's batch holds a retired node.
    fn flush_pending(&self) {
        if self.batch_count.get() > 0 {
            self.flush();
        }
    }

    /// Flush: submit every value this thread retired and free what that
    /// releases, so that the thread returns holding no retired node.
    ///
    /// [FlushPlace] The partial batch (fewer than RETIRE_FREQ nodes, never
    /// published) goes out through `try_retire`, padded so it is always
    /// placed: other threads may still hold guard-protected references to
    /// its values, so the normal epoch-based checks apply and nothing is
    /// just dropped. [FlushAdvance] A placed batch advances the epoch (the
    /// pending slow paths helped first, as every advance is). [FlushDrain]
    /// The free-list cache is drained. The destructors these steps run may
    /// retire (into the batch, emptied by the placement) and so the three
    /// steps repeat while the batch holds a value: bounded by the
    /// destructors the thread's own values run, never by other threads,
    /// since the thread's slot is inactive with no guard live and takes no
    /// node of another thread's batch meanwhile.
    ///
    /// Destructors run with the pin count as the caller left it: outside
    /// any section their pins are outermost (they enter and leave the slot
    /// for their own section); inside one they are nested under the
    /// caller's reservation, which protects their loads.
    ///
    /// This does NOT guarantee that nodes retired by OTHER threads are freed:
    /// those threads must flush themselves or exit. A thread that has no ID
    /// (it never pinned) and nothing in its batch returns at once; with a
    /// value in its batch it claims an ID first, as its first pin would.
    fn flush(&self) {
        // Skip if we're already inside a reclamation operation (e.g. a
        // destructor from free_batch_list dropped an Atom which calls flush).
        // The nodes will be reclaimed by the outer operation or the next
        // flush/cleanup call. This prevents reading stale free_list Cell
        // pointers that the outer operation is actively freeing.
        if self.in_reclaim.get() {
            return;
        }
        self.in_reclaim.set(true);
        self.place_all();
        self.in_reclaim.set(false);
    }

    /// [FlushPlace] and [FlushDrain], repeated while the batch holds a
    /// value: the one placement loop of `flush` and of the exit. A
    /// placement needs no thread ID: the batch goes to the slots of the
    /// threads that hold reservations, and the epoch moves with allocations.
    fn place_all(&self) {
        // Another pass only for what the destructors of this pass retired:
        // bounded by those destructors, never by other threads.
        loop {
            if let Some((first, last)) = self.take_batch() {
                let placed = self.try_retire(first, last, true);
                debug_assert!(placed, "a padded batch was not placed");
            }
            self.drain_free_list();
            if self.batch_first.get().is_null() {
                return;
            }
        }
    }
}

impl Handle {
    /// Cleanup thread-local state. Called on thread exit.
    /// Extracted from Drop so it can also be called by the nightly sentinel.
    fn cleanup(&self) {
        // A thread with neither an ID nor a batch holds nothing: a thread
        // that never pinned nor retired, or an exit already run.
        if self.tid.get().is_none() && self.batch_first.get().is_null() {
            return;
        }
        // The destructors this exit runs reach this handle, `HANDLE`
        // being the thread-local whose destructor runs it.
        #[cfg(not(feature = "nightly"))]
        let _ = EXITING.try_with(|exiting| exiting.set(self));
        // Prevent re-entrant flush() from destructors during cleanup.
        self.in_reclaim.set(true);
        // A guard still live here was never dropped (`mem::forget`): no
        // code can use it any more, so its section ends here, and the
        // destructors below run outside any section, as `leave`'s do.
        self.pin_count.set(0);

        // [ExitLeave] The slot is left as the outermost drop leaves it:
        // one exchange, what it captured traversed onto the cache. From
        // here no batch is placed in this thread's slot.
        if let Some(tid) = self.tid.get() {
            let first = self.leave_slot(tid);
            if first != 0 && first != INVPTR {
                unsafe { self.traverse_onto_cache(first as *mut RetiredNode) };
            }
        }

        // [ExitPlace] What the thread holds is placed and freed as a
        // flush does, until it holds nothing: the batches the traversal
        // released, the partial batch, and whatever the destructors of
        // those retire. The slot is inactive, so no other thread's
        // retire lands in it meanwhile, and the exit is bounded by the
        // thread's own values. A destructor that pins claims an ID for its
        // section if the thread has none, and leaves its slot at its end.
        self.place_all();

        // Only now may another thread take this ID over, after every
        // destructor this exit ran: a destructor may pin and load, and
        // its section enters and leaves this ID's slot. Released
        // earlier, the ID could already belong to a new thread, whose
        // reservation that section would overwrite.
        if let Some(tid) = self.tid.get() {
            self.global().release_tid(tid);
        }

        self.in_reclaim.set(false);

        // Mark TID as unused so cleanup is idempotent. The slot is gone:
        // a later pin on this thread (a TLS destructor running after
        // this one) must allocate a tid and enter before any load is
        // protected, so no cached epoch may claim a publication.
        self.tid.set(None);
        self.cached_epoch.set(0);
        #[cfg(not(feature = "nightly"))]
        let _ = EXITING.try_with(|exiting| exiting.set(core::ptr::null()));
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(not(feature = "nightly"))]
mod late;
#[cfg(not(feature = "nightly"))]
use late::{late_pin, late_unpin, on_late_handle};

#[cfg(test)]
mod tests;

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
    /// This thread's handle while its exit runs, null otherwise. `HANDLE`
    /// no longer answers then (its destructor is the exit), yet the
    /// destructors the exit runs are critical sections and retirers like any
    /// others and must reach the handle: through this. It has no destructor,
    /// so it answers during every thread-local destructor.
    static EXITING: Cell<*const Handle> = const { Cell::new(core::ptr::null()) };
}

/// Run `f` on this thread's handle if its exit is running (see `EXITING`).
#[cfg(not(feature = "nightly"))]
#[cold]
fn on_exiting_handle<R>(f: impl FnOnce(&Handle) -> R) -> Option<R> {
    let handle = EXITING.try_with(Cell::get).unwrap_or(core::ptr::null());
    // SAFETY: `cleanup` stores its own handle here and clears it before it
    // returns, on this thread; the handle outlives the store.
    (!handle.is_null()).then(|| f(unsafe { &*handle }))
}

/// Whether this thread's exit is still to run (nightly: the sentinel whose
/// destructor runs it is still registered). A retire after the exit goes to
/// a handle of its own, since the exit that would place it has run.
#[cfg(feature = "nightly")]
#[inline]
fn exit_pending() -> bool {
    HANDLE_CLEANUP_SENTINEL.try_with(|_| {}).is_ok()
}

/// The epoch clock and the birth stamp of a node allocated on this thread
/// (`RetiredNode::new`): see `Handle::alloc_birth_epoch`.
#[inline]
pub(crate) fn alloc_birth_epoch() -> u64 {
    #[cfg(feature = "nightly")]
    {
        HANDLE.alloc_birth_epoch()
    }
    #[cfg(not(feature = "nightly"))]
    {
        // During process teardown TLS may be destroyed. Fall back to the
        // exiting handle, else to the late section's, else to the global
        // counter alone (a birth no advance precedes, always correct).
        HANDLE
            .try_with(|handle| handle.alloc_birth_epoch())
            .unwrap_or_else(|_| {
                on_exiting_handle(Handle::alloc_birth_epoch)
                    .or_else(|| on_late_handle(Handle::alloc_birth_epoch))
                    .unwrap_or_else(slot::epoch)
            })
    }
}

/// The barrier after a publication of this thread's slot (an activation,
/// an epoch) and before the loads it must precede. On x86-64 the SeqCst
/// read-modify-write that published is a locked instruction, the full
/// barrier that `try_retire`'s scan fence pairs with, and its acquire half
/// keeps the later loads after it. Elsewhere an Acquire load may be RCpc
/// (AArch64 LDAPR), which an earlier write does not order, so a SeqCst
/// fence follows.
#[inline(always)]
fn publish_fence() {
    #[cfg(not(target_arch = "x86_64"))]
    fence(Ordering::SeqCst);
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
        // After `HANDLE` is destroyed: the exiting handle (a destructor the
        // exit runs), else the late section's (a destructor that runs after
        // the exit). With neither, no guard is live on this thread, which
        // the caller's contract rules out: a plain load.
        HANDLE
            .try_with(|handle| handle.protect_load(data, order))
            .unwrap_or_else(|_| {
                on_exiting_handle(|handle| handle.protect_load(data, order))
                    .or_else(|| on_late_handle(|handle| handle.protect_load(data, order)))
                    .unwrap_or_else(|| data.load(order))
            })
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
        // After `HANDLE` is destroyed: pin the exiting handle (a destructor
        // the exit runs), else start a late section (a destructor that runs
        // after the exit), which protects its loads as any section does.
        //
        // unwrap_or_else (lazy), NOT unwrap_or: unwrap_or evaluates its
        // argument eagerly, running the fallback on every successful call.
        HANDLE
            .try_with(|handle| handle.pin())
            .unwrap_or_else(|_| on_exiting_handle(Handle::pin).unwrap_or_else(late_pin))
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
///   thread happens to traverse the slot the batch landed in - typically
///   **not** the retiring thread. The caller must
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
    // SAFETY: Caller upholds the safety contract.
    unsafe { retire_with(ptr as *mut RetiredNode, drop_boxed::<T>) }
}

/// Retire a node whose deallocation is its [`Reclaimable::dealloc`].
///
/// Identical to [`retire`] in every respect (same batching, same wait-free
/// bounds, same cross-thread destructor, same handling of a thread that is
/// exiting) except for the destructor it installs: when the node's batch is
/// reclaimed, kovan calls `<T as Reclaimable>::dealloc(ptr)` instead of
/// dropping a `Box<T>`. With the default `dealloc` the two are the same. An
/// override can recycle the node into a free list, so a structure that
/// publishes one node per update allocates nothing in the steady state.
///
/// If the call is made while the process is tearing down and no thread
/// handle is left to take the node, it is leaked exactly as `retire` leaks
/// it, and `dealloc` is never called for it.
///
/// # Safety
///
/// Everything [`retire`] requires, and in addition:
///
/// - `T` must be `#[repr(C)]` with a `RetiredNode` as its first field, as
///   [`Reclaimable`] requires.
/// - `T::dealloc` must be sound to call once on `ptr` from any thread, and
///   must not panic (a panic is caught and discarded under `std` and the
///   node is leaked; without `std` it unwinds out of kovan's reclamation
///   code and leaks the rest of the batch).
/// - After kovan calls `T::dealloc(ptr)` it never reads or writes `*ptr`
///   again, so `dealloc` owns the whole allocation (the embedded
///   `RetiredNode` included) from its first instruction and may hand it to
///   another thread at once. See [`Reclaimable`] for the recycling contract
///   and for what to do before a recycled node is published again.
///
/// # Examples
///
/// ```
/// use kovan::{Atomic, Shared, pin, retire_reclaimable, Reclaimable, RetiredNode};
/// use std::sync::atomic::Ordering;
///
/// #[repr(C)]
/// struct Node {
///     retired: RetiredNode,
///     value: u64,
/// }
///
/// // SAFETY: `RetiredNode` is the first field of a `#[repr(C)]` struct.
/// unsafe impl Reclaimable for Node {
///     fn retired_node(&self) -> &RetiredNode {
///         &self.retired
///     }
///     fn retired_node_mut(&mut self) -> &mut RetiredNode {
///         &mut self.retired
///     }
/// }
///
/// let new = |value| Box::into_raw(Box::new(Node { retired: RetiredNode::new(), value }));
/// let slot = Atomic::new(new(1));
/// let guard = pin();
/// let old = slot.swap(unsafe { Shared::from_raw(new(2)) }, Ordering::AcqRel, &guard);
/// // SAFETY: `old` is unlinked from `slot` and retired exactly once.
/// unsafe { retire_reclaimable(old.as_raw()) };
/// # drop(guard);
/// # let guard = pin();
/// # let last = slot.swap(unsafe { Shared::from_raw(std::ptr::null_mut()) }, Ordering::AcqRel, &guard);
/// # unsafe { retire_reclaimable(last.as_raw()) };
/// ```
#[inline]
pub unsafe fn retire_reclaimable<T: Reclaimable + 'static>(ptr: *mut T) {
    // SAFETY: Caller upholds the safety contract.
    unsafe { retire_with(ptr as *mut RetiredNode, dealloc_reclaimable::<T>) }
}

/// Type-erased destructor installed by [`retire`]: frees the `Box<T>`.
unsafe fn drop_boxed<T>(ptr: *mut RetiredNode) {
    unsafe { drop(Box::from_raw(ptr as *mut T)) }
}

/// The destructor of a filler (see `Handle::filler`): frees the bare node,
/// which holds no value. Runs where every destructor of its batch runs.
unsafe fn drop_filler(ptr: *mut RetiredNode) {
    // SAFETY: a filler is a `Box<RetiredNode>`, reached by nothing once its
    // batch is freed, and freed this once.
    unsafe { drop(Box::from_raw(ptr)) }
}

/// Type-erased destructor installed by [`retire_reclaimable`].
unsafe fn dealloc_reclaimable<T: Reclaimable>(ptr: *mut RetiredNode) {
    unsafe { <T as Reclaimable>::dealloc(ptr as *mut T) }
}

/// The one retire path both public entry points share: the nightly
/// thread-local handle, the stable one, and the exiting thread's handle.
#[inline]
pub(crate) unsafe fn retire_with(node_ptr: *mut RetiredNode, destructor: DestructorFn) {
    #[cfg(feature = "nightly")]
    {
        // The handle outlives the sentinel whose destructor runs the exit,
        // and takes the retires of the destructors that exit runs. Once the
        // exit is over, a retire (from a thread-local destructor that runs
        // after it) would fill a batch no exit submits any more.
        // A late section's retires go to its handle, whose exit (the
        // section's end) submits them.
        if exit_pending() || HANDLE.in_reclaim.get() || HANDLE.late.get() {
            // SAFETY: Caller upholds the safety contract.
            unsafe { HANDLE.retire_with(node_ptr, destructor) }
        } else {
            // SAFETY: Caller upholds the safety contract.
            unsafe { retire_after_exit(node_ptr, destructor) }
        }
    }
    #[cfg(not(feature = "nightly"))]
    {
        // SAFETY: Caller upholds the safety contract.
        // Through the handle; through the exiting handle while the exit runs
        // (a destructor it runs); after the exit (a thread-local destructor
        // that runs after the handle's), through the late section's handle
        // if one is live, else through a handle of its own.
        if HANDLE
            .try_with(|handle| unsafe { handle.retire_with(node_ptr, destructor) })
            .is_err()
            && on_exiting_handle(|handle| unsafe { handle.retire_with(node_ptr, destructor) })
                .is_none()
            && on_late_handle(|handle| unsafe { handle.retire_with(node_ptr, destructor) })
                .is_none()
        {
            unsafe { retire_after_exit(node_ptr, destructor) };
        }
    }
}

/// A retire after this thread's exit ran: a handle of its own takes the
/// node, and its exit, as it is dropped, places it padded, as any thread's
/// exit does with the batch it leaves; no thread ID is needed for that.
///
/// # Safety
///
/// As [`retire_with`].
#[cold]
unsafe fn retire_after_exit(node_ptr: *mut RetiredNode, destructor: DestructorFn) {
    let late = Handle::new();
    // SAFETY: forwarded from the caller.
    unsafe { late.retire_with(node_ptr, destructor) };
}

/// Flush all retired nodes on the calling thread.
///
/// Submits the thread's partial batch (padded with fillers, so it is always
/// placed, whatever other threads' reservations hold) and frees what that
/// released, repeating while the destructors it ran retired more: it returns
/// holding no retired node. A thread that retired nothing since its batch
/// was last submitted returns at once (see [`flush_pending`]).
///
/// Call this before dropping data structures that use kovan (e.g. at the end of
/// a test or before process exit) to ensure retired nodes are freed promptly.
///
/// A thread's reservation needs no flush: it ends with the drop of the
/// thread's outermost [`Guard`], which frees what the reservation held back.
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
        // After `HANDLE` is destroyed: the exiting handle (a no-op: its exit
        // is reclaiming), else a live late section's, else nothing.
        if HANDLE.try_with(|handle| handle.flush()).is_err()
            && on_exiting_handle(Handle::flush).is_none()
        {
            on_late_handle(Handle::flush);
        }
    }
}

/// Submits this thread's batch of retired nodes to reclamation now, as
/// [`flush`] does, when it holds one; with nothing retired on this thread
/// since its batch was last submitted it does nothing. A structure's drop
/// calls it: what its thread retired goes to reclamation without waiting for
/// the batch to fill, and a drop that retired nothing costs nothing.
pub fn flush_pending() {
    #[cfg(feature = "nightly")]
    {
        HANDLE.flush_pending()
    }
    #[cfg(not(feature = "nightly"))]
    {
        if HANDLE.try_with(|handle| handle.flush_pending()).is_err()
            && on_exiting_handle(Handle::flush_pending).is_none()
        {
            on_late_handle(Handle::flush_pending);
        }
    }
}
