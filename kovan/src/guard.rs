//! Guard and Handle for critical section management.
//!
//! Implements the ASMR read/reserve_slot/retire protocol:
//! - Pin: an outermost pin enters the thread's slot (do_update: one exchange
//!   of the list word, the epoch published), retried while the epoch moves
//! - Pin (slow path): set up helping state, loop until stable epoch
//! - Guard drop: an outermost drop leaves the slot (one exchange of the list
//!   word to INVPTR), traverses what it held and frees what that released
//! - Retire: batch construction, try_retire with epoch-based slot scanning;
//!   a flush or an exit pads its batch so it is always placed
//! - Helping: help_read/help_thread for wait-free progress

use crate::atomic::AtomicUsize;
use crate::reclaim::Reclaimable;
use crate::retired::{DestructorFn, INVPTR, REFC_PROTECT, RetiredNode, rnode_mark};
use crate::slot::{self, ASMRState, EPOCH_FREQ, EPOCH_UNCONDITIONAL, RETIRE_FREQ, ThreadSlots};
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
/// While a Guard exists, the thread's reservation slot is active and
/// publishes an epoch, protecting any `Shared<'g, T>` pointer loaded
/// during this critical section. The outermost `pin()` enters the slot
/// (activates it and publishes the current epoch), and the drop of the
/// outermost Guard leaves it: the slot becomes inactive, what retirers put
/// in its list is traversed, and the batches that released are freed.
///
/// Nested `pin()` calls are safe: only the outermost pin does real
/// epoch work. Inner guards simply share the outer guard's epoch
/// protection. This prevents re-entrant `pin()` from freeing nodes
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
    #[inline(always)]
    fn drop(&mut self) {
        // Decrement the pin count; the outermost drop leaves the slot.
        with_handle(Handle::unpin);
    }
}

/// Thread-local handle for ASMR state.
///
/// Manages thread ID, batch accumulation, and cached free lists. A
/// thread-local with no destructor, so every access is one address and it
/// answers during every thread-local destructor, the exit's own included
/// (the exit runs from the destructor of `HANDLE_CLEANUP_SENTINEL`, which a
/// thread registers when it claims an ID or starts a batch).
struct Handle {
    global: Cell<Option<&'static ASMRState>>,
    /// Thread ID (lazily allocated)
    tid: Cell<Option<usize>>,
    /// This thread's slots, the ones of `tid`: null until the ID is claimed,
    /// so the activation and the leave reach the slot's words with one load
    /// and no page-table walk.
    slots: Cell<*const ThreadSlots>,
    /// Thread IDs this handle claimed (`claim_tid`), for the tests.
    #[cfg(test)]
    claims: Cell<usize>,
    /// Number of live Guard instances on this thread.
    /// Only the outermost pin() checks/transitions the epoch slot.
    /// Guard::drop decrements this. When it reaches 0, the critical
    /// section ends and the next pin() call may transition the epoch.
    pin_count: Cell<usize>,
    /// Batch state
    batch_first: Cell<*mut RetiredNode>,
    batch_last: Cell<*mut RetiredNode>,
    batch_count: Cell<usize>,
    /// Retires counted for epoch advancement (see `enqueue_node`)
    alloc_counter: Cell<usize>,
    /// Cached free list and count
    free_list: Cell<*mut RetiredNode>,
    list_count: Cell<usize>,
    /// Cached epoch: mirrors `slots.epoch[0]` to avoid atomic read of
    /// 128-bit WordPair on every load. Written where the slot's epoch is
    /// (every outermost `pin()`, a raise, the slow path), read by
    /// `protect_load`'s one compare, which runs only under a live guard.
    cached_epoch: Cell<u64>,
    /// Thread-cached global epoch for stamping `birth_epoch` on allocation,
    /// avoiding a contended `Acquire` load of the global epoch counter on
    /// every `RetiredNode::new`. Refreshed at every outermost `pin()`
    /// (which every `Atom::new`/`store`/`swap` performs right after boxing
    /// the node, and which read/CAS workloads perform constantly) with the
    /// epoch the pin publishes, and at every epoch advance this thread
    /// makes (`increment_era`), with the epoch it advanced to, so a thread
    /// that retires without pinning still stamps recent births. Seeded
    /// lazily on first use. A stale value is
    /// always *low*, the global epoch being monotone, which lowers a
    /// batch's `min_epoch`, making more slots eligible in `try_retire`
    /// (strictly more conservative deferral). It can never exceed the true
    /// epoch, so it can never cause premature reclamation.
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
    /// Fillers this thread added to padded batches (see `try_retire`).
    #[cfg(test)]
    fillers: Cell<usize>,
    /// Calls of `try_retire` live on this thread, for the test that no
    /// destructor runs inside one.
    #[cfg(test)]
    in_try_retire: Cell<u32>,
    /// The handle runs a critical section that began after this thread's
    /// exit was over (a thread-local destructor that runs after kovan's):
    /// the section holds a reservation under an ID of its own, and its end
    /// (the outermost drop's `leave`) runs the exit every thread runs, since
    /// no exit runs for the thread any more.
    late: Cell<bool>,
    /// This thread's exit (`cleanup`) has run: a retire from now on, outside
    /// a late section and the exit's own destructors, goes to a handle of
    /// its own, whose exit places it (`retire_after_exit`), since no exit
    /// would submit this handle's batch any more.
    exited: Cell<bool>,
}

impl Handle {
    const fn new() -> Self {
        Self {
            global: Cell::new(None),
            tid: Cell::new(None),
            slots: Cell::new(core::ptr::null()),
            #[cfg(test)]
            claims: Cell::new(0),
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
            #[cfg(test)]
            fillers: Cell::new(0),
            #[cfg(test)]
            in_try_retire: Cell::new(0),
            late: Cell::new(false),
            exited: Cell::new(false),
        }
    }

    /// Whether this thread's exit has run and is over: no late section is
    /// live and no destructor of the exit is running, so nothing would
    /// submit a retire into this handle's batch any more.
    #[inline]
    fn exit_over(&self) -> bool {
        self.exited.get() && !self.in_reclaim.get() && !self.late.get()
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
        let e = slot::epoch();
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
                let tid = self.claim_tid();
                // Register the sentinel whose destructor runs the exit. Once
                // the sentinel is gone the exit is over and runs no more:
                // the ID is a late section's, which releases it at its end.
                if !exit_pending() {
                    self.late.set(true);
                }
                tid
            }
        }
    }

    /// Claim a thread ID for this handle (`alloc_tid`), which its exit
    /// releases. The exit itself claims through this, not `tid()`.
    fn claim_tid(&self) -> usize {
        let tid = self.global().alloc_tid();
        self.tid.set(Some(tid));
        self.slots.set(self.global().thread_slots(tid));
        #[cfg(test)]
        self.claims.set(self.claims.get() + 1);
        tid
    }

    /// This thread's slots, claimed with its ID at the first use.
    #[inline(always)]
    fn slots(&self) -> &'static ThreadSlots {
        let slots = self.slots.get();
        if slots.is_null() {
            return self.claim_slots();
        }
        // SAFETY: set by `claim_tid` to a slot group of the global table,
        // which lives for the rest of the process.
        unsafe { &*slots }
    }

    #[cold]
    fn claim_slots(&self) -> &'static ThreadSlots {
        self.tid();
        // SAFETY: as `slots`, set by the claim above.
        unsafe { &*self.slots.get() }
    }

    /// Protect: era tracking on load - **wait-free**, bounded at
    /// `MAX_LOAD_ATTEMPTS` convergence iterations plus one escalated load.
    ///
    /// Invariant on return (convergence case): the returned pointer was
    /// loaded *before* observing `global_epoch == cached_epoch`, and
    /// `cached_epoch` is a lower bound of this thread's published slot
    /// epoch. Combined these give
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
    /// ends with the section: the outermost guard drop leaves the slot, and
    /// the next outermost `pin()` publishes a real epoch.
    ///
    /// The `fence(SeqCst)` after each slot publication pairs with the
    /// `fence(SeqCst)` in `try_retire()`'s scan phase (store-buffering /
    /// Dekker pairing): either the scanner observes our published value, or
    /// our subsequent pointer load observes the retirer's unlink. A SeqCst
    /// store alone is not sufficient - a later non-SeqCst load may be
    /// reordered above it in the C++/Rust model (and concretely on ARMv8.3+
    /// where acquire loads lower to LDAPR, which may pass an earlier STLR).
    ///
    /// # Wait-free bound: MAX_LOAD_ATTEMPTS + 1 iterations
    ///
    /// The convergence loop re-iterates only when the global epoch advanced
    /// between the previous publication and the pointer load. After
    /// MAX_LOAD_ATTEMPTS failed attempts the load escalates to the
    /// unconditional reservation and completes with a single further load,
    /// independent of all other threads. Fast path cost: one pointer load,
    /// one epoch load, one compare against the thread-local cache.
    #[inline(always)]
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
        // covers both. `cached_epoch` is either an epoch read from the
        // global counter before this load (by this thread, or by a helper
        // whose result this thread acquired), so `curr_epoch >= cached` by
        // read-read coherence and `<=` means equal; or 0 (no publication
        // yet), below every epoch; or `EPOCH_UNCONDITIONAL` (just below the
        // inactive tag), above every epoch.
        if curr_epoch <= self.cached_epoch.get() {
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

            // The epoch advanced: publish the new epoch and retry the load.
            // Raising the published epoch never releases protection of
            // pointers loaded earlier in this critical section (their
            // batches stay in the slot list until the outermost drop).
            slots.epoch[0].store_lo(curr_epoch, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            self.cached_epoch.set(curr_epoch);

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

    /// Publish the unconditional reservation on this thread's slot: every
    /// load after it in the critical section is protected whatever the
    /// global epoch does. It ends with the section: the outermost guard
    /// drop leaves the slot, and the next outermost `pin()` publishes a
    /// real epoch.
    #[cold]
    fn escalate(&self) {
        let slots = self.global().thread_slots(self.tid());
        slots.epoch[0].store_lo(EPOCH_UNCONDITIONAL, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        self.cached_epoch.set(EPOCH_UNCONDITIONAL);
    }

    /// Guard drop: end a critical section.
    ///
    /// The outermost drop (count 1) stores 0 and leaves the slot (`leave`).
    /// A nested drop decrements, saturating: a guard that outlived its
    /// handle (kept by a thread-local destructor that runs after the exit)
    /// drops through another handle, whose count must not wrap.
    #[inline(always)]
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
    /// [LeaveMark] One store tags the era word (`ERA_INACTIVE`, the escalated
    /// era reset to 0), then [Leave] one exchange of the slot's list word to
    /// INVPTR, on the thread's own line: the slot is inactive from this
    /// instant, so no batch retired from now on is placed in it and no
    /// retirer waits for this thread while it holds no guard. Never a plain
    /// store: an insert that landed between a load and the store would be
    /// lost, its batch counted for a node no traversal takes (a retire that
    /// finds INVPTR behind its exchange rolls its node back, or counts it
    /// when this exchange took it first: `try_retire`). The tag, stored
    /// before the exchange, is what a retire whose exchange lands after it
    /// reads: it takes its node out again, so the left slot holds no list
    /// once every such retire is done, whatever other retires exchanged in
    /// meanwhile (`try_retire`, `InsertCheck`). [LeaveTraverse] What the
    /// exchange captured (the nodes every batch placed here while the slot
    /// was active) is traversed onto the free-list cache, with no
    /// destructor running yet. [LeaveDrain] Then the cache is drained with
    /// the slot inactive and the pin count at 0, so a destructor that pins
    /// enters the slot for a section of its own and leaves it at its end,
    /// and one that retires or flushes runs as code outside any section
    /// does. The drain takes the whole cache out of its cell before it frees
    /// anything, so a nested drain finds it empty (`drain_free_list`).
    ///
    /// A late section (see `late`) ends here as an exit does: no exit runs
    /// for its thread any more to submit what it retired and release its ID.
    ///
    /// # Progress: wait-free
    ///
    /// One store, one exchange, one walk of the list it captured (fixed in
    /// length by the exchange, see `reclaim::traverse`), and the frees of the
    /// batches that walk released: the destructors run are this thread's own
    /// work, bounded by the values freed, never by other threads.
    #[inline(always)]
    fn leave(&self) {
        if self.late.get() {
            self.late.set(false);
            self.cleanup();
            return;
        }
        let first = self.leave_slot();
        if is_list(first) || !self.free_list.get().is_null() {
            self.leave_cold(first);
        }
    }

    /// [LeaveTraverse] and [LeaveDrain], out of the line of the common
    /// leave, whose exchange took nothing and whose cache is empty.
    ///
    /// A leave inside a reclamation (a destructor's own section, run by a
    /// drain, a retire or an exit) only adds to the cache and leaves the
    /// freeing to the reclamation that is running, whose loop takes what the
    /// destructors added (`drain_free_list`): a destructor that pins would
    /// otherwise free the next batches inside its own frame, and the next
    /// destructor's leave inside that one, one frame deeper for every batch
    /// other threads placed meanwhile.
    #[cold]
    fn leave_cold(&self, first: u64) {
        if is_list(first) {
            // SAFETY: `first` is the list head the leave's exchange captured.
            unsafe { self.traverse_onto_cache(first as *mut RetiredNode) };
        }
        if !self.in_reclaim.get() {
            self.drain_free_list();
        }
    }

    /// [LeaveMark] and [Leave]: the tagged store and the exchange that leave
    /// this thread's reservation slot, returning the list word it displaced
    /// (0, INVPTR, or a list the caller traverses). Shared by the outermost
    /// drop and the exit.
    #[inline(always)]
    fn leave_slot(&self) -> u64 {
        let first = self.slots().leave(0);
        // The escalated era was reset by the leave; mirror it.
        if self.cached_epoch.get() == EPOCH_UNCONDITIONAL {
            self.cached_epoch.set(0);
        }
        #[cfg(test)]
        crate::stall::at(crate::stall::Step::Left, self.tid());
        first
    }

    /// do_update: enter a slot, or republish its epoch.
    /// Takes the slot's list (activating the slot), traverses it, then
    /// publishes the current epoch. Returns the epoch published.
    ///
    /// # Progress: wait-free; bound proportional to retirement history
    ///
    /// The exchange is a single atomic instruction (on native platforms)
    /// and captures the list atomically, so traversal terminates in finitely
    /// many steps. The list length is bounded by the number of `try_retire`
    /// calls (one node each) since this slot's previous traversal. See
    /// `reclaim::traverse`.
    ///
    /// [Enter] The list word is exchanged to 0 in one step, never stored
    /// (`ThreadSlots::activate`): the slot left INVPTR at the last drop may
    /// hold a node a retire exchanged in since and has not rolled back yet;
    /// the exchange takes it, the retire's rollback then fails and counts
    /// the node, and the traversal here balances that count. A store would
    /// lose the node.
    ///
    /// # When a thread traverses its own slot list
    ///
    /// Protection is guard-wide: one reservation covers every pointer the
    /// thread loaded since its outermost `pin()`. So the thread traverses
    /// its own list only where no guard of it is live: at the outermost
    /// `pin()` here (the count was 0 and the new section has loaded
    /// nothing), at the outermost drop (`leave`, the count already 0) and
    /// at thread exit. Nested pins never transition, and `protect_load`
    /// only raises the published epoch. A traversal therefore never
    /// releases a pointer loaded earlier in a live section; a batch is
    /// freed only once its count reaches zero, that is once every slot that
    /// took one of its nodes (every reservation that covered it when it was
    /// retired) has been left or re-entered by its owner. On the own
    /// reservation the traversal only moves the released batches onto the
    /// cache (`traverse_onto_cache`): no destructor runs inside `pin()`;
    /// the drop of the guard drains them. A helper slot's traversal frees
    /// the cache when it is full (`traverse_into_cache`), as a help runs
    /// inside a retire or a flush, outside any section of its own.
    fn do_update(&self, curr_epoch: u64, index: usize, tid: usize) -> u64 {
        let global = self.global();
        let slots = global.thread_slots(tid);
        let mut curr_epoch = curr_epoch;

        // A slot with a non-null list word (INVPTR included) is taken: the
        // swap to 0 activates it in the same step that captures the list.
        let list_lo = slots.first[index].load_lo();
        if list_lo != 0 {
            let first = slots.activate(index);
            if is_list(first) {
                if index == 0 {
                    unsafe { self.traverse_onto_cache(first as *mut RetiredNode) };
                } else {
                    unsafe { self.traverse_into_cache(first as *mut RetiredNode) };
                }
            }

            curr_epoch = slot::epoch();
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
            self.mirror_transition(curr_epoch);
        }
        curr_epoch
    }

    /// Record a publication of this thread's own reservation slot (slot 0)
    /// at `epoch`, after its list was traversed.
    #[inline]
    fn mirror_transition(&self, epoch: u64) {
        self.cached_epoch.set(epoch);
        self.cached_birth_epoch.set(epoch);
    }

    /// Pin: enter a critical section (matches ASMR reserve_slot).
    ///
    /// Nested pin() calls are safe: only the outermost pin() does real work
    /// (the enter). Inner calls just increment the pin count and return a
    /// Guard. Guard::drop decrements the count, and the outermost drop
    /// leaves the slot.
    ///
    /// # Wait-free bound: O(T) where T = number of active threads
    ///
    /// Fast path: one exchange, and no publication when the epoch did not
    /// move. Otherwise at most 16 iterations (fixed), then the slow path:
    /// O(T), bounded by Crystalline-W helping. Every `advance_epoch()` call
    /// is preceded by `help_read()`, so at most T concurrent epoch advances
    /// can occur during the slow path loop. The helpee can also self-complete
    /// when the epoch stabilizes, independent of helper progress.
    #[inline(always)]
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

    /// [Enter] The outermost pin: the reference's `reserve_slot`. The slot
    /// was left at the last drop, so it is activated first (`activate`: the
    /// tag cleared, then one exchange of the list word to 0, what it held
    /// traversed). The slot keeps the era of its last publication, and the
    /// global epoch is read after the activation: when it is that era the
    /// pin publishes nothing (no store, no fence beyond the activation's
    /// own), as the reference's fast path, since a scan that reads the slot
    /// active reads an era that holds every batch this section needs. When it
    /// moved, the slot publishes the epoch it reads (`transition`).
    ///
    /// A SeqCst exchange on x86-64 is the full barrier the scan's fence
    /// pairs with: a scan that did not see the slot active completed before
    /// it, so the loads of this section see that batch's unlinks.
    /// Elsewhere `publish_fence` follows.
    #[inline(always)]
    fn enter(&self) {
        let first = self.slots().activate(0);
        publish_fence();
        let curr_epoch = slot::epoch();
        if is_list(first) || curr_epoch != self.cached_epoch.get() {
            self.enter_cold(first, curr_epoch);
        } else {
            self.cached_birth_epoch.set(curr_epoch);
        }
    }

    /// The traversal of what the activation took and the publication of a
    /// moved epoch, out of the line of the common pin.
    #[cold]
    fn enter_cold(&self, first: u64, curr_epoch: u64) {
        if is_list(first) {
            // SAFETY: `first` is the list head the activation's exchange
            // captured.
            unsafe { self.traverse_onto_cache(first as *mut RetiredNode) };
        }
        if curr_epoch != self.cached_epoch.get() {
            self.transition(curr_epoch);
        } else {
            self.cached_birth_epoch.set(curr_epoch);
        }
    }

    /// Publish the current epoch on this thread's activated slot:
    /// retry until the epoch is stable (at most 16 times), then the helping
    /// slow path.
    fn transition(&self, mut curr_epoch: u64) {
        let tid = self.tid();
        let index = 0; // kovan uses only slot index 0
        let mut attempts = 16usize;
        // At most 16 attempts, each one do_update: one publication (and the
        // walk of a list a retire put in meanwhile).
        loop {
            let prev_epoch = self.do_update(curr_epoch, index, tid);
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::TransitionAttempt, tid);
            attempts -= 1;
            if attempts == 0 {
                break;
            }
            curr_epoch = slot::epoch();
            if curr_epoch == prev_epoch {
                return;
            }
        }
        // Slow path: set up helping state and wait for stable epoch
        self.slow_path(index, tid);
    }

    /// Slow path for pin when epoch keeps changing.
    /// Sets up helping state so other threads can assist.
    ///
    /// No destructor runs before the slot is republished at the end: every
    /// traversal here only moves zero-count batches onto the free-list
    /// cache (`traverse_onto_cache`), and the drain after the
    /// republication frees them. While a helper holds the slot in its
    /// hand-over (the epoch or list seqno odd), new batches skip the slot,
    /// so a load made by a destructor run then would go unprotected; and
    /// such a load could raise the slot's epoch under the transition.
    ///
    /// # Wait-free bound: O(T) where T = number of active threads
    ///
    /// The main loop exits when either:
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
        // drain_free_list -> free_batch_list in the slow path.
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
        #[cfg(test)]
        crate::stall::at(crate::stall::Step::SlowPending, tid);

        // The list the loop took after the request was answered (its list
        // seqno moved on): the helper that answered it detached the slot's
        // list before that, so this is what is left to traverse.
        let mut produced: Option<u64> = None;

        // A pass is followed by another only when the global epoch moved
        // during it and no helper completed the request. Every epoch
        // advance runs help_read first, and a help_read that finds this
        // request open completes it before its advance: an epoch change the
        // request survives comes from a thread whose help_read passed this
        // slot before the request was published, at most one per thread
        // (its next advance helps first). Then either the epoch holds for a
        // pass (self-completion) or a helped advance ends it: T + 2 passes
        // at most.
        loop {
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::SlowPass, tid);
            let mut curr_epoch = slot::epoch();
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
                    self.mirror_transition(prev_epoch);
                    // Order the epoch publication before the critical
                    // section's pointer loads (Dekker pairing, see
                    // protect_load).
                    fence(Ordering::SeqCst);
                    // The slot is live again: free what the loop's
                    // traversals moved to the cache.
                    self.drain_free_list();
                    self.in_reclaim.set(was_reclaiming);
                    return;
                }
            }

            // Dereference previous nodes and update the era: after the
            // traversal the era to publish is read again, as a transition
            // publishes the era it reads after its traversal.
            let list_lo = slots.first[index].load_lo();
            if list_lo != 0 && list_lo != INVPTR as u64 {
                let exchanged = slots.first[index].exchange_lo(0, Ordering::AcqRel);
                // Check if result was already produced (seqno changed)
                if slots.first[index].load_hi() != seqno {
                    produced = Some(exchanged);
                    break;
                }
                if exchanged != INVPTR as u64 {
                    unsafe { self.traverse_onto_cache(exchanged as *mut RetiredNode) };
                }
                curr_epoch = slot::epoch();
            }

            // Fails only when the result was produced: a helper moved the
            // seqno on.
            let _ = slots.epoch[index].compare_exchange(prev_epoch, seqno, curr_epoch, seqno);
            prev_epoch = curr_epoch;

            let result_ptr = slots.state[index].result.load_lo();
            if result_ptr != INVPTR as u64 {
                break; // Helper completed
            }
        }

        // The request was answered. Unless the loop found the list already
        // detached, detach it here as the answering helper does (whoever
        // runs it first takes the list).
        let mut list = match produced {
            Some(list) => list,
            None => self.detach_nodes(slots, index, seqno),
        };

        // Publish the result's era with the seqnos past the cycle [Produced]:
        // the era value first, then its seqno, then the list's.
        let result_epoch = slots.state[index].result.load_hi();
        slots.epoch[index].store_lo(result_epoch, Ordering::Release);
        slots.epoch[index].store_hi(seqno + 2, Ordering::Release);
        // Mirror the published epoch and order it before this critical
        // section's pointer loads (Dekker pairing, see protect_load).
        self.mirror_transition(result_epoch);
        fence(Ordering::SeqCst);
        slots.first[index].store_hi(seqno + 2, Ordering::Release);

        // Check if the result pointer is already retired (need to protect
        // it): its batch's refs-node goes into the list as its terminal
        // entry. (A pin's request carries no pointer, so this is inert.)
        let result_ptr = slots.state[index].result.load_lo() & 0xFFFFFFFFFFFFFFFC;
        if result_ptr != 0 {
            let ptr_node = result_ptr as *mut RetiredNode;
            let batch_link = unsafe { (*ptr_node).batch_link.load(Ordering::Acquire) };
            if !batch_link.is_null() {
                let refs = unsafe { crate::reclaim::get_refs_node(ptr_node) };
                unsafe {
                    (*refs).refs_or_next.fetch_add(1, Ordering::AcqRel);
                }
                if list != 0 && list != INVPTR as u64 {
                    unsafe { self.traverse_onto_cache(list as *mut RetiredNode) };
                }
                list = slots.first[index].exchange_lo(rnode_mark(refs) as u64, Ordering::AcqRel);
            }
        }

        global.dec_slow();

        // Traverse the list detached or swapped out above.
        if list != 0 && list != INVPTR as u64 {
            unsafe { self.traverse_onto_cache(list as *mut RetiredNode) };
        }

        self.drain_free_list();
        self.in_reclaim.set(was_reclaiming);
    }

    /// Detach the list of slot `index` of `slots`, whose slow-path cycle
    /// `tag` ends: the pending thread and the helper that answered its
    /// request both call this, and whoever runs it first takes the list.
    /// Returns the list taken, or INVPTR when the other one took it.
    ///
    /// Step `DetachEra` moves the era's seqno from `tag` to `tag + 1` with a
    /// compare-exchange of that word alone, which the era's value (whatever
    /// the pending thread last published) cannot fail; an odd era seqno
    /// makes every scan that reads it skip the slot. Step `DetachList` then
    /// empties the list and moves the list's seqno from `tag` to `tag + 1`
    /// in one compare-exchange of the slot, while that seqno is still `tag`.
    ///
    /// The loop is bounded by the retires already past their scan when the
    /// era seqno went odd, never by how many other threads retire after: a
    /// pass fails only when the slot changed between its read and its
    /// compare-exchange, by the other detach (which moves the list seqno, and
    /// the next pass returns) or by an insert (or its rollback) of a
    /// try_retire that read the era seqno still even. The era seqno stays
    /// odd until the list seqno has moved on, so no later scan inserts; a
    /// try_retire inserts once per slot, and a thread has one in flight per
    /// nesting level of its destructors.
    fn detach_nodes(&self, slots: &slot::ThreadSlots, index: usize, tag: u64) -> u64 {
        let _ = slots.epoch[index].compare_exchange_hi(tag, tag + 1);
        #[cfg(test)]
        crate::stall::at(crate::stall::Step::EraClosed, self.tid());
        loop {
            let (lo, hi) = slots.first[index].load();
            if hi != tag {
                return INVPTR as u64;
            }
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::DetachList, self.tid());
            if slots.first[index]
                .compare_exchange(lo, hi, 0, tag + 1)
                .is_ok()
            {
                return lo;
            }
        }
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

        // One pass over the T x HR_NUM request words, one help_thread per
        // open request.
        for i in 0..max_threads {
            let slots = global.thread_slots(i);
            for (j, state) in slots.state.iter().take(hr_num).enumerate() {
                if state.result.load_lo() == INVPTR as u64 {
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
    /// 2. The request it helps changed: another helper set the result, or
    ///    the pending thread went on to a later request (its era's seqno
    ///    moved on, which a torn read of the request cannot hide).
    ///
    /// For the epoch to advance during this loop, some thread must call
    /// `advance_epoch()`, which is preceded by `help_read()`. After at most
    /// T concurrent epoch advances, no more threads are in the
    /// advance phase and the epoch stabilizes. The bound is independent of
    /// the helpee's progress: the helpee is passive, and the loop never
    /// follows it into a later request, whose advances would count anew.
    ///
    /// The hand-over loops are bounded too: the list's (`detach_nodes`) by
    /// the retires in flight when the slot closed to new batches, the
    /// epoch's by the pending thread's republication (two passes at most).
    #[cold]
    fn help_thread(&self, helpee_tid: usize, index: usize, mytid: usize) {
        let global = self.global();
        let hr_num = global.hr_num();
        let helpee = global.thread_slots(helpee_tid);
        let mine = global.thread_slots(mytid);

        // The request to help, (INVPTR, its cycle's seqno) while open.
        let (result_lo, result_hi) = helpee.state[index].result.load();
        if result_lo != INVPTR as u64 {
            return;
        }

        let birth_epoch = helpee.state[index].epoch.load(Ordering::Acquire);
        let parent = helpee.state[index].parent.load(Ordering::Acquire);

        // Advertise the parent for a hand-over, protected by this thread's
        // parent slot meanwhile. (A pin's request carries no parent.)
        if parent != 0 {
            let first = mine.activate(hr_num);
            if is_list(first) {
                unsafe { self.traverse_into_cache(first as *mut RetiredNode) };
            }
            mine.epoch[hr_num].store_lo(birth_epoch, Ordering::SeqCst);
            mine.state[hr_num].parent.store(parent, Ordering::SeqCst);
        }

        let _obj = helpee.state[index].pointer.load(Ordering::Acquire);
        let seqno = helpee.epoch[index].load_hi();

        if result_hi == seqno {
            let mut curr_epoch = slot::epoch();

            // At most T + 2 passes, each one do_update of this thread's
            // helper slot: another pass only after the epoch moved during
            // this one while the same request is still open (the argument of
            // the pending thread's own loop). A request answered, or followed
            // by the pending thread's next one, ends the loop.
            loop {
                let prev_epoch = self.do_update(curr_epoch, hr_num + 1, mytid);
                #[cfg(test)]
                crate::stall::at(crate::stall::Step::HelpPass, mytid);
                // In reserve_slot mode (pointer=0), ptr is always null
                curr_epoch = slot::epoch();

                if curr_epoch == prev_epoch {
                    // Try to set result
                    if helpee.state[index]
                        .result
                        .compare_exchange(result_lo, result_hi, 0, curr_epoch)
                        .is_ok()
                    {
                        let list = self.detach_nodes(helpee, index, seqno);
                        if list != INVPTR as u64 && list != 0 {
                            unsafe { self.traverse_into_cache(list as *mut RetiredNode) };
                        }

                        let seqno = seqno + 1;

                        // Set the new era [HandOverEpoch]. A strong
                        // compare-exchange fails only when the word changed
                        // while its seqno is odd: the pending thread
                        // republishing (its era, then seqno + 1) or another
                        // helper succeeding, both of which move the seqno on
                        // and end the loop. At most two passes.
                        let (mut old_lo, mut old_hi) = helpee.epoch[index].load();
                        while old_hi == seqno {
                            match helpee.epoch[index].compare_exchange(
                                old_lo,
                                old_hi,
                                curr_epoch,
                                seqno + 1,
                            ) {
                                Ok(_) => break,
                                Err((lo, hi)) => {
                                    old_lo = lo;
                                    old_hi = hi;
                                }
                            }
                        }

                        // A simple list seqno transition [HandOverList]: a
                        // pin's request carries no pointer to hand over.
                        let _ = helpee.first[index].compare_exchange_hi(seqno, seqno + 1);
                    }
                    break;
                }

                // Another pass only while the request read first is still open:
                // its two words unchanged, and the era's seqno still the one the
                // request was made with. The words are read one at a time, the
                // low one first, as `WordPair::load` reads them, so a read can
                // pair a later request's INVPTR with a high word equal to
                // `seqno` again: the 0 a self-completion leaves (a thread's
                // first request has seqno 0) or the epoch a helper's answer
                // carries. The era's seqno is one word, and it never returns to
                // `seqno` once the cycle ends (a cycle ends with it at
                // seqno + 2), so the loop never follows the pending thread into
                // a later cycle, whose epoch advances would count anew.
                let lo = helpee.state[index].result.load_lo();
                #[cfg(test)]
                crate::stall::at(crate::stall::Step::HelpRecheck, mytid);
                let hi = helpee.state[index].result.load_hi();
                if (lo, hi) != (result_lo, result_hi) || helpee.epoch[index].load_hi() != seqno {
                    break;
                }
            }

            // Leave the helper slot [HelperLeave].
            let first = mine.leave(hr_num + 1);
            if is_list(first) {
                unsafe { self.traverse_into_cache(first as *mut RetiredNode) };
            }
        }

        if parent != 0 {
            // If the parent was handed over, drop the reference it came with.
            let old_parent = mine.state[hr_num].parent.swap(0, Ordering::SeqCst);
            if old_parent != parent {
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
            // Leave the parent slot.
            let first = mine.leave(hr_num);
            if is_list(first) {
                unsafe { self.traverse_into_cache(first as *mut RetiredNode) };
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
    /// The first call, every EPOCH_FREQ (128)th after, and every call that
    /// placed a batch: help_read (O(T ^ 2) worst case, O(1) when no stalled
    /// threads) + advance_epoch (fetch_add).
    ///
    /// # Safety
    ///
    /// - `node_ptr` must point to a valid, heap-allocated `RetiredNode` at offset 0.
    /// - `destructor` must already be set on the node.
    /// - `birth_epoch` must already be set correctly.
    /// - The node must not be enqueued more than once.
    #[inline(always)]
    unsafe fn enqueue_node(&self, node_ptr: *mut RetiredNode) {
        // A retire outside every reclamation frees what its submission and
        // its helping released itself, once they are done; a nested one
        // (a destructor's) leaves it to the reclamation it runs in.
        let outermost = !self.in_reclaim.get();
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
            // A thread's exit (`cleanup`) runs from the sentinel, which a
            // thread registers when it claims an ID. A thread with no ID
            // that starts a batch registers it here, so its exit runs and
            // submits the batch.
            if self.tid.get().is_none() {
                let _ = exit_pending();
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

        // The common retire only links its node and counts it, inline; the
        // steps below run, in this order, only when one of them is due.
        let alloc_count = self.alloc_counter.get();
        if count.is_multiple_of(RETIRE_FREQ)
            || alloc_count.is_multiple_of(EPOCH_FREQ)
            || (outermost && !self.free_list.get().is_null())
        {
            self.enqueue_due(count, outermost);
        } else {
            self.alloc_counter.set(alloc_count + 1);
        }
    }

    /// The submission, the era advance and the drain of a retire
    /// (`enqueue_node`), out of its inline line: run when one is due.
    #[cold]
    #[inline(never)]
    fn enqueue_due(&self, count: usize, outermost: bool) {
        let mut placed = false;
        if count.is_multiple_of(RETIRE_FREQ) {
            // Set in_reclaim: a destructor a drain runs from here (a batch
            // the submission freed) may drop an Atom that calls flush().
            let was_reclaiming = self.in_reclaim.get();
            self.in_reclaim.set(true);

            // Orphan adoption is left to flush(): its pass over the thread
            // IDs is O(T) and the chain it takes is as long as what exited
            // threads parked, work a retire does not take on. Orphans
            // (created only at thread exit) are adopted by flush().

            // Detach the batch BEFORE try_retire (see `take_batch`).
            if let Some((first, last)) = self.take_batch() {
                placed = self.try_retire(first, last, false);
                if !placed {
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

        // Advance epoch periodically, after the batch step above, not
        // before it: help_read can free cached batches whose destructors
        // retire on this thread, and nested retires may submit and empty
        // the batch. The batch step reads `count` before anything re-enters
        // and the batch cells right before it detaches them; run after the
        // help, it would size the batch from a stale `count` and could
        // finalize an emptied one.
        //
        // The count is checked before it is raised, as Crystalline's
        // `alloc_cnt++ % FREQ == 0`: a thread's first retire advances, then
        // every `EPOCH_FREQ`th, so threads that each retire fewer than
        // `EPOCH_FREQ` values and exit still move the epoch, and the values
        // born after a long section's reservation skip its slot. A retire
        // that placed a batch advances too, so the sections that took a
        // node of it see a later epoch at their next load.
        let alloc_count = self.alloc_counter.get();
        self.alloc_counter.set(alloc_count + 1);
        if placed || alloc_count.is_multiple_of(EPOCH_FREQ) {
            self.increment_era(self.tid());
        }

        // The batches the submission released wait on the cache: no
        // destructor runs inside `try_retire`, so one that retires never
        // nests in it.
        if outermost && !self.free_list.get().is_null() {
            self.drain_free_list();
        }
    }

    /// Advance the global epoch [IncrementEra]: help the pending slow-path
    /// requests first, as every advance does (the slow path's bound rests
    /// on it), then advance, and stamp this thread's next allocations with
    /// the epoch it advanced to, as fresh a birth as the epoch it just made.
    fn increment_era(&self, tid: usize) {
        // Set in_reclaim: help_read -> help_thread -> do_update ->
        // traverse_into_cache can free a full cache, whose destructors
        // drop Atoms triggering flush(). The flag prevents re-entrant
        // flush from reading stale free_list Cell state.
        let was_reclaiming = self.in_reclaim.replace(true);
        self.help_read(tid);
        self.in_reclaim.set(was_reclaiming);
        self.cached_birth_epoch.set(slot::advance_epoch());
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
    /// the caller runs afterwards (the drain of what the submission freed)
    /// may re-enter retire()/flush() and must see empty batch cells, never
    /// the batch being submitted (re-submitting it would double-insert and
    /// double-free the whole batch).
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
    /// No destructor runs inside: a batch the submission frees (its count
    /// reached zero, or a displaced list a take-back walked) goes onto the
    /// thread's cache, and the caller drains the cache after it
    /// (`enqueue_node`, `place_all`). A destructor that retires therefore
    /// never nests in a submission, however many batches that frees.
    ///
    /// # Wait-free bound: O(T + batch_size) where T = number of active threads
    ///
    /// Two phases, both bounded:
    /// - **Scan phase**: O(T * SLOTS_PER_THREAD) - iterates all active thread
    ///   slots once, assigning batch nodes to active slots with epoch ≥ min_epoch.
    ///   No loops within - each slot is visited exactly once (one filler
    ///   allocated per eligible slot beyond the batch's nodes, with `pad`).
    /// - **Insert phase**: O(batch_size) - iterates batch nodes, exchanging each
    ///   into its assigned slot. The exchange is a single atomic instruction (on
    ///   native platforms). Contention handling (INVPTR rollback, list tainting)
    ///   is O(1) per node.
    fn try_retire(&self, batch_first: *mut RetiredNode, refs: *mut RetiredNode, pad: bool) -> bool {
        #[cfg(test)]
        let _scope = TryRetireScope::enter(self);
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
        // One pass over the T x SLOTS_PER_THREAD slots, each visited once.
        for i in 0..max_threads {
            let slots = global.thread_slots(i);
            // Regular reservation slots (0..hr_num)
            for j in 0..hr_num {
                let first_lo = slots.first[j].load_lo();
                if first_lo == INVPTR as u64 {
                    continue;
                }
                // Check seqno odd (in slow-path transition)
                if slots.first[j].load_hi() & 1 != 0 {
                    continue;
                }
                let epoch = slots.epoch[j].load_lo();
                if !slot::era_covers(epoch, min_epoch) {
                    continue;
                }
                // Check epoch seqno odd
                if slots.epoch[j].load_hi() & 1 != 0 {
                    continue;
                }

                if last == refs {
                    if !pad {
                        return false; // Not enough batch nodes for all eligible slots
                    }
                    // [Pad] A filler takes this slot, linked ahead of the
                    // batch's first node (the insert walks from `curr`).
                    curr = self.filler(curr, refs, min_epoch);
                    unsafe { (*curr).set_slot_info(i, j) };
                    continue;
                }
                unsafe {
                    (*last).set_slot_info(i, j);
                }
                last = unsafe { (*last).batch_next() };
            }
            // Helper slots (hr_num..hr_num+2)
            for j in hr_num..hr_num + 2 {
                let first_lo = slots.first[j].load_lo();
                if first_lo == INVPTR as u64 {
                    continue;
                }
                let epoch = slots.epoch[j].load_lo();
                if !slot::era_covers(epoch, min_epoch) {
                    continue;
                }

                if last == refs {
                    if !pad {
                        return false; // Not enough batch nodes for all eligible slots
                    }
                    curr = self.filler(curr, refs, min_epoch);
                    unsafe { (*curr).set_slot_info(i, j) };
                    continue;
                }
                unsafe {
                    (*last).set_slot_info(i, j);
                }
                last = unsafe { (*last).batch_next() };
            }
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
            if !slot::era_covers(slot_epoch.load_lo(), min_epoch) {
                curr = unsafe { (*curr).batch_next() };
                continue;
            }

            // Exchange our node in as the new list head
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::InsertReady, self.tid());
            let prev = slot_first_ref.exchange_lo(curr as u64, Ordering::AcqRel);
            #[cfg(test)]
            crate::stall::at(crate::stall::Step::InsertExchanged, self.tid());

            if prev == INVPTR as u64 {
                // Slot was inactive (its thread left it) - undo by
                // restoring the INVPTR sentinel, as in the reference
                // try_retire rollback. Restoring 0 here would resurrect a
                // dead slot as active-empty and break the `INVPTR ==
                // inactive` invariant.
                // [InsertRollback] A compare-exchange of the list word
                // alone: the node still heads the list or it does not.
                if slot_first_ref.compare_exchange_lo(curr as u64, INVPTR as u64) {
                    // Undo succeeded - node removed from slot.
                    curr = unsafe { (*curr).batch_next() };
                    continue;
                }
                // Undo failed - node was captured by a concurrent exchange
                // (the owner's, or the undo of a retire that exchanged in
                // above, `InsertUndo`), which traverses it. Count as
                // inserted to balance refs, matching Crystalline's cnt++ on
                // CAS failure (Figure 9, line 66).
                adjs = adjs.wrapping_add(1);
                curr = unsafe { (*curr).batch_next() };
                continue;
            }

            // The list the node displaced, to walk here if a traversal
            // passed the node before it was linked, or if the undo below
            // takes the node and the list out together: once at most.
            let mut behind = prev as *mut RetiredNode;
            if prev != 0 {
                // Link prev as next of curr
                if unsafe {
                    (*curr).next.compare_exchange(
                        core::ptr::null_mut(),
                        behind,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                }
                .is_err()
                {
                    // Someone already set next (concurrent traverse): the
                    // list goes onto the cache, freed after the submission.
                    unsafe { self.traverse_onto_cache(behind) };
                    behind = core::ptr::null_mut();
                }
            }

            // [InsertCheck] The era word read after the exchange: tagged,
            // the owner left since the scan (its tag store precedes the
            // leave's exchange, which precedes ours or took our node), and
            // nothing traverses the slot until the owner's next pin or exit.
            // [InsertUndo] So the node is taken out again, with whatever it
            // displaced (a retire's node that found the slot left, or the
            // list the leave is about to find gone), which is walked here.
            // A failed undo means an exchange took the node (the owner's,
            // or another retire's undo), and that traverses it: counted.
            if slot::era_left(slot_epoch.load_lo())
                && slot_first_ref.compare_exchange_lo(curr as u64, INVPTR as u64)
            {
                if !behind.is_null() {
                    unsafe { self.traverse_onto_cache(behind) };
                }
                curr = unsafe { (*curr).batch_next() };
                continue;
            }

            adjs = adjs.wrapping_add(1);
            curr = unsafe { (*curr).batch_next() };
        }

        // Adjust reference count
        let old = unsafe { (*refs).refs_or_next.fetch_add(adjs, Ordering::AcqRel) };
        if old == adjs.wrapping_neg() {
            // refs reached 0: the batch is free, and goes onto the cache as
            // a traversal's last decrement pushes one (`reclaim::traverse`).
            unsafe {
                (*refs).next.store(self.free_list.get(), Ordering::Relaxed);
            }
            self.free_list.set(refs);
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

    /// Traverse a captured slot list into this thread's free-list cache
    /// (the one traversal every helper path uses).
    ///
    /// The cache holds batches whose count reached zero; once it holds
    /// `MAX_CACHE` traversals' worth it is freed before the next traversal.
    /// Destructors run in that free and may re-enter this thread's
    /// reclamation (pin, retire and through it help another thread,
    /// traverse), so the cache leaves its cell before any of them runs, and
    /// the traversal, which runs no destructor, pushes onto what the cell
    /// holds when it runs. A re-entrant traversal's entries are kept, never
    /// overwritten by a copy taken before it ran, and no batch is freed
    /// twice from a copy of the cell.
    ///
    /// # Safety
    ///
    /// `first` must be a slot list head captured by exchange (not null, not
    /// INVPTR), as `reclaim::traverse` requires.
    unsafe fn traverse_into_cache(&self, first: *mut RetiredNode) {
        if self.list_count.get() >= crate::reclaim::MAX_CACHE {
            let full = self.free_list.replace(core::ptr::null_mut());
            self.list_count.set(0);
            let was_reclaiming = self.in_reclaim.replace(true);
            unsafe { crate::reclaim::free_batch_list(full) };
            self.in_reclaim.set(was_reclaiming);
        }
        unsafe { self.traverse_onto_cache(first) };
    }

    /// Traverse a captured slot list onto this thread's free-list cache and
    /// free nothing: no destructor runs. Step `Traverse`, the one every
    /// traversal takes: `reclaim::traverse` over the captured list, whose
    /// length was fixed by the exchange that captured it.
    ///
    /// # Safety
    ///
    /// As for `traverse_into_cache`.
    unsafe fn traverse_onto_cache(&self, first: *mut RetiredNode) {
        #[cfg(test)]
        crate::stall::at(crate::stall::Step::Traverse, self.tid());
        let mut free_list = self.free_list.get();
        unsafe { crate::reclaim::traverse(&mut free_list, first) };
        self.free_list.set(free_list);
        self.list_count.set(self.list_count.get() + 1);
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
        // pushed onto the cache through kovan calls of their own (a help, or
        // a transition where their pin is outermost): bounded by those
        // destructors, never by other threads.
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
        // A thread that has never claimed an ID claims one only when its
        // batch holds a value (not reached before the exit any more: a
        // thread's first retire claims an ID for its epoch advance; kept as
        // a guard), since the ID stays taken until the thread exits, and only
        // while that exit is still to run, as the exit is what releases it.
        if self.tid.get().is_none() && (self.batch_first.get().is_null() || !exit_pending()) {
            return;
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
        let tid = self.tid();
        self.place_all(tid);
        self.in_reclaim.set(false);
    }

    /// [FlushPlace] to [FlushDrain], repeated while the batch holds a
    /// value: the one placement loop of `flush` and of the exit.
    fn place_all(&self, tid: usize) {
        // Another pass only for what the destructors of this pass retired:
        // bounded by those destructors, never by other threads.
        loop {
            if let Some((first, last)) = self.take_batch() {
                let placed = self.try_retire(first, last, true);
                debug_assert!(placed, "a padded batch was not placed");
                // Help pending slow-path threads before advancing the epoch
                // (the wait-free pin() bound requires every advance to be
                // helped first).
                self.increment_era(tid);
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
        self.exited.set(true);
        // A thread with a batch and no ID (a retire after its exit, where the
        // handle outlives the exit and its retire count is not at an advance)
        // claims one, as a first pin would, and exits as any thread does.
        // Without an ID its batch would be dropped with the handle, unfreed.
        if self.tid.get().is_none() && !self.batch_first.get().is_null() {
            self.claim_tid();
        }
        if let Some(tid) = self.tid.get() {
            // Prevent re-entrant flush() from destructors during cleanup.
            self.in_reclaim.set(true);
            // A guard still live here was never dropped (`mem::forget`): no
            // code can use it any more, so its section ends here, and the
            // destructors below run outside any section, as `leave`'s do.
            self.pin_count.set(0);

            // [ExitLeave] The slot is left as the outermost drop leaves it:
            // one exchange, what it captured traversed onto the cache. From
            // here no batch is placed in this thread's slot.
            let first = self.leave_slot();
            if is_list(first) {
                unsafe { self.traverse_onto_cache(first as *mut RetiredNode) };
            }
            // Every helper slot was left by the help that used it; the
            // parent slot is never activated (see `help_thread`).
            debug_assert!(
                (1..crate::slot::SLOTS_PER_THREAD)
                    .all(|j| self.global().thread_slots(tid).first[j].load_lo() == INVPTR as u64),
                "kovan: a helper slot active at exit"
            );

            // [ExitPlace] What the thread holds is placed and freed as a
            // flush does, until it holds nothing: the batches the traversal
            // released, the partial batch, and whatever the destructors of
            // those retire. The slot is inactive, so no other thread's
            // retire lands in it meanwhile, and the exit is bounded by the
            // thread's own values.
            self.place_all(tid);

            // Only now may another thread take this ID over, after every
            // destructor this exit ran: a destructor may pin and load, and
            // its section enters and leaves this ID's slot. Released
            // earlier, the ID could already belong to a new thread, whose
            // reservation that section would overwrite.
            self.global().release_tid(tid);

            self.in_reclaim.set(false);

            // Mark TID as unused so cleanup is idempotent. The slot is gone:
            // a later pin on this thread (a TLS destructor running after
            // this one) must allocate a tid and enter before any load is
            // protected, so no cached epoch may claim a publication.
            self.tid.set(None);
            self.slots.set(core::ptr::null());
            self.cached_epoch.set(0);
        }
    }
}

#[cfg(test)]
mod tests;

/// This thread's handle: a thread-local with no destructor (see `Handle`).
/// On nightly a `#[thread_local]` static; elsewhere a `thread_local!` with
/// a const initializer, which for a type with no destructor is one address
/// too, with no state to check.
#[cfg(feature = "nightly")]
#[thread_local]
static HANDLE: Handle = Handle::new();

#[cfg(not(feature = "nightly"))]
thread_local! {
    static HANDLE: Handle = const { Handle::new() };
}

/// Counts the `try_retire` calls live on a thread (tests only).
#[cfg(test)]
struct TryRetireScope<'a>(&'a Handle);

#[cfg(test)]
impl<'a> TryRetireScope<'a> {
    fn enter(handle: &'a Handle) -> Self {
        handle.in_try_retire.set(handle.in_try_retire.get() + 1);
        Self(handle)
    }
}

#[cfg(test)]
impl Drop for TryRetireScope<'_> {
    fn drop(&mut self) {
        self.0.in_try_retire.set(self.0.in_try_retire.get() - 1);
    }
}

/// Run `f` on this thread's handle.
#[inline(always)]
fn with_handle<R>(f: impl FnOnce(&Handle) -> R) -> R {
    #[cfg(feature = "nightly")]
    {
        f(&HANDLE)
    }
    #[cfg(not(feature = "nightly"))]
    {
        HANDLE.with(f)
    }
}

/// The thread-local whose destructor runs the thread's exit
/// (`Handle::cleanup`): `HANDLE` has none, so that it answers during every
/// thread-local destructor, the exit's own included. Registered when the
/// thread claims an ID or starts a batch (`exit_pending`).
#[cfg(not(feature = "shuttle"))]
struct HandleCleanupSentinel;

#[cfg(not(feature = "shuttle"))]
impl Drop for HandleCleanupSentinel {
    fn drop(&mut self) {
        with_handle(Handle::cleanup);
    }
}

#[cfg(not(feature = "shuttle"))]
thread_local! {
    static HANDLE_CLEANUP_SENTINEL: HandleCleanupSentinel = const { HandleCleanupSentinel };
}

/// Whether this thread's exit is still to run: the sentinel whose
/// destructor runs it is still registered (registered here if it never
/// was). False once it has run, during the thread-local destructors that
/// follow it.
#[cfg(not(feature = "shuttle"))]
#[inline]
fn exit_pending() -> bool {
    HANDLE_CLEANUP_SENTINEL.try_with(|_| {}).is_ok()
}

// Under shuttle a task's thread-locals cannot reach one another while they
// are destroyed, so the handle runs its exit from a destructor of its own
// and no task-local destructor runs after it.
#[cfg(feature = "shuttle")]
impl Drop for Handle {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(feature = "shuttle")]
#[inline]
fn exit_pending() -> bool {
    true
}

/// Whether a slot's list word holds a list: neither 0 (active, empty) nor
/// INVPTR (left). The word holds a pointer, so it is read at pointer width:
/// there the two sentinels are the two values below 2 once incremented.
#[inline(always)]
fn is_list(word: u64) -> bool {
    (word as usize).wrapping_add(1) > 1
}

/// The barrier after a publication of this thread's slot (an activation)
/// and before the loads it must precede. On x86-64 the SeqCst
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

/// Current epoch for stamping a freshly allocated node's `birth_epoch`.
///
/// Called by `RetiredNode::new`. Returns the thread-cached global epoch
/// (refreshed at every `pin()`), avoiding a contended atomic read of the
/// global epoch counter on every allocation. The value is always ≤ the true
/// global epoch, which is the safety requirement (see `Handle::current_birth_epoch`).
#[inline]
pub(crate) fn current_birth_epoch() -> u64 {
    with_handle(Handle::current_birth_epoch)
}

/// Protect: era tracking for `Atomic::load()`.
///
/// Called internally by `Atomic::load()`. Must NOT be called without a live Guard.
#[inline(always)]
pub(crate) fn protect_load(data: &AtomicUsize, order: Ordering) -> usize {
    with_handle(|handle| handle.protect_load(data, order))
}

/// Enter a critical section.
///
/// Returns a `Guard` that represents the active critical section.
/// While the guard exists, any `Shared<'g, T>` pointers loaded are
/// guaranteed to remain valid.
#[inline(always)]
pub fn pin() -> Guard {
    with_handle(Handle::pin)
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

/// The one retire path both public entry points share.
#[inline]
pub(crate) unsafe fn retire_with(node_ptr: *mut RetiredNode, destructor: DestructorFn) {
    with_handle(|handle| {
        // The handle outlives the sentinel whose destructor runs the exit,
        // and takes the retires of the destructors that exit runs and of a
        // late section (whose end submits them). Once the exit is over, a
        // retire (from a thread-local destructor that runs after it) would
        // fill a batch no exit submits any more: a handle of its own.
        if handle.exit_over() {
            // SAFETY: Caller upholds the safety contract.
            unsafe { retire_after_exit(node_ptr, destructor) }
        } else {
            // SAFETY: Caller upholds the safety contract.
            unsafe { handle.retire_with(node_ptr, destructor) }
        }
    })
}

/// A retire after this thread's exit ran: a handle of its own takes the
/// node, and its exit, run at once, submits it or parks it for a live
/// thread to adopt, as any thread's exit does with the batch it leaves. The
/// handle claims a thread ID for that exit and releases it, so nothing of
/// the thread is held afterwards.
///
/// # Safety
///
/// As [`retire_with`].
#[cold]
unsafe fn retire_after_exit(node_ptr: *mut RetiredNode, destructor: DestructorFn) {
    let late = Handle::new();
    // SAFETY: forwarded from the caller.
    unsafe { late.retire_with(node_ptr, destructor) };
    late.cleanup();
}

/// Flush all retired nodes on the calling thread.
///
/// Submits the thread's partial batch (padded with fillers, so it is always
/// placed, whatever other threads' reservations hold), advances the global
/// epoch when it placed one, and frees what that released, repeating while
/// the destructors it ran retired more: it returns holding no retired node.
/// A thread that retired nothing since its batch was last submitted returns
/// at once and advances nothing (see [`flush_pending`]).
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
    with_handle(Handle::flush)
}

/// Submits this thread's batch of retired nodes to reclamation now, as
/// [`flush`] does, when it holds one; with nothing retired on this thread
/// since its batch was last submitted it does nothing. A structure's drop
/// calls it: what its thread retired goes to reclamation without waiting for
/// the batch to fill, and a drop that retired nothing costs no epoch advance
/// ([`flush`] makes one, which sends every other thread's next pin down its
/// slow path).
pub fn flush_pending() {
    with_handle(Handle::flush_pending)
}
