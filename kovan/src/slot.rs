//! Slot structures and global state.
//!
//! Each thread gets its own `ThreadSlots` containing per-reservation DCAS pairs
//! for the retirement list and epoch, plus helping state. The global state holds
//! the epoch counter, slow-path counter, and thread ID allocator.

use crate::retired::INVPTR;
use crate::ttas::TTas;
use alloc::boxed::Box;
use core::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

// ---------------------------------------------------------------------------
// WordPair: split-field (native) vs single-AtomicU128 (fallback)
// ---------------------------------------------------------------------------

/// Native implementation for platforms with hardware 128-bit atomics.
/// Sub-word ops (store_lo, store_hi, exchange_lo) are single atomic
/// instructions instead of CAS loops like fallback path.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "s390x"))]
mod native {
    use core::sync::atomic::{AtomicU64, Ordering};
    use portable_atomic::AtomicU128;

    // Field order must match u128 bit-layout so that as_u128() reinterpret works:
    // - Little-endian: offset 0 = low 64 bits  -> lo first
    // - Big-endian:    offset 0 = high 64 bits -> hi first
    #[cfg(target_endian = "little")]
    #[repr(C, align(16))]
    pub(crate) struct WordPair {
        lo: AtomicU64,
        hi: AtomicU64,
    }

    #[cfg(target_endian = "big")]
    #[repr(C, align(16))]
    pub(crate) struct WordPair {
        hi: AtomicU64,
        lo: AtomicU64,
    }

    impl WordPair {
        pub(crate) const fn new(lo: u64, hi: u64) -> Self {
            Self {
                lo: AtomicU64::new(lo),
                hi: AtomicU64::new(hi),
            }
        }

        // Sub-word operations: single atomic instruction

        #[inline]
        pub(crate) fn load_lo(&self) -> u64 {
            self.lo.load(Ordering::Acquire)
        }

        #[inline]
        pub(crate) fn load_hi(&self) -> u64 {
            self.hi.load(Ordering::Acquire)
        }

        // DYK: Pipeline based architectures like non-TSOs don't guarantee that
        // sub-word operations are atomic as a pair.
        //
        // Here we have two individual loads (NOT atomic as a pair, algorithm tolerates
        // torn reads; subsequent DCAS catches inconsistencies).
        #[inline]
        pub(crate) fn load(&self) -> (u64, u64) {
            let lo = self.lo.load(Ordering::Acquire);
            let hi = self.hi.load(Ordering::Acquire);
            (lo, hi)
        }

        #[inline]
        pub(crate) fn store_lo(&self, val: u64, order: Ordering) {
            self.lo.store(val, order);
        }

        #[inline]
        pub(crate) fn store_hi(&self, val: u64, order: Ordering) {
            self.hi.store(val, order);
        }

        /// Two individual stores. hi is written first, then lo: lo often
        /// serves as the "signal" (e.g. INVPTR = pending), so it must be
        /// written last to ensure helpers see the correct seqno when they
        /// detect the signal.
        #[inline]
        pub(crate) fn store(&self, lo: u64, hi: u64, order: Ordering) {
            self.hi.store(hi, order);
            self.lo.store(lo, order);
        }

        /// Single atomic swap — replaces the CAS loop in the fallback path.
        #[inline]
        pub(crate) fn exchange_lo(&self, new_lo: u64, order: Ordering) -> u64 {
            self.lo.swap(new_lo, order)
        }

        // -----------------------------------------
        // Full DCAS via AtomicU128 reinterpret cast
        // -----------------------------------------

        #[inline]
        fn as_u128(&self) -> &AtomicU128 {
            // SAFETY: WordPair is #[repr(C, align(16))] with two AtomicU64
            // fields = 16 bytes contiguous, 16-byte aligned, which is same layout as
            // AtomicU128. On native platforms the hardware ensures 8-byte
            // and 16-byte atomics to the same cache line are properly
            // synchronized (x86: LOCK prefix holds cache-line exclusive;
            // aarch64: 8-byte store clears the exclusive monitor so a
            // concurrent STXP retries; s390x: CDSG similar). Don't try to
            // apply aliasing rules here, it is intentional.
            unsafe { &*(self as *const Self as *const AtomicU128) }
        }

        #[inline]
        fn pack(lo: u64, hi: u64) -> u128 {
            (lo as u128) | ((hi as u128) << 64)
        }

        #[inline]
        fn unpack(v: u128) -> (u64, u64) {
            (v as u64, (v >> 64) as u64)
        }

        #[inline]
        pub(crate) fn compare_exchange(
            &self,
            old_lo: u64,
            old_hi: u64,
            new_lo: u64,
            new_hi: u64,
        ) -> Result<(u64, u64), (u64, u64)> {
            match self.as_u128().compare_exchange(
                Self::pack(old_lo, old_hi),
                Self::pack(new_lo, new_hi),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(v) => Ok(Self::unpack(v)),
                Err(v) => Err(Self::unpack(v)),
            }
        }

        #[inline]
        pub(crate) fn compare_exchange_weak(
            &self,
            old_lo: u64,
            old_hi: u64,
            new_lo: u64,
            new_hi: u64,
        ) -> Result<(u64, u64), (u64, u64)> {
            match self.as_u128().compare_exchange_weak(
                Self::pack(old_lo, old_hi),
                Self::pack(new_lo, new_hi),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(v) => Ok(Self::unpack(v)),
                Err(v) => Err(Self::unpack(v)),
            }
        }
    }
}

/// Fallback implementation for platforms without native 128-bit atomics
/// (riscv64, mips64, etc.) where portable_atomic uses a spinlock.
/// Sub-word ops must go through the same AtomicU128 to stay within the
/// spinlock's protection.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "s390x")))]
mod fallback {
    use core::sync::atomic::Ordering;
    use portable_atomic::AtomicU128;

    #[repr(align(16))]
    pub(crate) struct WordPair {
        data: AtomicU128,
    }

    impl WordPair {
        pub(crate) const fn new(lo: u64, hi: u64) -> Self {
            let val = (lo as u128) | ((hi as u128) << 64);
            Self {
                data: AtomicU128::new(val),
            }
        }

        #[inline]
        pub(crate) fn load(&self) -> (u64, u64) {
            let val = self.data.load(Ordering::Acquire);
            (val as u64, (val >> 64) as u64)
        }

        #[inline]
        pub(crate) fn load_lo(&self) -> u64 {
            self.load().0
        }

        #[inline]
        pub(crate) fn load_hi(&self) -> u64 {
            self.load().1
        }

        #[inline]
        pub(crate) fn store(&self, lo: u64, hi: u64, order: Ordering) {
            let val = (lo as u128) | ((hi as u128) << 64);
            self.data.store(val, order);
        }

        #[inline]
        pub(crate) fn store_lo(&self, lo: u64, order: Ordering) {
            loop {
                let old = self.data.load(Ordering::Acquire);
                let hi = (old >> 64) as u64;
                let new = (lo as u128) | ((hi as u128) << 64);
                if self
                    .data
                    .compare_exchange_weak(old, new, order, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }

        #[inline]
        pub(crate) fn store_hi(&self, hi: u64, order: Ordering) {
            loop {
                let old = self.data.load(Ordering::Acquire);
                let lo = old as u64;
                let new = (lo as u128) | ((hi as u128) << 64);
                if self
                    .data
                    .compare_exchange_weak(old, new, order, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }

        #[inline]
        pub(crate) fn compare_exchange(
            &self,
            old_lo: u64,
            old_hi: u64,
            new_lo: u64,
            new_hi: u64,
        ) -> Result<(u64, u64), (u64, u64)> {
            let old = (old_lo as u128) | ((old_hi as u128) << 64);
            let new = (new_lo as u128) | ((new_hi as u128) << 64);
            match self
                .data
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(v) => Ok((v as u64, (v >> 64) as u64)),
                Err(v) => Err((v as u64, (v >> 64) as u64)),
            }
        }

        #[inline]
        pub(crate) fn compare_exchange_weak(
            &self,
            old_lo: u64,
            old_hi: u64,
            new_lo: u64,
            new_hi: u64,
        ) -> Result<(u64, u64), (u64, u64)> {
            let old = (old_lo as u128) | ((old_hi as u128) << 64);
            let new = (new_lo as u128) | ((new_hi as u128) << 64);
            match self
                .data
                .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(v) => Ok((v as u64, (v >> 64) as u64)),
                Err(v) => Err((v as u64, (v >> 64) as u64)),
            }
        }

        #[inline]
        pub(crate) fn exchange_lo(&self, new_lo: u64, order: Ordering) -> u64 {
            loop {
                let old = self.data.load(Ordering::Acquire);
                let old_lo = old as u64;
                let hi = (old >> 64) as u64;
                let new = (new_lo as u128) | ((hi as u128) << 64);
                if self
                    .data
                    .compare_exchange_weak(old, new, order, Ordering::Relaxed)
                    .is_ok()
                {
                    return old_lo;
                }
            }
        }
    }
}

// Re-export the appropriate implementation
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "s390x"))]
pub(crate) use native::WordPair;

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "s390x")))]
pub(crate) use fallback::WordPair;

/// Number of reservation slots per thread (only 1 needed for kovan's API)
pub(crate) const HR_NUM: usize = 1;

/// Total slots per thread: hr_num reservations + 2 helper slots
pub(crate) const SLOTS_PER_THREAD: usize = HR_NUM + 2;

/// Number of thread slots per page (power of 2 for shift/mask indexing).
const SLOTS_PER_PAGE: usize = 128;

/// log2(SLOTS_PER_PAGE) — used for tid >> PAGE_SHIFT.
const PAGE_SHIFT: usize = 7;

/// SLOTS_PER_PAGE - 1 — used for tid & PAGE_MASK.
const PAGE_MASK: usize = 127;

/// Maximum number of pages in the page table. Supports up to 65,536 threads.
const MAX_PAGES: usize = 512;

/// A page of thread slots, allocated on demand.
struct SlotPage([ThreadSlots; SLOTS_PER_PAGE]);

/// Batch retirement frequency (try_retire every `freq` retires)
pub(crate) const RETIRE_FREQ: usize = 64;

/// Epoch advancement frequency (in terms of retire calls per thread)
pub(crate) const EPOCH_FREQ: usize = 128;

/// Unconditional reservation marker for a slot's epoch word.
///
/// A slot publishing this value is eligible for **every** batch in
/// `try_retire`'s scan (`epoch < min_epoch` is false for any `min_epoch`),
/// so every subsequently retired batch inserts a node into the slot and is
/// deferred until the slot's next traversal. This is the wait-free
/// completion path for protected loads: when the global epoch keeps
/// advancing during a load's bounded convergence attempts, the loader
/// escalates to an unconditional reservation and finishes with one more
/// pointer load — protected regardless of birth epochs.
///
/// The marker is cleared (replaced by the real current epoch, with the
/// slot's list traversed) at the next `pin()` boundary or outermost guard
/// drop. The global epoch counter is monotonically increasing from 1 and
/// can never reach this value.
pub(crate) const EPOCH_UNCONDITIONAL: u64 = u64::MAX;

/// Helping state for the slow-path / wait-free mechanism
pub(crate) struct HelpState {
    /// Result pair: (result_ptr, seqno). INVPTR64 in lo means "pending".
    pub(crate) result: WordPair,
    /// Birth epoch of the parent object being protected
    pub(crate) epoch: AtomicU64,
    /// Pointer to the atomic being read (0 = no specific atomic / reserve_slot)
    pub(crate) pointer: AtomicU64,
    /// Parent node pointer (for reference handoff)
    pub(crate) parent: AtomicU64,
}

impl HelpState {
    const fn new() -> Self {
        Self {
            result: WordPair::new(0, 0),
            epoch: AtomicU64::new(0),
            pointer: AtomicU64::new(0),
            parent: AtomicU64::new(0),
        }
    }
}

/// Per-thread slot group: first (list+seqno), epoch (epoch+seqno), state (helping)
#[repr(align(128))]
pub(crate) struct ThreadSlots {
    pub(crate) first: [WordPair; SLOTS_PER_THREAD],
    pub(crate) epoch: [WordPair; SLOTS_PER_THREAD],
    pub(crate) state: [HelpState; SLOTS_PER_THREAD],
}

impl ThreadSlots {
    fn new() -> Self {
        // Initialize: first.lo = INVPTR (inactive), first.hi = 0 (seqno)
        //             epoch.lo = 0, epoch.hi = 0
        //             state = zeroed
        Self {
            first: core::array::from_fn(|_| WordPair::new(INVPTR as u64, 0)),
            epoch: core::array::from_fn(|_| WordPair::new(0, 0)),
            state: core::array::from_fn(|_| HelpState::new()),
        }
    }
}

/// Global ASMR state
pub(crate) struct ASMRState {
    /// Two-level page table of per-thread slot arrays. Pages are allocated on
    /// demand when new thread IDs are assigned.
    pages: [AtomicPtr<SlotPage>; MAX_PAGES],
    /// Global epoch counter (starts at 1)
    epoch: AtomicU64,
    /// Count of threads currently in the slow path
    slow_counter: AtomicU64,
    /// Thread ID allocator (next available ID)
    next_tid: AtomicUsize,
    /// Bitmap of free thread IDs for recycling
    free_tids: TTas<alloc::vec::Vec<usize>>,
    /// Orphaned batches abandoned by exiting threads. Each entry is a
    /// finalized refs-node (batch_link = RNODE(batch_first)) whose batch
    /// could not be submitted via try_retire at thread exit. Stored as
    /// usize because raw pointers are not Send. Adopted (merged into the
    /// adopter's accumulating batch) by enqueue_node/flush.
    orphans: TTas<alloc::vec::Vec<usize>>,
}

/// Null-initialized page table constant for use in array initialization.
#[allow(clippy::declare_interior_mutable_const)]
const NULL_PAGE: AtomicPtr<SlotPage> = AtomicPtr::new(core::ptr::null_mut());

impl ASMRState {
    fn new() -> Self {
        // The DCAS protocol mixes sub-word AtomicU64 operations with
        // AtomicU128 compare-exchange on the same 16 bytes. That is only
        // coherent when the 128-bit atomics are genuine hardware atomics.
        // If portable-atomic fell back to a lock-based emulation (e.g.
        // x86_64 without cmpxchg16b), the sub-word stores would bypass the
        // emulation lock and silently break the DCAS. Watch the "Harper" movie.
        // https://open.spotify.com/track/2iG7MCJdoM4NfvsK2AVgqY
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "s390x"))]
        assert!(
            portable_atomic::AtomicU128::is_lock_free(),
            "kovan requires lock-free 128-bit atomics \
             [x86_64: cmpxchg16b; aarch64: ldxp/stxp OR LSE; s390x: cdsg]"
        );
        Self {
            pages: [NULL_PAGE; MAX_PAGES],
            epoch: AtomicU64::new(1),
            slow_counter: AtomicU64::new(0),
            next_tid: AtomicUsize::new(0),
            free_tids: TTas::new(alloc::vec::Vec::new()),
            orphans: TTas::new(alloc::vec::Vec::new()),
        }
    }

    /// Ensure the page at `page_idx` is allocated. Uses CAS to handle races.
    fn ensure_page(&self, page_idx: usize) {
        if self.pages[page_idx].load(Ordering::Acquire).is_null() {
            let page = Box::into_raw(Box::new(SlotPage(core::array::from_fn(|_| {
                ThreadSlots::new()
            }))));
            if self.pages[page_idx]
                .compare_exchange(
                    core::ptr::null_mut(),
                    page,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                // Another thread allocated this page first — free ours.
                unsafe {
                    drop(Box::from_raw(page));
                }
            }
        }
    }

    /// Get the thread slots for a given thread ID.
    #[inline]
    pub(crate) fn thread_slots(&self, tid: usize) -> &ThreadSlots {
        let page_idx = tid >> PAGE_SHIFT;
        let slot_idx = tid & PAGE_MASK;
        let page = self.pages[page_idx].load(Ordering::Acquire);
        debug_assert!(
            !page.is_null(),
            "kovan: page {page_idx} not allocated for tid {tid}"
        );
        unsafe { &(*page).0[slot_idx] }
    }

    /// Get the current global epoch
    #[inline]
    pub(crate) fn get_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Increment the global epoch
    #[inline]
    pub(crate) fn advance_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Get slow counter value
    #[inline]
    pub(crate) fn slow_counter(&self) -> u64 {
        self.slow_counter.load(Ordering::Acquire)
    }

    /// Increment slow counter
    #[inline]
    pub(crate) fn inc_slow(&self) {
        self.slow_counter.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement slow counter
    #[inline]
    pub(crate) fn dec_slow(&self) {
        self.slow_counter.fetch_sub(1, Ordering::AcqRel);
    }

    /// Number of active thread slots (upper bound for scanning).
    #[inline]
    pub(crate) fn max_threads(&self) -> usize {
        self.next_tid.load(Ordering::Acquire)
    }

    /// Allocate a thread ID
    pub(crate) fn alloc_tid(&self) -> usize {
        // Try recycled IDs first (page already exists for recycled tids)
        {
            let mut free = self.free_tids.lock();
            if let Some(tid) = free.pop() {
                return tid;
            }
        }
        // CAS loop: only increment on success so the counter stays valid
        // if the assert panics and is caught by catch_unwind.
        loop {
            let current = self.next_tid.load(Ordering::Relaxed);
            let page_idx = current >> PAGE_SHIFT;
            assert!(
                page_idx < MAX_PAGES,
                "kovan: exceeded maximum thread count ({})",
                MAX_PAGES * SLOTS_PER_PAGE,
            );
            // Ensure the page is allocated BEFORE publishing the tid via next_tid.
            // This guarantees concurrent scanners (via max_threads()) never see a
            // tid whose page doesn't exist yet.
            self.ensure_page(page_idx);
            match self.next_tid.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return current,
                Err(_) => continue,
            }
        }
    }

    /// Release a thread ID for recycling.
    ///
    /// Deactivation must be race-aware: a concurrent `try_retire` may insert
    /// a node into a slot at any time before the slot reads INVPTR. A blind
    /// `store(INVPTR)` would obliterate such a node — its refs-decrement
    /// would never happen and the whole batch would leak. So the list word
    /// is *exchanged* to INVPTR and any captured list is returned to the
    /// caller for traversal.
    ///
    /// Seqnos (the `hi` halves) are deliberately preserved: the slow-path
    /// parity protocol relies on seqnos never repeating for a slot, even
    /// across tid recycling. Only the epoch `lo` is cleared (lowering an
    /// epoch is the safe direction — the slot merely stops being eligible).
    ///
    /// Returns up to SLOTS_PER_THREAD captured list heads that the caller
    /// must traverse (entries are null when there was nothing to capture).
    pub(crate) fn free_tid(&self, tid: usize) -> [u64; SLOTS_PER_THREAD] {
        let slots = self.thread_slots(tid);
        let mut captured = [0u64; SLOTS_PER_THREAD];
        for (j, cap) in captured.iter_mut().enumerate() {
            // Stop eligibility first so try_retire's insert-phase re-check
            // (epoch < min_epoch) rejects the slot before we capture.
            slots.epoch[j].store_lo(0, Ordering::SeqCst);
            let first = slots.first[j].exchange_lo(INVPTR as u64, Ordering::AcqRel);
            if first != INVPTR as u64 && first != 0 {
                *cap = first;
            }
        }
        let mut free = self.free_tids.lock();
        free.push(tid);
        captured
    }

    /// Park an orphaned batch (finalized refs-node) for later adoption.
    pub(crate) fn push_orphan(&self, refs_node: usize) {
        let mut orphans = self.orphans.lock();
        orphans.push(refs_node);
    }

    /// Take one orphaned batch for adoption, if any.
    pub(crate) fn pop_orphan(&self) -> Option<usize> {
        let mut orphans = self.orphans.lock();
        orphans.pop()
    }

    /// HR_NUM getter
    #[inline]
    pub(crate) fn hr_num(&self) -> usize {
        HR_NUM
    }
}

use once_cell::race::OnceBox;

/// Global singleton instance
static GLOBAL: OnceBox<ASMRState> = OnceBox::new();

/// Get reference to global ASMR state
#[inline]
pub(crate) fn global() -> &'static ASMRState {
    GLOBAL.get_or_init(|| Box::new(ASMRState::new()))
}
