//! Slot structures and global state.
//!
//! Each thread gets its own `ThreadSlots` containing per-reservation DCAS pairs
//! for the retirement list and epoch, plus helping state. The global state holds
//! the epoch counter, slow-path counter, and thread ID allocator.

use crate::cache_padded::CachePadded;
use crate::retired::INVPTR;
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

        /// Single atomic swap - replaces the CAS loop in the fallback path.
        #[inline]
        pub(crate) fn exchange_lo(&self, new_lo: u64, order: Ordering) -> u64 {
            self.lo.swap(new_lo, order)
        }

        /// Compare-exchange of the `hi` half alone: a change of `lo` never
        /// fails it. One instruction.
        #[inline]
        pub(crate) fn compare_exchange_hi(&self, old_hi: u64, new_hi: u64) -> bool {
            self.hi
                .compare_exchange(old_hi, new_hi, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        }

        /// Compare-exchange of the `lo` half alone: a change of `hi` never
        /// fails it. One instruction.
        #[inline]
        pub(crate) fn compare_exchange_lo(&self, old_lo: u64, new_lo: u64) -> bool {
            self.lo
                .compare_exchange(old_lo, new_lo, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
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
    }
}

/// Fallback implementation for platforms without native 128-bit atomics
/// (riscv64, mips64, etc.) where portable_atomic uses a spinlock.
/// Sub-word ops must go through the same AtomicU128 to stay within the
/// spinlock's protection. Each op is a compare-exchange loop over that
/// word, which another thread's write can fail: on these targets no
/// operation is wait-free, or lock-free (see lib.rs).
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

        /// Compare-exchange of the `hi` half, keeping whatever `lo` holds.
        #[inline]
        pub(crate) fn compare_exchange_hi(&self, old_hi: u64, new_hi: u64) -> bool {
            loop {
                let old = self.data.load(Ordering::Acquire);
                if (old >> 64) as u64 != old_hi {
                    return false;
                }
                let new = (old as u64 as u128) | ((new_hi as u128) << 64);
                if self
                    .data
                    .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return true;
                }
            }
        }

        /// Compare-exchange of the `lo` half, keeping whatever `hi` holds.
        #[inline]
        pub(crate) fn compare_exchange_lo(&self, old_lo: u64, new_lo: u64) -> bool {
            loop {
                let old = self.data.load(Ordering::Acquire);
                if old as u64 != old_lo {
                    return false;
                }
                let new = (old >> 64 << 64) | new_lo as u128;
                if self
                    .data
                    .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return true;
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

/// log2(SLOTS_PER_PAGE) - used for tid >> PAGE_SHIFT.
const PAGE_SHIFT: usize = 7;

/// SLOTS_PER_PAGE - 1 - used for tid & PAGE_MASK.
const PAGE_MASK: usize = 127;

/// Maximum number of pages in the page table. Supports up to 65,536 threads.
const MAX_PAGES: usize = 512;

/// Maximum number of distinct thread IDs the page table can hand out.
const MAX_THREADS: usize = MAX_PAGES * SLOTS_PER_PAGE;

// `RetiredNode::set_slot_info` packs (tid, slot_index) into one `usize`, with
// `slot_index` in the low `SLOT_INFO_INDEX_BITS` bits and `tid` above it. The
// tid field is 48 bits wide on a 64-bit target but only 16 on a 32-bit one, so
// these bounds are load-bearing for 32-bit support (wasm32, i686) rather than
// merely documentary. Growing MAX_PAGES or SLOTS_PER_PAGE past 2^16 total
// threads breaks the 32-bit build here instead of silently truncating a tid at
// runtime.
const _: () = assert!(
    MAX_THREADS <= 1_usize << (usize::BITS - crate::retired::SLOT_INFO_INDEX_BITS),
    "MAX_PAGES * SLOTS_PER_PAGE exceeds the tid field of the packed slot info"
);
const _: () = assert!(
    SLOTS_PER_THREAD <= 1_usize << crate::retired::SLOT_INFO_INDEX_BITS,
    "SLOTS_PER_THREAD exceeds the slot_index field of the packed slot info"
);

/// A page of thread slots, allocated on demand.
struct SlotPage([ThreadSlots; SLOTS_PER_PAGE]);

/// Batch retirement frequency (try_retire every `freq` retires)
pub(crate) const RETIRE_FREQ: usize = 64;

/// Epoch advancement frequency: a thread advances at its first retire, then every `EPOCH_FREQ`th
pub(crate) const EPOCH_FREQ: usize = 128;

/// The tag of a left slot's era word, its top bit. The leave of a slot sets
/// it, before the exchange that makes the list word INVPTR, and the next
/// activation clears it, before the exchange that makes the list word 0: a
/// scan skips a tagged slot (`era_covers`), and a retire that exchanged a
/// node into a slot and then reads the tag knows the slot was left since
/// its scan, so it takes the node out again (`try_retire`). The era the
/// slot last published stays in the low bits. Every slot of a thread (the
/// reservation and the helper slots) carries it.
pub(crate) const ERA_INACTIVE: u64 = 1 << 63;

/// Unconditional reservation marker for a slot's epoch word: the largest
/// untagged era.
///
/// A slot publishing this value is eligible for **every** batch in
/// `try_retire`'s scan (`era_covers` holds for any `min_epoch`), so every
/// subsequently retired batch inserts a node into the slot and is deferred
/// until the slot's next traversal. This is the wait-free completion path
/// for protected loads: when the global epoch keeps advancing during a
/// load's bounded convergence attempts, the loader escalates to an
/// unconditional reservation and finishes with one more pointer load -
/// protected regardless of birth epochs.
///
/// The marker ends with the critical section that escalated: the outermost
/// guard drop leaves the slot (its list word becomes INVPTR, so no scan
/// reads its epoch) and resets the era to 0, so the next outermost `pin()`
/// publishes the real current epoch. The global epoch counter is
/// monotonically increasing from 1 and can never reach this value.
pub(crate) const EPOCH_UNCONDITIONAL: u64 = ERA_INACTIVE - 1;

/// Whether a slot whose era word reads `era` reserves the nodes born at
/// `min_epoch` or later: the algorithm's `era >= min_epoch`, false for a
/// left slot (tagged: negative as a signed word, below every birth) and
/// true for an escalated one (the largest untagged value). One compare.
#[inline]
pub(crate) fn era_covers(era: u64, min_epoch: u64) -> bool {
    era as i64 >= min_epoch as i64
}

/// Whether `era`, read from a slot's era word, carries the tag of a left
/// slot (`ERA_INACTIVE`).
#[inline]
pub(crate) fn era_left(era: u64) -> bool {
    (era as i64) < 0
}

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

/// Per-thread slot group: first (list+seqno), epoch (epoch+seqno) and state
/// (helping). A slot's list word is INVPTR while the slot is inactive: a
/// thread's reservation (index 0) is active only between its outermost
/// `pin()` and the drop of that guard, which leaves the slot.
#[repr(align(128))]
pub(crate) struct ThreadSlots {
    pub(crate) first: [WordPair; SLOTS_PER_THREAD],
    pub(crate) epoch: [WordPair; SLOTS_PER_THREAD],
    pub(crate) state: [HelpState; SLOTS_PER_THREAD],
}

impl ThreadSlots {
    fn new() -> Self {
        // Initialize: first.lo = INVPTR (inactive), first.hi = 0 (seqno)
        //             epoch.lo = ERA_INACTIVE (left, era 0), epoch.hi = 0
        //             state = zeroed
        Self {
            first: core::array::from_fn(|_| WordPair::new(INVPTR as u64, 0)),
            epoch: core::array::from_fn(|_| WordPair::new(ERA_INACTIVE, 0)),
            state: core::array::from_fn(|_| HelpState::new()),
        }
    }

    /// [Enter] Activate slot `index`: one store clears the tag of the era
    /// word (the era the slot last published stays), then one exchange of
    /// the list word to 0 takes what it held. Returns the list word
    /// displaced (0, INVPTR, or a list the caller traverses).
    ///
    /// The store comes first, so a retire whose exchange lands after ours
    /// reads the era untagged and keeps its node: with the tag still set it
    /// would take the node out of a slot the owner believes active
    /// (`try_retire`, `InsertCheck`). The exchange, never a store: the slot
    /// left INVPTR may hold a node a retire exchanged in and has not rolled
    /// back yet; the exchange takes it, the rollback then fails and counts
    /// the node, and the caller's traversal balances that count. SeqCst; on
    /// x86-64 the guard's SeqCst protected loads pair with the scan's fence,
    /// elsewhere the caller follows it with `publish_fence`.
    #[inline(always)]
    pub(crate) fn activate(&self, index: usize) -> u64 {
        let era = self.epoch[index].load_lo();
        if era_left(era) {
            // Relaxed: the exchange that follows releases it.
            self.epoch[index].store_lo(era & !ERA_INACTIVE, Ordering::Relaxed);
        }
        self.first[index].exchange_lo(0, Ordering::SeqCst)
    }

    /// [LeaveMark] and [Leave] Leave slot `index`: one store tags the era
    /// word (the escalated era reset to 0), then one exchange of the list
    /// word to INVPTR. Returns the list word displaced (0, INVPTR, or a list
    /// the caller traverses). The tag is stored before the exchange, so a
    /// retire whose exchange lands after ours reads it and takes its node
    /// out again (`try_retire`, `InsertCheck`).
    #[inline(always)]
    pub(crate) fn leave(&self, index: usize) -> u64 {
        let era = self.epoch[index].load_lo() & !ERA_INACTIVE;
        let kept = if era == EPOCH_UNCONDITIONAL { 0 } else { era };
        // Release: a retire's scan reads the era with a plain Acquire load
        // after a plain load of the list word (`try_retire`, the scan and the
        // insert's re-check), not only after an exchange of its own. A scan
        // that reads the list word still active and this tag skips the slot,
        // and the batch is freed elsewhere: only a Release store makes this
        // section's reads happen before that free. Relaxed held on x86, which
        // keeps loads ahead of later stores; ARM and POWER do not.
        self.epoch[index].store_lo(kept | ERA_INACTIVE, Ordering::Release);
        self.first[index].exchange_lo(INVPTR as u64, Ordering::AcqRel)
    }
}

/// Global ASMR state
pub(crate) struct ASMRState {
    /// Two-level page table of per-thread slot arrays. Pages are allocated on
    /// demand when new thread IDs are assigned.
    pages: [AtomicPtr<SlotPage>; MAX_PAGES],
    /// Count of threads currently in the slow path
    slow_counter: AtomicU64,
    /// Thread ID allocator (next available ID)
    next_tid: AtomicUsize,
    /// Released thread IDs, one bit per ID: set by
    /// [`release_tid`](Self::release_tid), cleared by the
    /// [`alloc_tid`](Self::alloc_tid) that takes the ID over. An ID never
    /// handed out has its bit clear, so only its first owner takes it.
    released: [AtomicU64; MAX_THREADS / 64],
}

/// Global epoch counter (starts at 1).
///
/// A static of its own rather than a field of the lazily built
/// [`ASMRState`]: every outermost `pin()` and every protected load reads it,
/// and a static is one load away, with no state pointer to load and check
/// first. Padded so the line it lives on holds nothing else.
static EPOCH: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(1));

/// Current global epoch.
#[inline]
pub(crate) fn epoch() -> u64 {
    EPOCH.load(Ordering::Acquire)
}

/// Advance the global epoch by one; returns the epoch it advanced to.
#[inline]
pub(crate) fn advance_epoch() -> u64 {
    EPOCH.fetch_add(1, Ordering::AcqRel) + 1
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
            slow_counter: AtomicU64::new(0),
            next_tid: AtomicUsize::new(0),
            released: [const { AtomicU64::new(0) }; MAX_THREADS / 64],
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
                // Another thread allocated this page first - free ours.
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

    /// Allocate a thread ID: a released one when there is one, else the next
    /// one never handed out.
    ///
    /// Wait-free, and no step waits for another thread. Step `TidClaim` is
    /// one pass over the release words of the IDs handed out so far, with at
    /// most one `fetch_and` per released bit the pass loaded: the bits it
    /// tries come from its own load of the word, never from a reload, so a
    /// word costs at most 64 claims whatever other threads release
    /// meanwhile, and the pass at most `MAX_THREADS` in all. An ID another
    /// thread claims first is passed over. Step `TidFresh` then takes
    /// `next_tid` with a compare-exchange that fails only when another thread
    /// took that ID first, which can happen at most `MAX_THREADS` times in
    /// the process.
    pub(crate) fn alloc_tid(&self) -> usize {
        let handed_out = self.next_tid.load(Ordering::Acquire);
        for (w, word) in self.released[..handed_out.div_ceil(64)].iter().enumerate() {
            let mut seen = word.load(Ordering::Relaxed);
            while seen != 0 {
                let bit = seen.isolate_lowest_one();
                seen ^= bit;
                // Acquire: pairs with the release in `release_tid`, so the
                // exited owner's last writes to the ID's slots happen before
                // this thread's first.
                if word.fetch_and(!bit, Ordering::Acquire) & bit != 0 {
                    return w * 64 + bit.trailing_zeros() as usize;
                }
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
            // Strong: a failure means another thread took `current`, never a
            // spurious one, which keeps the retries bounded.
            if self
                .next_tid
                .compare_exchange(current, current + 1, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return current;
            }
        }
    }

    /// Release a thread ID for recycling, once its thread has left its
    /// reservation (every slot of the ID holds INVPTR). Step `TidRelease`:
    /// one `fetch_or`, whose release orders every write the owner made to
    /// the ID's slots before the next owner's claim.
    pub(crate) fn release_tid(&self, tid: usize) {
        self.released[tid / 64].fetch_or(1 << (tid % 64), Ordering::Release);
    }

    /// Whether `tid` is released and waiting to be handed out again.
    #[cfg(test)]
    pub(crate) fn tid_is_released(&self, tid: usize) -> bool {
        self.released[tid / 64].load(Ordering::SeqCst) & (1 << (tid % 64)) != 0
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
