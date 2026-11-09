//! Slot structures and global state.
//!
//! Each thread with an ID owns a reservation slot: a list word (INVPTR while
//! the slot is inactive, else the head of the list of retired batches placed
//! in it) and an era word (the epoch its loads are protected from), on a cache
//! line of their own. The global state holds the epoch counter and the thread
//! ID allocator.

use crate::cache_padded::CachePadded;
use crate::retired::INVPTR;
use alloc::boxed::Box;
use core::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

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

// `RetiredNode::set_slot_info` keeps a tid in a pointer-sized word, so the
// table never hands out more IDs than a `usize` holds at any width.
const _: () = assert!(
    MAX_THREADS <= 1_usize << (usize::BITS - 1),
    "MAX_PAGES * SLOTS_PER_PAGE exceeds the tid field of the slot info"
);

/// A page of thread slots, allocated on demand.
struct SlotPage([ThreadSlots; SLOTS_PER_PAGE]);

/// Batch retirement frequency (try_retire every `freq` retires)
pub(crate) const RETIRE_FREQ: usize = 64;

/// Epoch advancement frequency: a thread advances at its first retire, then every `EPOCH_FREQ`th
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
/// pointer load - protected regardless of birth epochs.
///
/// The marker ends with the critical section that escalated: the outermost
/// guard drop leaves the slot (its list word becomes INVPTR, so no scan
/// reads its epoch) and resets the era word to 0, the era a slot starts
/// with, so the first protected load of the next section publishes the
/// epoch it reads. The global epoch counter is monotonically increasing
/// from 1 and can never reach this value.
pub(crate) const EPOCH_UNCONDITIONAL: u64 = u64::MAX;

/// A thread's reservation slot: the list word and the era word, on a cache
/// line of their own. The list word is INVPTR while the slot is inactive: a
/// thread's reservation is active only between its outermost `pin()` and
/// the drop of that guard, which leaves the slot. The era word is written
/// by the owner alone: a protected load that finds the global epoch moved
/// publishes the epoch it read, and the leave of an escalated section
/// resets it to 0.
#[repr(align(128))]
pub(crate) struct ThreadSlots {
    pub(crate) first: AtomicUsize,
    pub(crate) epoch: AtomicU64,
}

impl ThreadSlots {
    const fn new() -> Self {
        Self {
            first: AtomicUsize::new(INVPTR),
            epoch: AtomicU64::new(0),
        }
    }
}

/// Global ASMR state
pub(crate) struct ASMRState {
    /// Two-level page table of per-thread slot arrays. Pages are allocated on
    /// demand when new thread IDs are assigned.
    pages: [AtomicPtr<SlotPage>; MAX_PAGES],
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
/// [`ASMRState`]: every protected load and every allocation reads it, and a
/// static is one load away, with no state pointer to load and check
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
        Self {
            pages: [NULL_PAGE; MAX_PAGES],
            next_tid: AtomicUsize::new(0),
            released: [const { AtomicU64::new(0) }; MAX_THREADS / 64],
        }
    }

    /// Ensure the page at `page_idx` is allocated. Uses CAS to handle races.
    fn ensure_page(&self, page_idx: usize) {
        if self.pages[page_idx].load(Ordering::Acquire).is_null() {
            let page = Box::into_raw(Box::new(SlotPage(
                [const { ThreadSlots::new() }; SLOTS_PER_PAGE],
            )));
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
}

use once_cell::race::OnceBox;

/// Global singleton instance
static GLOBAL: OnceBox<ASMRState> = OnceBox::new();

/// Get reference to global ASMR state
#[inline]
pub(crate) fn global() -> &'static ASMRState {
    GLOBAL.get_or_init(|| Box::new(ASMRState::new()))
}
