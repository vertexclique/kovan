//! Slot-based retirement list management
//!
//! This module implements the core slot structure with packed atomic operations
//! for reference counting and retirement list management.

use alloc::boxed::Box;
use core::sync::atomic::Ordering;

// Always use portable_atomic for AtomicU128.
// core::sync::atomic::AtomicU128 requires #![feature(integer_atomics)] (unstable).
// portable_atomic uses native hardware instructions where available:
// - aarch64: LDXP/STXP (LL/SC) or CASP — lock-free, always lock-free
// - x86-64: CMPXCHG16B (when target-feature=+cmpxchg16b) — lock-free
// - fallback: spinlock-based for platforms without native 128-bit atomics
use portable_atomic::AtomicU128;

#[cfg(feature = "robust")]
use core::sync::atomic::{AtomicIsize, AtomicU64};

/// Packed atomic structure storing reference count + list pointer
///
/// Layout:
/// - [127:64] = List pointer (head of retirement list)
/// - [63:0]   = Reference count (active threads in slot)
#[repr(align(16))]
pub(crate) struct SlotHead {
    data: AtomicU128,
}

impl SlotHead {
    const PTR_SHIFT: u32 = 64;
    const PTR_MASK: u128 = !0u128 << Self::PTR_SHIFT;
    const REF_MASK: u128 = (1u128 << Self::PTR_SHIFT) - 1;

    /// Create a new SlotHead with zero ref count and null pointer
    pub(crate) const fn new() -> Self {
        Self {
            data: AtomicU128::new(0),
        }
    }

    /// Atomically increment reference count by 1
    ///
    /// Returns: (old_refcount, old_list_ptr)
    #[inline]
    pub(crate) fn fetch_add_ref(&self) -> (u64, usize) {
        let old = self.data.fetch_add(1, Ordering::AcqRel);
        let refs = (old & Self::REF_MASK) as u64;
        let ptr = ((old & Self::PTR_MASK) >> Self::PTR_SHIFT) as usize;
        (refs, ptr)
    }

    /// Atomically decrement reference count by 1
    ///
    /// Returns: (new_refcount, current_list_ptr)
    #[inline]
    pub(crate) fn fetch_sub_ref(&self) -> (u64, usize) {
        let old = self.data.fetch_sub(1, Ordering::AcqRel);
        let refs = ((old & Self::REF_MASK) as u64).wrapping_sub(1);
        let ptr = ((old & Self::PTR_MASK) >> Self::PTR_SHIFT) as usize;
        (refs, ptr)
    }

    /// Atomically load both reference count and list pointer
    ///
    /// Returns: (refcount, list_ptr)
    #[inline]
    pub(crate) fn load(&self) -> (u64, usize) {
        let val = self.data.load(Ordering::Acquire);
        let refs = (val & Self::REF_MASK) as u64;
        let ptr = ((val & Self::PTR_MASK) >> Self::PTR_SHIFT) as usize;
        (refs, ptr)
    }

    /// Atomically compare-exchange both reference count and list pointer
    ///
    /// Returns: Ok(()) on success, Err((actual_refs, actual_ptr)) on failure
    #[inline]
    pub(crate) fn compare_exchange(
        &self,
        current_refs: u64,
        current_ptr: usize,
        new_refs: u64,
        new_ptr: usize,
    ) -> Result<(), (u64, usize)> {
        let current = (current_refs as u128) | ((current_ptr as u128) << Self::PTR_SHIFT);
        let new = (new_refs as u128) | ((new_ptr as u128) << Self::PTR_SHIFT);

        match self
            .data
            .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(actual) => {
                let refs = (actual & Self::REF_MASK) as u64;
                let ptr = ((actual & Self::PTR_MASK) >> Self::PTR_SHIFT) as usize;
                Err((refs, ptr))
            }
        }
    }
}

/// Per-slot state for retirement list management
#[repr(align(128))]
pub(crate) struct Slot {
    /// Packed reference count + list head pointer
    pub(crate) head: SlotHead,

    /// Latest era accessed in this slot (for robust feature)
    #[cfg(feature = "robust")]
    pub(crate) access_era: AtomicU64,

    /// Acknowledgment counter for stall detection (for robust feature)
    #[cfg(feature = "robust")]
    pub(crate) ack_counter: AtomicIsize,
}

impl Slot {
    /// Create a new Slot with zero state
    pub(crate) const fn new() -> Self {
        Self {
            head: SlotHead::new(),
            #[cfg(feature = "robust")]
            access_era: AtomicU64::new(0),
            #[cfg(feature = "robust")]
            ack_counter: AtomicIsize::new(0),
        }
    }
}

/// Global state containing fixed array of slots
pub(crate) struct GlobalState {
    slots: &'static [Slot],
    slot_order: u32,
}

impl GlobalState {
    /// Create global state with 2^order slots
    ///
    /// Default: order=6 (64 slots)
    pub(crate) fn new(order: u32) -> Self {
        let num_slots = 1usize << order;

        // Allocate slots as static array
        let slots = {
            let mut vec = alloc::vec::Vec::with_capacity(num_slots);
            for _ in 0..num_slots {
                vec.push(Slot::new());
            }
            vec.into_boxed_slice()
        };

        Self {
            slots: Box::leak(slots),
            slot_order: order,
        }
    }

    /// Get slot by index
    #[inline]
    pub(crate) fn slot(&self, index: usize) -> &Slot {
        &self.slots[index & self.slot_mask()]
    }

    /// Get total number of slots
    #[inline]
    pub(crate) fn num_slots(&self) -> usize {
        self.slots.len()
    }

    /// Get slot mask for index wrapping
    #[inline]
    pub(crate) fn slot_mask(&self) -> usize {
        (1usize << self.slot_order) - 1
    }

    /// Get slot order (log2 of slot count)
    #[inline]
    pub(crate) fn slot_order(&self) -> u32 {
        self.slot_order
    }
}

use once_cell::race::OnceBox;

/// Global singleton instance
static GLOBAL: OnceBox<GlobalState> = OnceBox::new();

/// Get reference to global state
#[inline]
pub(crate) fn global() -> &'static GlobalState {
    GLOBAL.get_or_init(|| Box::new(GlobalState::new(6)))
}

/// Calculate adjustment value for reference counting
///
/// ADDEND = (~0 >> order) + 1
#[inline]
pub(crate) fn calculate_adjustment(order: u32) -> isize {
    ((!0usize >> order) + 1) as isize
}
