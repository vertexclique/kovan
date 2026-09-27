//! The hopscotch table: its buckets (each a slot and, for the bucket as a home, a control word
//! holding the home's hop bits, writer guard and move stamp), the entries they hold, and the
//! home guard a writer holds while it links, replaces, unlinks or moves an entry of a home. Only
//! a home guard's holder changes the home's hop bits and stamp, and every occupied slot of a home
//! is written only under that home's guard.

extern crate alloc;

use super::{MIN_CAPACITY, NEIGHBORHOOD_SIZE};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::borrow::Borrow;
use crate::sync::AtomicU64;
use core::sync::atomic::Ordering;
use kovan::{Atomic, RetiredNode, Shared, pin};

// A bucket's control word describes the bucket as a home, in three fields:
// - the hop bits (`HOP_MASK`): bit `i` set means slot `home + i` holds an entry of this home. An
//   entry's bit is set after it is linked and cleared after it is unlinked, and a move sets the
//   new slot's bit before it unlinks the old slot, so from the publication of its bit to its
//   removal an entry is always in a slot its home's bits name;
// - the home's writer guard (`GUARD`, Herlihy-style hopscotch): held across every write that
//   links, replaces, unlinks or moves an entry of the home (an insert's existence scan and slot
//   claim, a remove, a displacement moving one of the home's entries) and, for every home at
//   once, by a resize or a clear. Readers never take it;
// - the move stamp (`STAMP_MASK`): advanced each time an entry of the home moves, before the entry
//   leaves its old slot. A lookup that misses re-reads the word and scans again when the stamp
//   moved (see `get`). It wraps after 2^31 moves; a lookup could only be fooled by exactly a
//   multiple of 2^31 moves of one home's entries between its two reads of the word, and an entry
//   moves at most `NEIGHBORHOOD_SIZE - 1` times.
// Only the guard's holder changes the hop bits and the stamp (a writer that fails to take the
// guard ORs in a bit that is already set, changing nothing), so the holder writes the word with
// plain stores from its own copy (`HomeGuard`). One word keeps a bucket at 16 bytes and gives a
// lookup its hop bits and its stamp in one load.

/// The hop bits of a control word.
pub(super) const HOP_MASK: u64 = (1u64 << NEIGHBORHOOD_SIZE) - 1;

/// The home's writer guard in a control word.
pub(super) const GUARD: u64 = 1u64 << NEIGHBORHOOD_SIZE;

/// One step of a control word's move stamp.
const STAMP_UNIT: u64 = GUARD << 1;

/// The move stamp of a control word.
pub(super) const STAMP_MASK: u64 = !(HOP_MASK | GUARD);

const _: () = assert!(NEIGHBORHOOD_SIZE <= 32, "a home's hop bits fit a u32");

/// The hop bits of a control word.
#[inline(always)]
pub(super) fn hop_bits(word: u64) -> u32 {
    (word & HOP_MASK) as u32
}

/// The hop bit of the slot `offset` past its home.
#[inline(always)]
pub(super) fn hop_bit(offset: usize) -> u64 {
    1u64 << offset
}

/// A bucket in the hopscotch hash table
pub(super) struct Bucket<K, V> {
    /// This bucket as a home: its hop bits, its writer guard and its move stamp (see the
    /// control word comment above `HOP_MASK`).
    pub(super) control: AtomicU64,
    /// The actual key-value slot at this position
    pub(super) slot: Atomic<Entry<K, V>>,
}

/// A held writer guard of one home bucket, with the home's control word as this holder last
/// wrote it. Dropping it releases the guard, publishing the staged hop bits in the same store.
pub(super) struct HomeGuard<'t> {
    control: &'t AtomicU64,
    /// The home bucket's index.
    pub(super) idx: usize,
    /// The control word as this holder publishes it next, guard bit set.
    word: u64,
}

impl HomeGuard<'_> {
    /// The home's hop bits.
    #[inline(always)]
    pub(super) fn hops(&self) -> u32 {
        hop_bits(self.word)
    }

    /// Stage the hop bit of the slot `offset` past the home (an entry linked there), published
    /// when the guard is released.
    #[inline(always)]
    pub(super) fn stage_linked(&mut self, offset: usize) {
        self.word |= hop_bit(offset);
    }

    /// Stage clearing the hop bit of the slot `offset` past the home (its entry unlinked),
    /// published when the guard is released.
    #[inline(always)]
    pub(super) fn stage_unlinked(&mut self, offset: usize) {
        self.word &= !hop_bit(offset);
    }

    /// Publish now, still holding the guard, the hop bit of the slot `offset` past the home (an
    /// entry moved there) with the move stamp advanced.
    #[inline]
    pub(super) fn publish_moved_in(&mut self, offset: usize) {
        // Wrapping: the stamp is the top of the word, a carry out of it is dropped.
        self.word = (self.word | hop_bit(offset)).wrapping_add(STAMP_UNIT);
        // Release: a reader that acquires this word sees the entry linked at `offset` before it.
        self.control.store(self.word, Ordering::Release);
    }
}

impl Drop for HomeGuard<'_> {
    fn drop(&mut self) {
        // Release: pairs with the next holder's acquire (and a reader's), publishing every slot
        // and hop bit this holder wrote.
        self.control.store(self.word & !GUARD, Ordering::Release);
    }
}

/// An entry in the hash table
#[repr(C)]
pub(super) struct Entry<K, V> {
    pub(super) retired: RetiredNode,
    pub(super) hash: u64,
    pub(super) key: K,
    pub(super) value: V,
}

// SAFETY (kovan retirement rule): a retired Entry's destructor may run on
// any thread, and entries (with K and V inside) move between threads -
// hence `K: Send, V: Send` for Send. Lookups DO produce `&K`/`&V` from a
// shared `&Entry` (get() clones V through &V under concurrent readers),
// so Sync additionally requires `K: Sync, V: Sync` - the same bounds the
// map-level Sync impl has always required for sharing the map.
unsafe impl<K: Send, V: Send> Send for Entry<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Entry<K, V> {}

/// The null entry pointer: a free slot.
#[inline(always)]
pub(super) fn null_entry<'g, K, V>() -> Shared<'g, Entry<K, V>> {
    // SAFETY: a null `Shared` points at nothing, so it is valid for any lifetime.
    unsafe { Shared::from_raw(core::ptr::null_mut()) }
}

/// Claim the free slot `slot` for `entry`: `Ok` when the slot took it (the table owns it now),
/// `Err` with the entry back when another writer took the slot first.
pub(super) fn link<K, V>(
    slot: &Atomic<Entry<K, V>>,
    entry: Box<Entry<K, V>>,
    guard: &kovan::Guard,
) -> Result<(), Box<Entry<K, V>>> {
    let raw = Box::into_raw(entry);
    // Release: a reader that acquires the slot sees the entry's fields. Relaxed on failure: the
    // value read is not used.
    match slot.compare_exchange(
        null_entry(),
        unsafe { Shared::from_raw(raw) },
        Ordering::Release,
        Ordering::Relaxed,
        guard,
    ) {
        Ok(_) => Ok(()),
        // SAFETY: the CAS failed, so `raw` was never published: it is still the allocation
        // `Box::into_raw` gave this call.
        Err(_) => Err(unsafe { Box::from_raw(raw) }),
    }
}

/// The hash table structure
#[repr(C)]
pub(super) struct Table<K, V> {
    retired: RetiredNode,
    pub(super) buckets: Box<[Bucket<K, V>]>,
    pub(super) capacity: usize,
    mask: usize,
}

// SAFETY (kovan retirement rule): same reasoning as Entry - a retired
// Table's destructor may run on any thread (hence K, V: Send via the
// contained entries); shared access to entries through a `&Table` carries
// Entry's Sync requirements.
unsafe impl<K: Send, V: Send> Send for Table<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Table<K, V> {}

impl<K, V> Table<K, V> {
    pub(super) fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(MIN_CAPACITY);
        // We add padding to the array so we don't have to check bounds constantly
        // during neighborhood scans.
        let mut buckets = Vec::with_capacity(capacity + NEIGHBORHOOD_SIZE);

        for _ in 0..(capacity + NEIGHBORHOOD_SIZE) {
            buckets.push(Bucket {
                control: AtomicU64::new(0),
                slot: Atomic::null(),
            });
        }

        Self {
            retired: RetiredNode::new(),
            buckets: buckets.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
        }
    }

    #[inline(always)]
    pub(super) fn bucket_index(&self, hash: u64) -> usize {
        (hash as usize) & self.mask
    }

    #[inline(always)]
    pub(super) fn get_bucket(&self, idx: usize) -> &Bucket<K, V> {
        // SAFETY: Internal indices are calculated via mask or bounded offset loops.
        // The buckets array has padding to handle overflow up to NEIGHBORHOOD_SIZE.
        unsafe { self.buckets.get_unchecked(idx) }
    }

    /// Whether slot `idx` looks free. Relaxed: only a hint for where to try, the claim's CAS
    /// decides.
    #[inline(always)]
    pub(super) fn looks_free(&self, idx: usize, guard: &kovan::Guard) -> bool {
        self.get_bucket(idx)
            .slot
            .load(Ordering::Relaxed, guard)
            .is_null()
    }

    /// Take the writer guard of the home bucket `idx` without waiting: `None` when another
    /// writer holds it.
    #[inline]
    pub(super) fn home_guard(&self, idx: usize) -> Option<HomeGuard<'_>> {
        let control = &self.get_bucket(idx).control;
        // Acquire: pairs with the previous holder's release, so every slot and hop bit it wrote
        // is visible to this holder.
        let prev = control.fetch_or(GUARD, Ordering::Acquire);
        if prev & GUARD != 0 {
            return None;
        }
        Some(HomeGuard {
            control,
            idx,
            word: prev | GUARD,
        })
    }

    /// The entry of `key` among the slots `hops` names past the home `home`, and its offset
    /// there.
    #[inline]
    pub(super) fn find<'g, Q>(
        &self,
        home: usize,
        hops: u32,
        hash: u64,
        key: &Q,
        guard: &'g kovan::Guard,
    ) -> Option<(usize, Shared<'g, Entry<K, V>>)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut rest = hops;
        while rest != 0 {
            let offset = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            let slot = &self.get_bucket(home + offset).slot;
            // Acquire: pairs with the release that linked the entry, so its fields are visible.
            let entry_ptr = slot.load(Ordering::Acquire, guard);
            if !entry_ptr.is_null() {
                // SAFETY: loaded under `guard`, which keeps it from being freed.
                let entry = unsafe { &*entry_ptr.as_raw() };
                if entry.hash == hash && entry.key.borrow() == key {
                    return Some((offset, entry_ptr));
                }
            }
        }
        None
    }
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        // Exclusive at this point: either the map itself is being dropped,
        // or kovan reclaimed the table after every guard that could observe
        // it has been released. Slots hold exactly the entries that were
        // never individually unlinked+retired (remove/clear null the slot
        // before retiring), so each entry is freed exactly once. No entry is
        // in two slots: a move links an entry twice only while it holds the
        // entry's home guard, and a resize holds every home guard before it
        // replaces (and retires) a table. Without this, every resize leaked
        // the old table's entries.
        let guard = pin();
        for i in 0..(self.capacity + NEIGHBORHOOD_SIZE) {
            let entry_ptr = self.buckets[i]
                .slot
                .load(Ordering::Relaxed, &guard)
                .as_raw();
            if !entry_ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(entry_ptr));
                }
            }
        }
    }
}
