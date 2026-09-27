//! The hopscotch table: its buckets (each a slot and, for the bucket as a home, a control word
//! holding the home's hop bits, writer guard and move stamp), the entries they hold, and the
//! home guard a writer holds while it links, replaces, unlinks or moves an entry of a home. Only
//! a home guard's holder changes the home's hop bits and stamp, and every occupied slot of a home
//! is written only under that home's guard.

extern crate alloc;

#[cfg(test)]
use super::pause;
use super::{MIN_CAPACITY, NEIGHBORHOOD_SIZE};
use crate::sync::AtomicU64;
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::sync::atomic::Ordering;
use kovan::{Atomic, RetiredNode, Shared, pin};

// A bucket's control word describes the bucket as a home, in three fields:
// - the hop bits (`HOP_MASK`): bit `i` set means slot `home + i` holds an entry of this home. An
//   insert publishes its entry's bit before it links the entry (and takes it back when the link
//   loses the slot), a remove clears the bit after it unlinks the entry, and a move sets the new
//   slot's bit before it unlinks the old slot, so from its link to its removal an entry is always
//   in a slot its home's bits name, and no lookup finds an entry a later lookup could miss;
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
    /// The key-value slot at this position, as a [`Word`].
    slot: Atomic<Entry<K, V>>,
}

impl<K, V> Bucket<K, V> {
    /// This bucket's slot.
    #[inline(always)]
    pub(super) fn load<'g>(&self, order: Ordering, guard: &'g kovan::Guard) -> Word<'g, K, V> {
        Word(self.slot.load(order, guard))
    }

    /// This bucket's slot, read by the holder of the writer guard of the home whose hop bits
    /// name it: `guard` need not protect the entry, as only that holder unlinks or retires it.
    ///
    /// # Safety
    ///
    /// The caller holds the writer guard of a home of a live table whose hop bits name this
    /// slot, and uses the entry only while it holds that guard (or after unlinking it itself).
    #[inline(always)]
    pub(super) unsafe fn load_held<'g>(
        &self,
        order: Ordering,
        guard: &'g kovan::Guard,
    ) -> Word<'g, K, V> {
        // SAFETY: the caller's contract: no other thread retires the entry meanwhile.
        Word(unsafe { self.slot.load_unprotected(order, guard) })
    }

    /// Store `word` in this bucket's slot.
    #[inline(always)]
    pub(super) fn store(&self, word: Word<'_, K, V>, order: Ordering) {
        self.slot.store(word.0, order);
    }

    /// Store `new` in this bucket's slot if it holds `current`: whether it did.
    #[inline(always)]
    pub(super) fn replace(
        &self,
        current: Word<'_, K, V>,
        new: Word<'_, K, V>,
        success: Ordering,
        failure: Ordering,
        guard: &kovan::Guard,
    ) -> bool {
        self.slot
            .compare_exchange(current.0, new.0, success, failure, guard)
            .is_ok()
    }
}

/// Low bits of an entry's address that its alignment keeps zero, where a slot's word carries
/// the entry's tag.
const TAG_BITS: u32 = 4;

/// The tag bits of a slot's word.
const TAG_MASK: usize = (1 << TAG_BITS) - 1;

/// The tag of an entry of hash `hash`: the top bits of the hash. A home's entries share the low
/// bits (the home index), so the top bits are the ones that tell them apart.
#[inline(always)]
pub(super) fn tag(hash: u64) -> usize {
    (hash >> (u64::BITS - TAG_BITS)) as usize
}

/// A slot's word: null for a free slot, or the address of the entry linked there with the
/// entry's tag in the low `TAG_BITS` bits. Address and tag are one atomic word, so the tag a
/// scan reads is always the tag of the entry the word names, and a scan for a hash whose tag
/// differs skips the entry without reading it: exactly the entries the full hash comparison
/// would skip on those bits, so a lookup of another key costs no read of the entry's line.
pub(super) struct Word<'g, K, V>(Shared<'g, Entry<K, V>>);

impl<K, V> Clone for Word<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for Word<'_, K, V> {}

impl<K, V> PartialEq for Word<'_, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'g, K, V> Word<'g, K, V> {
    /// A free slot's word.
    #[inline(always)]
    pub(super) fn free() -> Self {
        // SAFETY: a null `Shared` points at nothing, so it is valid for any lifetime.
        Self(unsafe { Shared::from_raw(core::ptr::null_mut()) })
    }

    /// The word linking `entry`, whose ownership passes to the slot that takes the word (or
    /// back through [`Word::into_entry`] when none does).
    #[inline(always)]
    pub(super) fn of(entry: Box<Entry<K, V>>) -> Self {
        let tag = tag(entry.hash);
        let raw = Box::into_raw(entry);
        // SAFETY: the address of a live allocation with its alignment bits holding the tag. No
        // word is dereferenced through `Shared`: `ptr` takes the tag off first.
        Self(unsafe { Shared::from_raw(raw.map_addr(|addr| addr | tag)) })
    }

    /// Whether this is a free slot's word.
    #[inline(always)]
    pub(super) fn is_free(self) -> bool {
        self.0.is_null()
    }

    /// Whether the entry this word names may have a hash of tag `tag`: `false` when its tag
    /// differs (and for a free slot unless `tag` is 0).
    #[inline(always)]
    pub(super) fn has_tag(self, tag: usize) -> bool {
        self.0.as_raw().addr() & TAG_MASK == tag
    }

    /// The address of the entry this word names, null for a free slot.
    #[inline(always)]
    pub(super) fn ptr(self) -> *mut Entry<K, V> {
        self.0.as_raw().map_addr(|addr| addr & !TAG_MASK)
    }

    /// The entry this word names, `None` for a free slot.
    #[inline(always)]
    pub(super) fn entry(self) -> Option<&'g Entry<K, V>> {
        // SAFETY: the word was loaded under a guard that lives for `'g` (or names an entry its
        // holder owns), which keeps the entry from being freed.
        unsafe { self.ptr().as_ref() }
    }

    /// The entry of a word [`Word::of`] made that no slot took, back to its owner.
    ///
    /// # Safety
    ///
    /// No slot holds the word, and it came from [`Word::of`].
    #[inline(always)]
    pub(super) unsafe fn into_entry(self) -> Box<Entry<K, V>> {
        // SAFETY: the caller's contract: the allocation `Word::of` took, owned by no slot.
        unsafe { Box::from_raw(self.ptr()) }
    }
}

/// A held writer guard of one home bucket, with the home's control word as this holder last
/// wrote it. Dropping it releases the guard, publishing the hop bits it staged (an unlink's) in
/// the same store.
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

    /// Publish now, still holding the guard, the hop bit of the slot `offset` past the home,
    /// for an entry about to be linked there. Relaxed: the link's release CAS, sequenced after
    /// this store, carries it to every reader that finds the entry.
    #[inline(always)]
    pub(super) fn publish_linked(&mut self, offset: usize) {
        self.word |= hop_bit(offset);
        self.control.store(self.word, Ordering::Relaxed);
    }

    /// Take back the hop bit `publish_linked` published, when the link lost the slot. A reader
    /// that met the bit meanwhile found another home's entry (or none) in the slot.
    #[inline(always)]
    pub(super) fn retract_linked(&mut self, offset: usize) {
        self.word &= !hop_bit(offset);
        self.control.store(self.word, Ordering::Relaxed);
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

/// An entry in the hash table. Aligned to `1 << TAG_BITS` so a slot's word has room for its tag.
#[repr(C, align(16))]
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

const _: () = assert!(core::mem::align_of::<Entry<(), ()>>() == 1 << TAG_BITS);

/// How many slots ahead of the one it yields a [`Walk`] reads. The entries of consecutive slots
/// sit anywhere in memory, so a walk that reads each entry when it reaches its slot waits out a
/// cache miss per entry; reading the slot this far ahead and prefetching its entry overlaps those
/// misses with the work on the slots in between.
pub(super) const WALK_AHEAD: usize = 16;

const _: () = assert!(MIN_CAPACITY + NEIGHBORHOOD_SIZE >= WALK_AHEAD);

/// Ask the cache for the line at `ptr` ahead of a read of it.
#[inline(always)]
fn prefetch<T>(ptr: *const T) {
    // SAFETY: a prefetch is a hint: it reads nothing architecturally and never faults, whatever
    // the address. `sse` is part of the x86_64 baseline.
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(ptr.cast());
    }
    // SAFETY: as above, on the x86 targets built with `sse`.
    #[cfg(all(target_arch = "x86", target_feature = "sse"))]
    unsafe {
        core::arch::x86::_mm_prefetch::<{ core::arch::x86::_MM_HINT_T0 }>(ptr.cast());
    }
    // SAFETY: as above.
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) ptr,
            options(nostack, readonly, preserves_flags)
        );
    }
    #[cfg(not(any(
        target_arch = "x86_64",
        all(target_arch = "x86", target_feature = "sse"),
        target_arch = "aarch64"
    )))]
    let _ = ptr;
}

/// A walk over the slots of a table no writer changes (a resize's copy of a table whose writers
/// it holds, a table being dropped or drained), in increasing index order. It reads each slot
/// once, `WALK_AHEAD` slots before it yields it, and prefetches the entry the slot names: with
/// the slots unchanging, the same answers as reading each slot when it is yielded, only the
/// entries' lines arrive earlier. Every call passes the same table and the same guard, which
/// keeps the entries yielded from being freed.
pub(super) struct Walk<K, V> {
    /// The next slot to yield.
    next: usize,
    /// The entries read for the slots `next..next + WALK_AHEAD` (slot `i` at `i % WALK_AHEAD`),
    /// null for a free slot.
    ahead: [*mut Entry<K, V>; WALK_AHEAD],
}

impl<K, V> Walk<K, V> {
    /// A walk of `table` from its first slot.
    pub(super) fn new(table: &Table<K, V>, guard: &kovan::Guard) -> Self {
        let mut ahead = [core::ptr::null_mut(); WALK_AHEAD];
        for (idx, entry) in ahead.iter_mut().enumerate() {
            *entry = table.read_ahead(idx, guard);
        }
        Self { next: 0, ahead }
    }

    /// The next slot of `table` and the entry read there, null for a free slot.
    #[inline]
    pub(super) fn next(
        &mut self,
        table: &Table<K, V>,
        guard: &kovan::Guard,
    ) -> Option<(usize, *mut Entry<K, V>)> {
        let idx = self.next;
        let len = table.buckets.len();
        if idx == len {
            return None;
        }
        self.next = idx + 1;
        let cell = &mut self.ahead[idx % WALK_AHEAD];
        let entry = *cell;
        if idx + WALK_AHEAD < len {
            *cell = table.read_ahead(idx + WALK_AHEAD, guard);
        }
        Some((idx, entry))
    }
}

/// Claim the free slot of `bucket` for `entry`: `Ok` with the linked entry when the slot took it
/// (the table owns it now), `Err` with the entry back when another writer took the slot first.
pub(super) fn link<K, V>(
    bucket: &Bucket<K, V>,
    entry: Box<Entry<K, V>>,
    guard: &kovan::Guard,
) -> Result<*const Entry<K, V>, Box<Entry<K, V>>> {
    let word = Word::of(entry);
    // Release: a reader that acquires the slot sees the entry's fields. Relaxed on failure: the
    // value read is not used.
    if bucket.replace(
        Word::free(),
        word,
        Ordering::Release,
        Ordering::Relaxed,
        guard,
    ) {
        Ok(word.ptr())
    } else {
        // SAFETY: the CAS failed, so the word was never published: its entry is still the
        // allocation `Word::of` took from this call.
        Err(unsafe { word.into_entry() })
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

    /// The mask [`bucket_index`](Self::bucket_index) applies to a hash.
    #[inline(always)]
    pub(super) fn home_mask(&self) -> usize {
        self.mask
    }

    #[inline(always)]
    pub(super) fn get_bucket(&self, idx: usize) -> &Bucket<K, V> {
        // SAFETY: Internal indices are calculated via mask or bounded offset loops.
        // The buckets array has padding to handle overflow up to NEIGHBORHOOD_SIZE.
        unsafe { self.buckets.get_unchecked(idx) }
    }

    /// Slot `idx`'s entry for a [`Walk`], prefetched (a free slot prefetches the table itself,
    /// which keeps the prefetch free of a branch). Acquire: pairs with the release that linked
    /// the entry, so its fields are visible.
    #[inline(always)]
    fn read_ahead(&self, idx: usize, guard: &kovan::Guard) -> *mut Entry<K, V> {
        let entry = self.get_bucket(idx).load(Ordering::Acquire, guard).ptr();
        let line: *const u8 = if entry.is_null() {
            (self as *const Self).cast()
        } else {
            entry.cast_const().cast()
        };
        prefetch(line);
        entry
    }

    /// Whether slot `idx` looks free. Relaxed: only a hint for where to try, the claim's CAS
    /// decides.
    #[inline(always)]
    pub(super) fn looks_free(&self, idx: usize, guard: &kovan::Guard) -> bool {
        self.get_bucket(idx)
            .load(Ordering::Relaxed, guard)
            .is_free()
    }

    /// Take the writer guard of the home bucket `idx` without waiting: `None` when another
    /// writer holds it.
    #[inline]
    pub(super) fn home_guard(&self, idx: usize) -> Option<HomeGuard<'_>> {
        self.home_guard_unless(idx, |_| false).ok()
    }

    /// Take the writer guard of the home bucket `idx` without waiting, unless another writer
    /// holds it or `refuse` holds of the home's control word: `Err` with the word as read then.
    /// One read of the word serves `refuse`, the test of the guard and the compare-exchange that
    /// takes it, which sets the guard bit and nothing else, as a read-modify-write setting the
    /// bit would.
    #[inline(always)]
    pub(super) fn home_guard_unless(
        &self,
        idx: usize,
        refuse: impl Fn(u64) -> bool,
    ) -> Result<HomeGuard<'_>, u64> {
        let control = &self.get_bucket(idx).control;
        // A writer that finds the guard held leaves the word alone: its read shares the
        // holder's line, where a read-modify-write would take the line from the holder in the
        // middle of its write. Relaxed: the compare-exchange decides.
        let mut word = control.load(Ordering::Relaxed);
        loop {
            if word & GUARD != 0 || refuse(word) {
                return Err(word);
            }
            // Acquire: pairs with the previous holder's release, so every slot and hop bit it
            // wrote is visible to this holder. Relaxed on failure: the word read is only tested
            // again. The word changes only under the guard and at its release, so a failure
            // finds the guard held, or retries with the word a holder released.
            match control.compare_exchange_weak(
                word,
                word | GUARD,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(HomeGuard {
                        control,
                        idx,
                        word: word | GUARD,
                    });
                }
                Err(now) => word = now,
            }
        }
    }

    /// The entry of `key`, whose hash is `hash`, as `get` finds it, taking no guard: a scan of
    /// the slots its home's hop bits name, repeated while the home's move stamp shows an entry
    /// of the home moved during the scan.
    #[inline]
    pub(super) fn lookup<'g, Q>(
        &self,
        hash: u64,
        key: &Q,
        guard: &'g kovan::Guard,
    ) -> Option<&'g Entry<K, V>>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let home = self.bucket_index(hash);
        let control = &self.get_bucket(home).control;
        // Acquire: pairs with the release store that set each hop bit, so the entry linked
        // before it is visible to the scan.
        let mut word = control.load(Ordering::Acquire);
        loop {
            let hops = hop_bits(word);
            if hops == 0 {
                return None;
            }
            #[cfg(test)]
            pause::at(pause::Point::LookupReadHops);
            if let Some((_, found)) = self.find(home, hops, hash, key, guard) {
                return found.entry();
            }
            // A miss is final unless an entry of this home moved while the scan ran. A move
            // links the entry at its new slot, sets that slot's hop bit and advances the stamp,
            // and only then empties the old slot with a release store. A scan that found the
            // old slot empty (or reused) acquired that store, so this re-read sees the advanced
            // stamp and the new bit with it. Acquire: the rescan reads the slots the new bits
            // name.
            let again = control.load(Ordering::Acquire);
            if (again ^ word) & STAMP_MASK == 0 {
                return None;
            }
            word = again;
        }
    }

    /// The entry of `key` among the slots `hops` names past the home `home`, and its offset
    /// there, for a reader: every entry read is protected by `guard`.
    #[inline]
    pub(super) fn find<'g, Q>(
        &self,
        home: usize,
        hops: u32,
        hash: u64,
        key: &Q,
        guard: &'g kovan::Guard,
    ) -> Option<(usize, Word<'g, K, V>)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        // Acquire: pairs with the release that linked the entry, so its fields are visible.
        self.scan(home, hops, hash, key, |bucket| {
            bucket.load(Ordering::Acquire, guard)
        })
    }

    /// [`find`](Self::find) for the holder of the home's writer guard `held`, the one thread
    /// that unlinks or retires an entry of the home until it releases the guard: the entries
    /// read need no protection of `guard`.
    #[inline]
    pub(super) fn find_held<'g, Q>(
        &self,
        held: &HomeGuard<'_>,
        hash: u64,
        key: &Q,
        guard: &'g kovan::Guard,
    ) -> Option<(usize, Word<'g, K, V>)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        // Acquire: pairs with the release that linked the entry, so its fields are visible.
        // SAFETY: `held` is the guard of the home whose bits name every slot read, of this
        // table, which is live while its guard is held (a resize takes every guard first).
        self.scan(held.idx, held.hops(), hash, key, |bucket| unsafe {
            bucket.load_held(Ordering::Acquire, guard)
        })
    }

    /// The scan of [`find`](Self::find) and [`find_held`](Self::find_held), reading a slot with
    /// `load`. An entry whose tag differs from `hash`'s is skipped without being read.
    #[inline(always)]
    fn scan<'g, Q>(
        &self,
        home: usize,
        hops: u32,
        hash: u64,
        key: &Q,
        load: impl Fn(&Bucket<K, V>) -> Word<'g, K, V>,
    ) -> Option<(usize, Word<'g, K, V>)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let tag = tag(hash);
        let mut rest = hops;
        while rest != 0 {
            let offset = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            let word = load(self.get_bucket(home + offset));
            if word.has_tag(tag)
                && let Some(entry) = word.entry()
                && entry.hash == hash
                && entry.key.borrow() == key
            {
                return Some((offset, word));
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
        let mut walk = Walk::new(self, &guard);
        while let Some((_, entry_ptr)) = walk.next(self, &guard) {
            if !entry_ptr.is_null() {
                // SAFETY: the table owns the entries its slots hold (see above).
                unsafe {
                    drop(Box::from_raw(entry_ptr));
                }
            }
        }
    }
}
