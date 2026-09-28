//! Walking the map: a borrowed walk that keeps the table it started on and yields a key once
//! (it recognizes a key a displacement carried ahead of it), and the owned walk that consumes
//! the map.

extern crate alloc;

use super::table::{Entry, Table, Walk, Word};
use super::{HopscotchMap, NEIGHBORHOOD_SIZE};
use alloc::boxed::Box;
use core::hash::{BuildHasher, Hash};
use core::marker::PhantomData;
use core::sync::atomic::Ordering;
use kovan::pin;

// Construction: none of `iter`/`keys`/`values` hashes or clones a value (the walk that does
// lives in `Iterator for HopscotchIter` below). `K: Eq` is captured here as the walk's key
// comparison (how it recognizes a key it already met), so the `Iterator` impls keep the bounds
// they have always had, `K: Clone` and `V: Clone`.
impl<K: Eq + 'static, V: 'static, S> HopscotchMap<K, V, S> {
    /// Returns an iterator over the map entries.
    ///
    /// The iterator walks the table that is current when it is created, to
    /// its end, even when a resize replaces that table meanwhile (the walk's
    /// position means nothing in another table, whose layout differs). An
    /// entry present from the iterator's creation to its end is yielded
    /// exactly once: a resize neither skips nor repeats it, and a
    /// displacement, which moves an entry to a higher slot of its
    /// neighborhood, links it there before unlinking it from its old slot (so
    /// the walk meets it) and the walk recognizes a key it already met there
    /// (so it is not repeated). An insert publishes its entry's hop bit before
    /// it links the entry, so an entry the walk finds in a slot is one a
    /// lookup finds too. An entry inserted, removed or updated concurrently may
    /// or may not be reflected, and no key is yielded twice. The iterator's
    /// own loop (`fold`, and `for_each`, `count` and the other folds built on
    /// it) reads a group of slots before it yields their entries, so a write
    /// its closure makes is one of those concurrent writes too.
    pub fn iter(&self) -> HopscotchIter<'_, K, V, S> {
        let guard = pin();
        let table = self.table.load(Ordering::Acquire, &guard).as_raw();
        // SAFETY: loaded under `guard`, which the iterator keeps.
        let current = unsafe { &*table };
        let (slots, mask) = (current.buckets.len(), current.home_mask());
        HopscotchIter {
            table,
            slots,
            mask,
            bucket_idx: 0,
            seen: Seen::new(<K as PartialEq>::eq),
            guard,
            _map: PhantomData,
        }
    }

    /// Returns an iterator over the map keys.
    pub fn keys(&self) -> HopscotchKeys<'_, K, V, S> {
        HopscotchKeys { iter: self.iter() }
    }

    /// Returns an iterator over the map values (clones `V`).
    pub fn values(&self) -> HopscotchValues<'_, K, V, S> {
        HopscotchValues { iter: self.iter() }
    }
}

/// Iterator over HopscotchMap entries ([`HopscotchMap::iter`]).
pub struct HopscotchIter<'a, K: 'static, V: 'static, S> {
    /// The table the walk started on, loaded under `guard`.
    table: *const Table<K, V>,
    /// `table`'s slot count and home mask, read once: the walk never reloads them.
    slots: usize,
    mask: usize,
    bucket_idx: usize,
    seen: Seen<K, V>,
    guard: kovan::Guard,
    _map: PhantomData<&'a HopscotchMap<K, V, S>>,
}

/// Slots a walk's `fold` reads before it yields their entries, and a group whose entries `next`
/// prefetches when it enters it: one bit each of a `u64`.
const GROUP: usize = u64::BITS as usize;

/// What a walk remembers of the slots it met: how it recognizes a key it already met.
struct Seen<K, V> {
    /// The entry the walk read in each of its last `NEIGHBORHOOD_SIZE` slots (slot `i` at
    /// `i % NEIGHBORHOOD_SIZE`, null until the walk meets an entry there), loaded under the
    /// walk's guard.
    recent: [*const Entry<K, V>; NEIGHBORHOOD_SIZE],
    /// For each home the walk met an entry of (home `h` at `h % NEIGHBORHOOD_SIZE`): the home's
    /// low 32 bits over a 32-bit filter of the hashes of the home's entries the walk met, bit
    /// `hash >> 59` of each. A walk meets a key again only as an entry of the same hash, so the
    /// same home and the same bit: an entry whose bit is clear in its home's filter is one the
    /// walk has not met, known without a scan.
    homes: [u64; NEIGHBORHOOD_SIZE],
    /// `K`'s equality, taken where `iter` is built.
    same_key: fn(&K, &K) -> bool,
}

impl<K, V> Seen<K, V> {
    /// Nothing met yet.
    fn new(same_key: fn(&K, &K) -> bool) -> Self {
        Self {
            recent: [core::ptr::null(); NEIGHBORHOOD_SIZE],
            homes: [0; NEIGHBORHOOD_SIZE],
            same_key,
        }
    }

    /// Whether the walk yields `entry`, which it read at slot `idx` (the slots it read before
    /// are all lower) of a table whose home mask is `mask`: `false` for a key it already met.
    /// Every walk step, `next` and `fold` alike, is this call.
    #[inline(always)]
    fn first_meeting(&mut self, entry: &Entry<K, V>, idx: usize, mask: usize) -> bool {
        self.recent[idx % NEIGHBORHOOD_SIZE] = entry;
        let home = (entry.hash as usize) & mask;
        // The filter of `home` as the walk left it at its last entry of `home`, or an empty one
        // when the cell names another home. Every entry of `home` lies in its neighborhood,
        // `home..home + NEIGHBORHOOD_SIZE`, so no entry of another home of this cell lies
        // between two entries of `home` in slot order: the cell holds `home`'s filter from the
        // walk's first entry of `home` to its last. (A home equal to `home` in its low 32 bits
        // only is `NEIGHBORHOOD_SIZE` homes away or more: its bits can only add a scan.)
        let cell = &mut self.homes[home % NEIGHBORHOOD_SIZE];
        let bit = 1u32 << (entry.hash >> 59);
        let filter =
            core::hint::select_unpredictable((*cell >> 32) as u32 == home as u32, *cell as u32, 0);
        *cell = (u64::from(home as u32) << 32) | u64::from(filter | bit);
        filter & bit == 0 || !self.met_before(home, idx, entry)
    }

    /// Whether the walk already met the key of `entry`, read at slot `idx`, in a lower slot of
    /// the key's neighborhood (which starts at `home`). A move carries an entry to a higher slot
    /// of its neighborhood, so the walk can meet it a second time there (or a newer entry of its
    /// key, after an update or a re-insert). Every slot of that neighborhood below `idx` is still
    /// in `recent`: the neighborhood spans `NEIGHBORHOOD_SIZE` slots. A cell the walk last wrote
    /// at least `NEIGHBORHOOD_SIZE` slots back (a free slot leaves its cell as it was) holds an
    /// entry of a lower home, whose hash differs; the walk's guard keeps it allocated.
    #[cold]
    #[inline(never)]
    fn met_before(&self, home: usize, idx: usize, entry: &Entry<K, V>) -> bool {
        let lowest = home.max(idx.saturating_sub(NEIGHBORHOOD_SIZE - 1));
        (lowest..idx).any(|seen_idx| {
            let seen = self.recent[seen_idx % NEIGHBORHOOD_SIZE];
            // SAFETY: null for a slot where the walk met no entry yet, else loaded under the
            // walk's guard, which keeps it from being freed.
            unsafe { seen.as_ref() }.is_some_and(|seen| {
                seen.hash == entry.hash
                    && (core::ptr::eq(seen, entry) || (self.same_key)(&seen.key, &entry.key))
            })
        })
    }
}

impl<K, V, S> HopscotchIter<'_, K, V, S> {
    /// The next entry the walk yields: the step of every `next` of the map's walks.
    #[inline(always)]
    fn next_entry(&mut self) -> Option<&Entry<K, V>> {
        // SAFETY: owned by this iterator's `guard`, which was pinned before `table` was loaded
        // and dies with the iterator: a resize that retires the table cannot free it (or its
        // entries) while the guard is held.
        let table = unsafe { &*self.table };
        while self.bucket_idx < self.slots {
            let idx = self.bucket_idx;
            self.bucket_idx += 1;
            if idx.is_multiple_of(GROUP) {
                // The entries of the group of slots the walk enters reach the cache while it
                // walks them. Only a hint: the walk reads each slot again when it gets there.
                // Relaxed: no entry is read through these words.
                for bucket in &table.buckets[idx..self.slots.min(idx + GROUP)] {
                    table.prefetch_entry(bucket.load(Ordering::Relaxed, &self.guard).ptr());
                }
            }
            let entry = table
                .get_bucket(idx)
                .load(Ordering::Acquire, &self.guard)
                .entry();
            if let Some(entry) = entry
                && self.seen.first_meeting(entry, idx, self.mask)
            {
                return Some(entry);
            }
        }
        None
    }

    /// The rest of the walk, `f` on each entry it yields, in one loop whose position and
    /// accumulator are locals: the loop of every `fold` of the map's walks. It reads the slots
    /// a group of `GROUP` at a time, in order, and then yields the entries it read there: each
    /// entry's line, prefetched when its slot is read, reaches the cache while the rest of the
    /// group is read, and the group's free slots cost no branch. It reads each slot once, in
    /// order, as `next` does, so what it yields keeps the guarantees of [`HopscotchMap::iter`];
    /// it reads a slot up to `GROUP - 1` slots before it yields the slot's entry.
    #[inline(always)]
    fn fold_entries<B>(self, init: B, mut f: impl FnMut(B, &Entry<K, V>) -> B) -> B {
        let Self {
            table,
            slots,
            mask,
            bucket_idx,
            mut seen,
            guard,
            ..
        } = self;
        // SAFETY: as in `next_entry`: `guard` was pinned before `table` was loaded and lives to
        // the end of this call.
        let table = unsafe { &*table };
        let mut acc = init;
        let mut group = [core::ptr::null::<Entry<K, V>>(); GROUP];
        let mut first = bucket_idx;
        for buckets in table.buckets[bucket_idx..slots].chunks(GROUP) {
            let mut occupied = 0u64;
            for (offset, (bucket, cell)) in buckets.iter().zip(&mut group).enumerate() {
                let entry: *const Entry<K, V> = bucket.load(Ordering::Acquire, &guard).ptr();
                *cell = entry;
                occupied |= u64::from(!entry.is_null()) << offset;
                table.prefetch_entry(entry);
            }
            while occupied != 0 {
                let offset = occupied.trailing_zeros() as usize;
                occupied &= occupied - 1;
                // SAFETY: a slot's non-null word, loaded under `guard`, which keeps its entry from
                // being freed.
                let entry = unsafe { &*group[offset] };
                if seen.first_meeting(entry, first + offset, mask) {
                    acc = f(acc, entry);
                }
            }
            first += buckets.len();
        }
        acc
    }
}

impl<'a, K, V, S> Iterator for HopscotchIter<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
    }

    /// The rest of the walk in the walk's own loop (`for_each`, `count`, `sum` and the other
    /// folds of `Iterator` come here).
    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        self.fold_entries(init, |acc, entry| {
            f(acc, (entry.key.clone(), entry.value.clone()))
        })
    }
}

/// Iterator over HopscotchMap keys.
pub struct HopscotchKeys<'a, K: 'static, V: 'static, S> {
    iter: HopscotchIter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for HopscotchKeys<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

/// Iterator over HopscotchMap values (clones `V`).
pub struct HopscotchValues<'a, K: 'static, V: 'static, S> {
    iter: HopscotchIter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for HopscotchValues<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = V;

    #[inline]
    fn next(&mut self) -> Option<V> {
        self.iter.next().map(|(_, v)| v)
    }
}

/// Owned iterator yielding `(K, V)` by value - moves out of the entries, no
/// clone. Each drained slot is nulled so the table destructor stays a no-op.
pub struct HopscotchIntoIter<K: 'static, V: 'static> {
    table: *mut Table<K, V>,
    walk: Walk<K, V>,
    guard: kovan::Guard,
}

impl<K, V> Iterator for HopscotchIntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        let table = unsafe { &*self.table };
        while let Some((idx, entry)) = self.walk.next(table, &self.guard) {
            if !entry.is_null() {
                table.get_bucket(idx).store(Word::free(), Ordering::Relaxed);
                let k = unsafe { core::ptr::read(&(*entry).key) };
                let v = unsafe { core::ptr::read(&(*entry).value) };
                unsafe {
                    alloc::alloc::dealloc(
                        entry as *mut u8,
                        core::alloc::Layout::new::<Entry<K, V>>(),
                    );
                }
                return Some((k, v));
            }
        }
        None
    }
}

impl<K, V> Drop for HopscotchIntoIter<K, V> {
    fn drop(&mut self) {
        while self.next().is_some() {}
        // All slots nulled above; Table::drop frees only the bucket array.
        unsafe { drop(Box::from_raw(self.table)) };
    }
}

impl<K, V, S> IntoIterator for HopscotchMap<K, V, S>
where
    K: 'static,
    V: 'static,
{
    type Item = (K, V);
    type IntoIter = HopscotchIntoIter<K, V>;

    fn into_iter(self) -> HopscotchIntoIter<K, V> {
        let mut me = core::mem::ManuallyDrop::new(self);
        let guard = pin();
        let table = me.table.load(Ordering::Relaxed, &guard).as_raw();
        unsafe { core::ptr::drop_in_place(&mut me.hasher) };
        // SAFETY: the map is consumed, so the table is this iterator's alone.
        let walk = Walk::new(unsafe { &*table }, &guard);
        HopscotchIntoIter { table, walk, guard }
    }
}

// `K: Send, V: Send`: an entry an insert replaces is retired, and its destructor may run on
// another thread.
impl<K, V, S> core::iter::FromIterator<(K, V)> for HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
    S: BuildHasher + Default,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let map = Self::with_hasher(S::default());
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

// A concurrent walk yields owned clones (`K: Clone`, `V: Clone`) and recognizes a key it already
// met (`K: Eq`, taken by `iter`), so these bounds are unavoidable here, unlike std's
// unconstrained `IntoIterator for &HashMap`, which yields borrowed `(&K, &V)` and hashes nothing
// at this bound-checked level either.
impl<'a, K, V, S> IntoIterator for &'a HopscotchMap<K, V, S>
where
    K: Eq + Clone + 'static,
    V: Clone + 'static,
{
    type Item = (K, V);
    type IntoIter = HopscotchIter<'a, K, V, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
