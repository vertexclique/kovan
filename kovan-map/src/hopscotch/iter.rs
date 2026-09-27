//! Walking the map: a borrowed walk that keeps the table it started on and yields a key once
//! (it recognizes a key a displacement carried ahead of it), and the owned walk that consumes
//! the map.

extern crate alloc;

use super::table::{Entry, Table};
use super::{HopscotchMap, NEIGHBORHOOD_SIZE};
use alloc::boxed::Box;
use core::hash::{BuildHasher, Hash};
use core::marker::PhantomData;
use core::sync::atomic::Ordering;
use kovan::{Shared, pin};

impl<K, V, S> HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
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
    /// (so it is not repeated). An entry inserted, removed or updated
    /// concurrently may or may not be reflected, and no key is yielded twice.
    pub fn iter(&self) -> HopscotchIter<'_, K, V, S> {
        let guard = pin();
        let table = self.table.load(Ordering::Acquire, &guard).as_raw();
        HopscotchIter {
            table,
            bucket_idx: 0,
            recent: [core::ptr::null(); NEIGHBORHOOD_SIZE],
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
    bucket_idx: usize,
    /// The entry the walk read in each of its last `NEIGHBORHOOD_SIZE` slots (slot `i` at
    /// `i % NEIGHBORHOOD_SIZE`, null for a free slot), loaded under `guard`: how the walk
    /// recognizes a key it already met.
    recent: [*const Entry<K, V>; NEIGHBORHOOD_SIZE],
    guard: kovan::Guard,
    _map: PhantomData<&'a HopscotchMap<K, V, S>>,
}

impl<K: Eq, V, S> HopscotchIter<'_, K, V, S> {
    /// Whether the walk already met the key of `entry`, read at slot `idx`, in a lower slot of
    /// the key's neighborhood (which starts at `home`). A move carries an entry to a higher slot
    /// of its neighborhood, so the walk can meet it a second time there (or a newer entry of its
    /// key, after an update or a re-insert). Every slot of that neighborhood below `idx` is still
    /// in `recent`: the neighborhood spans `NEIGHBORHOOD_SIZE` slots.
    fn met_before(&self, home: usize, idx: usize, entry: &Entry<K, V>) -> bool {
        let lowest = home.max(idx.saturating_sub(NEIGHBORHOOD_SIZE - 1));
        (lowest..idx).any(|seen_idx| {
            let seen = self.recent[seen_idx % NEIGHBORHOOD_SIZE];
            // SAFETY: null for a free slot, else loaded under `self.guard`, which keeps it from
            // being freed.
            unsafe { seen.as_ref() }.is_some_and(|seen| {
                seen.hash == entry.hash && (core::ptr::eq(seen, entry) || seen.key == entry.key)
            })
        })
    }
}

impl<'a, K, V, S> Iterator for HopscotchIter<'a, K, V, S>
where
    K: Eq + Clone,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: owned by this iterator's `guard`, which was pinned before
        // `table` was loaded and dies with the iterator: a resize that
        // retires the table cannot free it (or its entries) while the guard
        // is held.
        let table = unsafe { &*self.table };

        while self.bucket_idx < table.buckets.len() {
            let idx = self.bucket_idx;
            self.bucket_idx += 1;

            let slot = &table.get_bucket(idx).slot;
            let entry_ptr: *const Entry<K, V> = slot.load(Ordering::Acquire, &self.guard).as_raw();
            self.recent[idx % NEIGHBORHOOD_SIZE] = entry_ptr;
            if entry_ptr.is_null() {
                continue;
            }
            // SAFETY: loaded under `self.guard`, which keeps it from being freed.
            let entry = unsafe { &*entry_ptr };
            if !self.met_before(table.bucket_index(entry.hash), idx, entry) {
                return Some((entry.key.clone(), entry.value.clone()));
            }
        }
        None
    }
}

/// Iterator over HopscotchMap keys.
pub struct HopscotchKeys<'a, K: 'static, V: 'static, S> {
    iter: HopscotchIter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for HopscotchKeys<'a, K, V, S>
where
    K: Eq + Clone,
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
    K: Eq + Clone,
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
    bucket_idx: usize,
    guard: kovan::Guard,
}

impl<K, V> Iterator for HopscotchIntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        let table = unsafe { &*self.table };
        while self.bucket_idx < table.buckets.len() {
            let bucket = table.get_bucket(self.bucket_idx);
            self.bucket_idx += 1;
            let entry = bucket.slot.load(Ordering::Acquire, &self.guard).as_raw();
            if !entry.is_null() {
                bucket.slot.store(
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::Relaxed,
                );
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
        HopscotchIntoIter {
            table,
            bucket_idx: 0,
            guard,
        }
    }
}

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

impl<'a, K, V, S> IntoIterator for &'a HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    type Item = (K, V);
    type IntoIter = HopscotchIter<'a, K, V, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
