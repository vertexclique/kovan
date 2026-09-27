//! Walking the map: the borrowed walks over a table snapshot and the owned walk that consumes
//! the map.
//!
//! A borrowed walk keeps the table that was current when it was created, to its end: a resize
//! freezes that table (every link final), so it keeps the entries it held then. It takes each
//! bucket in one pass, collecting the nodes that are not deleted, before it yields any of them
//! (a pass that fails a validation starts that bucket over from its head, with nothing yielded
//! from it yet), so an entry present for the whole walk is yielded exactly once: a single pass
//! meets each live node once, a replace puts the new node right after the old one (which the
//! pass either met live, or passes deleted), and a pass never goes back. A key removed and
//! inserted again during the walk can be yielded once for each of its lives. Modelled in
//! `tla/chained/ChainedMap.tla` (actions T0 to T9).

extern crate alloc;

use super::node::{Node, is_marked, ptr};
use super::table::TableRef;
use super::walk::still_links;
use super::{HashMap, MIN_CAPACITY};
use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{Atomic, pin};

// Construction only: none of `iter`/`keys`/`values` hashes or clones a value (the walk that
// does lives in `Iterator for Iter` below), so this block needs only the struct's own `'static`
// bound, as std's `HashMap::iter`/`keys`/`values` need none of `Hash`, `Eq`, `Clone` or
// `BuildHasher` either.
impl<K: 'static, V: 'static, S> HashMap<K, V, S> {
    /// Returns an iterator over the map entries, `(K, V)` clones.
    ///
    /// The iterator walks the table that is current when it is created, to its end, even when a
    /// resize replaces it meanwhile. An entry present from the iterator's creation to its end is
    /// yielded exactly once, with a value it held during the walk; an entry inserted, removed or
    /// updated concurrently may or may not be reflected, and a key removed and inserted again
    /// meanwhile may be yielded once per life. The iterator holds one kovan guard for its
    /// lifetime (nodes it passed are not freed while it lives).
    pub fn iter(&self) -> Iter<'_, K, V, S> {
        let guard = pin();
        let table = self.table_ref(&guard);
        Iter {
            _map: self,
            table,
            bucket_idx: 0,
            batch: Batch::new(),
            pos: 0,
            guard,
        }
    }

    /// Returns an iterator over the map keys.
    /// Yields K clones.
    pub fn keys(&self) -> Keys<'_, K, V, S> {
        Keys { iter: self.iter() }
    }

    /// Returns an iterator over the map values (clones `V`).
    pub fn values(&self) -> Values<'_, K, V, S> {
        Values { iter: self.iter() }
    }
}

/// How many of a bucket's nodes a walk keeps without allocating. A bucket holds under one node
/// on average (the map grows past three quarters of its buckets), so a longer chain is rare;
/// one past this spills into a vector the walk reuses for every later bucket.
const BATCH_INLINE: usize = 8;

/// The nodes one bucket pass collected, in chain order.
struct Batch<K, V> {
    inline: [*const Node<K, V>; BATCH_INLINE],
    spill: Vec<*const Node<K, V>>,
    len: usize,
}

impl<K, V> Batch<K, V> {
    fn new() -> Self {
        Self {
            inline: [core::ptr::null(); BATCH_INLINE],
            spill: Vec::new(),
            len: 0,
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.len = 0;
        self.spill.clear();
    }

    #[inline]
    fn push(&mut self, node: *const Node<K, V>) {
        if self.len < BATCH_INLINE {
            self.inline[self.len] = node;
        } else {
            self.spill.push(node);
        }
        self.len += 1;
    }

    #[inline]
    fn get(&self, i: usize) -> Option<*const Node<K, V>> {
        match i {
            _ if i >= self.len => None,
            _ if i < BATCH_INLINE => Some(self.inline[i]),
            _ => Some(self.spill[i - BATCH_INLINE]),
        }
    }
}

/// Iterator over HashMap entries ([`HashMap::iter`]).
///
/// Field ordering matters for drop safety.
/// Rust drops struct fields in declaration order.
/// The `guard` must be dropped *after* `batch`/`table` so that the epoch
/// pin covering the snapshot is not released before we're done with the raw
/// pointers.
pub struct Iter<'a, K: 'static, V: 'static, S> {
    _map: &'a HashMap<K, V, S>,
    table: TableRef<K, V>,
    bucket_idx: usize,
    batch: Batch<K, V>,
    pos: usize,
    guard: kovan::Guard,
}

impl<K, V, S> Iter<'_, K, V, S> {
    /// Collect bucket `b`'s nodes that are not deleted, in one validated pass.
    fn collect(&mut self, b: usize) {
        let guard = &self.guard;
        'bucket: loop {
            self.batch.clear();
            let mut prev: &Atomic<Node<K, V>> = self.table.bucket(b);
            let mut cur = ptr(prev.load(Ordering::Acquire, guard).as_raw());
            while !cur.is_null() {
                // SAFETY: loaded under the iterator's guard while reachable (see
                // `hashmap::walk`); the guard lives as long as the iterator.
                let node = unsafe { &*cur };
                let next = node.next.load(Ordering::Acquire, guard).as_raw();
                if !is_marked(next) {
                    self.batch.push(cur);
                    prev = &node.next;
                } else if !still_links(prev, cur, guard) {
                    continue 'bucket;
                }
                cur = ptr(next);
            }
            self.pos = 0;
            return;
        }
    }
}

impl<'a, K, V, S> Iterator for Iter<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(node) = self.batch.get(self.pos) {
                self.pos += 1;
                // SAFETY: collected under the iterator's guard, which keeps it from being freed.
                let node = unsafe { &*node };
                return Some((node.key.clone(), node.value.clone()));
            }
            if self.bucket_idx >= self.table.capacity() {
                return None;
            }
            let b = self.bucket_idx;
            self.bucket_idx += 1;
            self.collect(b);
        }
    }
}

/// Iterator over HashMap keys.
pub struct Keys<'a, K: 'static, V: 'static, S> {
    iter: Iter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for Keys<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

// Bounded by exactly what `Iterator for Iter` needs: a concurrent walk yields owned clones, so
// `K: Clone` and `V: Clone` are unavoidable here, unlike std's unconstrained `IntoIterator for
// &HashMap`, which yields borrowed `(&K, &V)` and hashes nothing at this bound-checked level
// either.
impl<'a, K, V, S> IntoIterator for &'a HashMap<K, V, S>
where
    K: Clone + 'static,
    V: Clone + 'static,
{
    type Item = (K, V);
    type IntoIter = Iter<'a, K, V, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over HashMap values (clones `V`).
pub struct Values<'a, K: 'static, V: 'static, S> {
    iter: Iter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for Values<'a, K, V, S>
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

/// Owned iterator yielding `(K, V)` by value - moves out of the nodes, no
/// clone. Consuming the map gives exclusive access, so no guard protection of
/// the yielded values is needed. A deleted node still in a chain (never
/// unlinked, so never retired) is freed here with its key and value.
pub struct IntoIter<K: 'static, V: 'static> {
    table: TableRef<K, V>,
    bucket_idx: usize,
    current: *mut Node<K, V>,
    guard: kovan::Guard,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        loop {
            if !self.current.is_null() {
                let node = self.current;
                let next = unsafe { (*node).next.load(Ordering::Acquire, &self.guard).as_raw() };
                self.current = ptr(next);
                if is_marked(next) {
                    // SAFETY: exclusive access; the table owns a deleted node still in a chain.
                    unsafe { drop(alloc::boxed::Box::from_raw(node)) };
                    continue;
                }
                // Move K and V out, then free the shell without running drop.
                let k = unsafe { core::ptr::read(&(*node).key) };
                let v = unsafe { core::ptr::read(&(*node).value) };
                unsafe {
                    alloc::alloc::dealloc(
                        node as *mut u8,
                        core::alloc::Layout::new::<Node<K, V>>(),
                    );
                }
                return Some((k, v));
            }
            if self.bucket_idx >= self.table.capacity() {
                return None;
            }
            let bucket = self.table.bucket(self.bucket_idx);
            self.bucket_idx += 1;
            self.current = ptr(bucket.load(Ordering::Acquire, &self.guard).as_raw());
        }
    }
}

impl<K, V> Drop for IntoIter<K, V> {
    fn drop(&mut self) {
        while self.next().is_some() {} // drop remaining live K/V + free shells
        unsafe { self.table.free_array_only() };
    }
}

impl<K, V, S> IntoIterator for HashMap<K, V, S>
where
    K: 'static,
    V: 'static,
{
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        let mut me = core::mem::ManuallyDrop::new(self);
        let guard = pin();
        let table = TableRef::<K, V>::from_raw(me.table.load(Ordering::Relaxed, &guard).as_raw());
        // Suppress HashMap::drop (we own the table now); drop the hasher and the
        // latch (a no-op but under shuttle) - the rest is a pointer, a usize and a marker.
        unsafe {
            core::ptr::drop_in_place(&mut me.hasher);
            core::ptr::drop_in_place(&mut me.resizing);
        }
        IntoIter {
            table,
            bucket_idx: 0,
            current: core::ptr::null_mut(),
            guard,
        }
    }
}

impl<K, V, S> core::iter::FromIterator<(K, V)> for HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher + Default,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let map = Self::with_capacity_and_hasher(MIN_CAPACITY, S::default());
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}
