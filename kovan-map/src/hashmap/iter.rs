//! Walking the map: the borrowed walks over a table snapshot and the owned walk that consumes
//! the map.

extern crate alloc;

use super::node::{Node, is_tagged, untag};
use super::table::TableRef;
use super::{HashMap, MIN_CAPACITY};
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::pin;

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// Returns an iterator over the map entries.
    /// Yields (K, V) clones from a table snapshot taken at creation.
    pub fn iter(&self) -> Iter<'_, K, V, S> {
        let guard = pin();
        let table = TableRef::<K, V>::from_raw(self.table.load(Ordering::Acquire, &guard).as_raw());
        Iter {
            _map: self,
            table,
            bucket_idx: 0,
            current: core::ptr::null(),
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

/// Iterator over HashMap entries.
///
/// Field ordering matters for drop safety.
/// Rust drops struct fields in declaration order.
/// The `guard` must be dropped *after* `current`/`table` so that the epoch
/// pin covering the snapshot is not released before we're done with the raw
/// pointers.
pub struct Iter<'a, K: 'static, V: 'static, S> {
    _map: &'a HashMap<K, V, S>,
    table: TableRef<K, V>,
    bucket_idx: usize,
    current: *const Node<K, V>,
    guard: kovan::Guard,
}

impl<'a, K, V, S> Iterator for Iter<'a, K, V, S>
where
    K: Clone,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.current.is_null() {
                unsafe {
                    let node = &*self.current;
                    let next = node.next.load(Ordering::Acquire, &self.guard).as_raw();
                    // Advance current (the pointer may carry a deletion tag).
                    self.current = untag(next);
                    if is_tagged(next) {
                        // Logically deleted - do not yield.
                        continue;
                    }
                    return Some((node.key.clone(), node.value.clone()));
                }
            }

            // Move to next bucket
            let table = self.table;
            if self.bucket_idx >= table.capacity() {
                return None;
            }

            let bucket = table.bucket(self.bucket_idx);
            self.bucket_idx += 1;
            self.current = bucket.load(Ordering::Acquire, &self.guard).as_raw();
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

impl<'a, K, V, S> IntoIterator for &'a HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
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
/// the yielded values is needed.
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
                self.current = untag(next);
                if is_tagged(next) {
                    continue; // logically deleted, owned by kovan
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
            self.current = bucket.load(Ordering::Acquire, &self.guard).as_raw();
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
        // Suppress HashMap::drop (we own the table now); drop only the hasher -
        // the remaining fields are atomics / usize / ZST marker.
        unsafe { core::ptr::drop_in_place(&mut me.hasher) };
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
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
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
