//! Replacing the table: a resize copies it into a table of another capacity, a clear replaces it
//! by an empty one. Both take the single resizer latch, freeze every link of the old table in
//! chain order (copying, for a resize, each node that is not deleted), and publish the new table
//! with the exact count of what it holds.
//!
//! Freezing is what makes a write final: every write is one CAS on one link, and a frozen link
//! fails every CAS a writer makes (it expects the word without the flag). A write whose CAS
//! landed before the freeze reached its link is in the chain the copy reads; a write that meets
//! a frozen link waits for the new table and writes there. No write lands in a table after its
//! copy read the link, so no landed write is lost and none is retried: an insert that landed
//! never meets its own copy and never reports it as another caller's entry, and a remove never
//! removes a copy of what it already removed. Modelled in `proofs/tla/chained/ChainedMap.tla`
//! (actions Z0 to Z6), where 0.1.20's read-then-revalidate protocol (Mutation "revalidate")
//! breaks both.

use super::node::{FROZEN, Node, is_frozen, is_held, is_marked, ptr, with, word};
use super::table::TableRef;
use super::{HashMap, MIN_CAPACITY};
use crate::sync::spin_hint;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{Atomic, Guard, Shared, alloc_node, pin, retire_node};

extern crate alloc;

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
    S: BuildHasher,
{
    /// Spin until the resize or clear in flight has published its table (a writer that met a
    /// frozen link, its guard dropped).
    #[inline]
    pub(super) fn wait_for_resize(&self) {
        while self.resizing.load(Ordering::Acquire) {
            spin_hint();
        }
    }

    /// Resize the table to `new_capacity` buckets (the single resizer wins; a call that finds
    /// the latch taken returns at once).
    pub(super) fn try_resize(&self, new_capacity: usize) {
        if self
            .resizing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let new_capacity = new_capacity
            .next_power_of_two()
            .max(MIN_CAPACITY)
            .max(self.floor);
        let guard = pin();
        let old = self.table_ref(&guard);
        if old.capacity() != new_capacity {
            let new = TableRef::alloc(new_capacity);
            // A `K::clone` or `V::clone` that panics in the migration unwinds through this,
            // which undoes it (see `Undo`): the old table stays current and every writer goes
            // on, where the latch held for good would have stopped them all.
            let undo = Undo {
                old,
                new,
                guard: &guard,
                latch: &self.resizing,
            };
            let copied = Self::migrate(old, Some(new), &guard);
            core::mem::forget(undo);
            // Relaxed: the table's publication below releases it.
            new.count().store(copied as isize, Ordering::Relaxed);
            self.publish(old, new);
        }
        drop(guard);
        self.resizing.store(false, Ordering::Release);
    }

    /// Clears the map, removing all key-value pairs.
    ///
    /// Linearizes when the empty table is published: a write that landed before its link was
    /// frozen is cleared, a write that met a frozen link lands in the empty table after.
    pub fn clear(&self) {
        while self
            .resizing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spin_hint();
        }
        let guard = pin();
        let old = self.table_ref(&guard);
        Self::migrate(old, None, &guard);
        self.publish(old, TableRef::alloc(old.capacity()));
        drop(guard);
        self.resizing.store(false, Ordering::Release);
    }

    /// Freeze `link` and return its frozen word. A held link is waited for: only its holder
    /// writes it, and the conditional write holding it lands in this table before the link
    /// freezes (so the copy reads it).
    fn freeze(link: &Atomic<Node<K, V>>, guard: &Guard) -> *mut Node<K, V> {
        loop {
            // A word a failed CAS returns is reloaded here rather than used: only a load
            // protects the node it names.
            let w = link.load(Ordering::Acquire, guard).as_raw();
            if is_frozen(w) {
                return w;
            }
            if is_held(w) {
                spin_hint();
                continue;
            }
            let frozen = with(w, FROZEN);
            // AcqRel: the copy below reads the fields of the node the word names.
            if link
                .compare_exchange(
                    word(w),
                    word(frozen),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                return frozen;
            }
        }
    }

    /// Freeze every link of `old`, bucket by bucket in chain order, and copy each node that is
    /// not deleted into `into` (a table no other thread sees yet). Returns the number copied.
    fn migrate(old: TableRef<K, V>, into: Option<TableRef<K, V>>, guard: &Guard) -> usize {
        let mut copied = 0;
        for b in 0..old.capacity() {
            let mut w = Self::freeze(old.bucket(b), guard);
            loop {
                let cur = ptr(w);
                if cur.is_null() {
                    break;
                }
                // SAFETY: the frozen link that names it named it when this thread froze it, and
                // a frozen link never changes, so it stays reachable (never retired) from here.
                let node = unsafe { &*cur };
                w = Self::freeze(&node.next, guard);
                if is_marked(w) {
                    continue;
                }
                if let Some(new) = into {
                    let dst = new.bucket(new.bucket_index(node.hash));
                    let copy = Node::new(node.hash, node.key.clone(), node.value.clone());
                    // Relaxed: `new` is private to this thread until its publication releases it.
                    copy.next
                        .store(dst.load(Ordering::Relaxed, guard), Ordering::Relaxed);
                    dst.store(word(alloc_node(copy)), Ordering::Relaxed);
                    copied += 1;
                }
            }
        }
        copied
    }

    /// Make `new` the current table and retire `old`, every link of which is frozen.
    fn publish(&self, old: TableRef<K, V>, new: TableRef<K, V>) {
        // Release: a thread that acquires the new table sees its buckets, nodes and count.
        // SAFETY: `new` is a live table allocation.
        self.table
            .store(unsafe { Shared::from_raw(new.as_raw()) }, Ordering::Release);
        // SAFETY: the old table is unreachable from the map now; kovan frees it (and the nodes
        // its frozen chains hold) once no guard that loaded it remains. TableProxy is
        // #[repr(C)] with its RetiredNode at offset 0.
        unsafe { retire_node(old.take_proxy()) };
    }
}

/// Undoes a migration that unwinds (a clone of a key or value panicked in it): the links it
/// froze in the old table thawed, the new table (no other thread ever saw it) freed with the
/// copies made so far, and the latch let go, so the map goes on with its old table.
struct Undo<'a, K: 'static, V: 'static> {
    old: TableRef<K, V>,
    new: TableRef<K, V>,
    guard: &'a Guard,
    latch: &'a crate::sync::AtomicBool,
}

impl<K: 'static, V: 'static> Drop for Undo<'_, K, V> {
    fn drop(&mut self) {
        thaw_links(self.old, self.guard);
        // SAFETY: never published, so this thread alone reaches it and its copies.
        unsafe { self.new.free() };
        self.latch.store(false, Ordering::Release);
    }
}

/// Clears the frozen flag of every link a migration froze in `table`: what a migration that
/// cannot finish leaves behind. A migration freezes each chain from its head in order, so
/// the frozen links of a chain are a prefix of it; while the latch is held no other thread
/// changes a frozen link (a writer that meets one waits for the latch).
fn thaw_links<K: 'static, V: 'static>(table: TableRef<K, V>, guard: &Guard) {
    for b in 0..table.capacity() {
        let mut link = table.bucket(b);
        loop {
            let w = link.load(Ordering::Acquire, guard).as_raw();
            if !is_frozen(w) {
                break;
            }
            let thawed = w.map_addr(|a| a & !FROZEN);
            // AcqRel: a writer that loads the thawed word sees the chain as it was.
            let _ = link.compare_exchange(
                word(w),
                word(thawed),
                Ordering::AcqRel,
                Ordering::Relaxed,
                guard,
            );
            let cur = ptr(w);
            if cur.is_null() {
                break;
            }
            // SAFETY: a frozen link that names it named it while this thread froze it, so it
            // is reachable and never retired while the guard is held.
            link = unsafe { &(*cur).next };
        }
    }
}
