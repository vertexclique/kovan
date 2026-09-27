//! Walking one bucket's chain: the writers' walk, which unlinks (snips) every deleted node it
//! passes and stops at the key's node, at the chain's end or at a frozen link, and the readers'
//! walk, which changes nothing.
//!
//! Every node a walk dereferences was loaded under the walk's guard from a link that named it
//! while it was reachable, which is what kovan's protection needs (a node is retired only after
//! it is unlinked, and never linked again): a link loaded unmarked belongs to a node not deleted
//! at that moment, so the node it names was reachable; a deleted node's `next` is not such a
//! link (its successor may have been unlinked and retired since the node was), so a walk steps
//! past a deleted node only after `still_links` found that the link it came through still
//! names the node unmarked, or after its own snip replaced the deleted node by its successor.
//! Modelled in `tla/chained/ChainedMap.tla` (actions F0 to F3, G0 to G3).

use super::node::{Node, is_frozen, is_marked, ptr, word};
use super::table::TableRef;
use super::{Backoff, HashMap};
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{Atomic, Guard, retire};

/// Where a writer's walk of a chain stopped.
pub(super) enum Found<'g, K: 'static, V: 'static> {
    /// The key's node, not deleted when `next`, its link word, was loaded; `prev` is the link
    /// the walk came through, which named the node unmarked.
    Hit {
        prev: &'g Atomic<Node<K, V>>,
        node: &'g Node<K, V>,
        next: *mut Node<K, V>,
    },
    /// The key is in no node of the chain: `tail`, the chain's last link, was null, unmarked
    /// and unfrozen when loaded.
    Miss { tail: &'g Atomic<Node<K, V>> },
    /// A link was frozen: a migration or a clear is replacing the table.
    Frozen,
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// The current table, protected by `guard`.
    #[inline(always)]
    pub(super) fn table_ref(&self, guard: &Guard) -> TableRef<K, V> {
        TableRef::from_raw(self.table.load(Ordering::Acquire, guard).as_raw())
    }

    /// The writers' walk of the chain of `hash` in the current table: snips every deleted node
    /// it passes (a snip whose CAS fails starts the walk over) and stops at the node of `key`,
    /// at the chain's end, or at a frozen link. Returns the table it walked.
    pub(super) fn find<'g, Q>(
        &self,
        hash: u64,
        key: &Q,
        guard: &'g Guard,
    ) -> (TableRef<K, V>, Found<'g, K, V>)
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut backoff = Backoff::new();
        'retry: loop {
            let table = self.table_ref(guard);
            let mut prev: &'g Atomic<Node<K, V>> = table.bucket(table.bucket_index(hash));
            // Acquire: pairs with the release that linked the node, so its fields are visible.
            let mut cur = prev.load(Ordering::Acquire, guard).as_raw();
            if is_frozen(cur) {
                return (table, Found::Frozen);
            }
            loop {
                if cur.is_null() {
                    return (table, Found::Miss { tail: prev });
                }
                // SAFETY: loaded under `guard` while reachable (module docs).
                let node: &'g Node<K, V> = unsafe { &*cur };
                let next = node.next.load(Ordering::Acquire, guard).as_raw();
                if is_frozen(next) {
                    return (table, Found::Frozen);
                }
                if is_marked(next) {
                    let succ = ptr(next);
                    // The snip. AcqRel: a walker that acquires the new word sees the successor's
                    // fields, which this thread acquired from the marked word.
                    if prev
                        .compare_exchange(
                            word(cur),
                            word(succ),
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                            guard,
                        )
                        .is_err()
                    {
                        backoff.spin();
                        continue 'retry;
                    }
                    // SAFETY: this CAS unlinked it, so no other thread retires it, and no link
                    // names it again; a walker that loaded it holds a guard that keeps it.
                    // Node is #[repr(C)] with its RetiredNode at offset 0.
                    unsafe { retire(cur) };
                    cur = succ;
                    continue;
                }
                if node.hash == hash && node.key.borrow() == key {
                    return (table, Found::Hit { prev, node, next });
                }
                prev = &node.next;
                cur = next;
            }
        }
    }

    /// After a delete whose own unlink failed: walk the chain of `hash` once more, snipping
    /// every deleted node on the way, so the node the caller deleted is unlinked (and retired,
    /// by whichever thread unlinked it) before the caller returns. The deleted node sits before
    /// the key's live node and before the chain's end, where the walk stops; a frozen link ends
    /// it, the frozen table owning its nodes.
    #[cold]
    pub(super) fn cleanup<Q>(&self, hash: u64, key: &Q, guard: &Guard)
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let _ = self.find(hash, key, guard);
    }

    /// The readers' walk: the node of `key`, not deleted when its `next` was loaded (the
    /// answer's linearization point), or `None` when the walk reached the chain's end.
    #[inline]
    pub(super) fn lookup<'g, Q>(&self, hash: u64, key: &Q, guard: &'g Guard) -> Option<&'g Node<K, V>>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        'restart: loop {
            let table = self.table_ref(guard);
            let mut prev: &'g Atomic<Node<K, V>> = table.bucket(table.bucket_index(hash));
            let mut cur = ptr(prev.load(Ordering::Acquire, guard).as_raw());
            loop {
                if cur.is_null() {
                    return None;
                }
                // SAFETY: loaded under `guard` while reachable (module docs).
                let node: &'g Node<K, V> = unsafe { &*cur };
                let next = node.next.load(Ordering::Acquire, guard).as_raw();
                if !is_marked(next) {
                    if node.hash == hash && node.key.borrow() == key {
                        return Some(node);
                    }
                    prev = &node.next;
                    cur = ptr(next);
                    continue;
                }
                if !still_links(prev, cur, guard) {
                    continue 'restart;
                }
                cur = ptr(next);
            }
        }
    }
}

/// Whether the link `prev` still names `cur` unmarked. It does only while `cur` is reachable,
/// so `cur` was reachable when the successor its deleted `next` names was loaded before this,
/// and that successor, reachable through it, was not retired then: stepping to it is safe.
#[inline]
pub(super) fn still_links<K, V>(prev: &Atomic<Node<K, V>>, cur: *mut Node<K, V>, guard: &Guard) -> bool {
    let w = prev.load(Ordering::Acquire, guard).as_raw();
    ptr(w) == cur && !is_marked(w)
}
