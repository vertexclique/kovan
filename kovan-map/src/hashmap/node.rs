//! A chain node of the map and its Harris-style deletion tag: a node whose `next` pointer
//! carries the tag is logically deleted and owned (and retired) by the thread that tagged it.

use kovan::{Atomic, RetiredNode};

/// Node in the lock-free linked list.
#[repr(C)]
pub(super) struct Node<K, V> {
    pub(super) retired: RetiredNode,
    pub(super) hash: u64,
    pub(super) key: K,
    pub(super) value: V,
    pub(super) next: Atomic<Node<K, V>>,
}

// SAFETY (kovan retirement rule): a retired Node's destructor may run on
// any thread, and nodes (with K and V inside) move between threads - hence
// `K: Send, V: Send` for Send. Unlike exclusive-transfer containers,
// lookups DO produce `&K`/`&V` from a shared `&Node` (get() clones V
// through &V under concurrent readers), so Sync additionally requires
// `K: Sync, V: Sync` - the same bounds the map-level Sync impl below has
// always required for sharing the map.
unsafe impl<K: Send, V: Send> Send for Node<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Node<K, V> {}

// ---------------------------------------------------------------------------
// Harris-style logical deletion
// ---------------------------------------------------------------------------
//
// Removing (or replacing) a node first TAGS the victim's `next` pointer
// (low bit set) - the logical delete - and only then unlinks it from its
// predecessor. The thread whose tag-CAS succeeded exclusively owns the node
// and is the only one to `retire()` it. This closes two races a plain
// unlink-CAS protocol has:
//
//  * insert-after-removed-tail: a tail insert CASes `tail.next: null -> new`;
//    if the tail was concurrently unlinked and retired, the new node is
//    spliced onto dead memory - the insert is lost and the node leaks.
//    With tagging, the remover first turns the tail's `next` into
//    tagged-null, so the insert's CAS (expecting untagged null) fails.
//
//  * adjacent removes: removing B (A->B->C) and C (B->C->D) concurrently
//    can unlink C from the already-detached B while C is still reachable
//    through A, retiring a reachable node (use-after-free for later
//    readers). With tagging, C's remover owns C via the tag; walkers
//    observe `B.next` tagged and never operate relative to deleted nodes.
//
// Invariants:
//  * tags appear only on `Node.next` fields, never on bucket heads
//    (snipping stores the untagged successor);
//  * a node whose `next` is tagged has been retired by its tag owner -
//    `clear()`, the migration sweep, and `Table::drop` must skip it;
//  * every traversal untags before following a `next` pointer.

#[inline(always)]
pub(super) fn tagged<K, V>(p: *mut Node<K, V>) -> *mut Node<K, V> {
    (p as usize | 1) as *mut Node<K, V>
}

#[inline(always)]
pub(super) fn untag<K, V>(p: *mut Node<K, V>) -> *mut Node<K, V> {
    (p as usize & !1) as *mut Node<K, V>
}

#[inline(always)]
pub(super) fn is_tagged<K, V>(p: *const Node<K, V>) -> bool {
    (p as usize) & 1 != 0
}
