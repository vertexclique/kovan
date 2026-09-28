//! A chain node and the link words that join nodes into a bucket's chain.
//!
//! A link word (a bucket head, or a node's `next`) is a node pointer with three flag bits:
//!
//! - `MARK`, on a node's `next` only: the node is deleted. A remove marks the word keeping the
//!   successor; a replace marks it naming the replacement node, which names the old successor.
//!   Either is one CAS and is the operation's linearization point. A marked word never changes
//!   again except to be frozen.
//! - `FROZEN`: a migration (or a clear) froze the link; no write lands on it again, and a writer
//!   that meets it waits for the new table.
//! - `HELD`, on an unmarked, unfrozen link: a conditional write (`remove_if`, `replace_if`,
//!   `compute`) holds the link while its closure runs, the link of the key's node, or the
//!   chain's last link for a `compute` of an absent key. Only the holder writes a held link:
//!   every other writer's CAS expects a word without the flag and fails (the writer walks again),
//!   and a migration waits for the release. Readers pass the flag. The holder's closure decides
//!   once on a node no other write can change meanwhile, and the holder's store (the mark, the
//!   replace, the link of a new node, or the word as it was) is its linearization point.
//!
//! A node is retired only by the thread whose CAS unlinked it (a snip, or the unlink a remove or
//! a replace does after its mark), so no node is retired while a table can reach it: a node
//! still in a chain when its table is frozen belongs to that table and is freed with it. A
//! walker steps past a marked node only after it has checked that the link it came through
//! still names the node unmarked (see `hashmap::walk`), because a deleted node's `next` is not a
//! link a remover ever writes again: the successor it names may have been retired since.

use kovan::{Atomic, RetiredNode, Shared};

/// Node in the lock-free linked list. `next` sits beside `hash`, so a walk that passes a node
/// reads one cache line of it.
#[repr(C)]
pub(super) struct Node<K, V> {
    pub(super) retired: RetiredNode,
    pub(super) hash: u64,
    pub(super) next: Atomic<Node<K, V>>,
    pub(super) key: K,
    pub(super) value: V,
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

/// The deleted flag of a node's `next`.
pub(super) const MARK: usize = 1;
/// The frozen flag of any link word.
pub(super) const FROZEN: usize = 2;
/// The held flag of an unmarked, unfrozen link word.
pub(super) const HELD: usize = 4;
const FLAGS: usize = MARK | FROZEN | HELD;

// The flags live in the low bits of a node pointer.
const _: () = assert!(core::mem::align_of::<RetiredNode>() > FLAGS);

/// The node a link word names.
#[inline(always)]
pub(super) fn ptr<K, V>(word: *mut Node<K, V>) -> *mut Node<K, V> {
    word.map_addr(|a| a & !FLAGS)
}

#[inline(always)]
pub(super) fn is_marked<K, V>(word: *mut Node<K, V>) -> bool {
    word.addr() & MARK != 0
}

#[inline(always)]
pub(super) fn is_frozen<K, V>(word: *mut Node<K, V>) -> bool {
    word.addr() & FROZEN != 0
}

#[inline(always)]
pub(super) fn is_held<K, V>(word: *mut Node<K, V>) -> bool {
    word.addr() & HELD != 0
}

/// `word` with `flags` set.
#[inline(always)]
pub(super) fn with<K, V>(word: *mut Node<K, V>, flags: usize) -> *mut Node<K, V> {
    word.map_addr(|a| a | flags)
}

/// A link word as kovan's `Shared`, for a store or a CAS.
#[inline(always)]
pub(super) fn word<'g, K, V>(word: *mut Node<K, V>) -> Shared<'g, Node<K, V>> {
    // SAFETY: the word is only stored or compared, never dereferenced through this `Shared`.
    unsafe { Shared::from_raw(word) }
}

impl<K, V> Node<K, V> {
    /// A new, unlinked node.
    #[inline]
    pub(super) fn new(hash: u64, key: K, value: V) -> Self {
        Self {
            retired: RetiredNode::new(),
            hash,
            next: Atomic::null(),
            key,
            value,
        }
    }
}
