//! High-Performance Lock-Free Concurrent Hash Map (FoldHash + resizable table).
//!
//! # Strategy
//!
//! 1. **FoldHash**: `foldhash::fast::FixedState` for fast, quality hashing.
//! 2. **Resizable bucket table**: the bucket array lives in a single
//!    allocation (header + inline buckets) swapped atomically on resize and
//!    reclaimed through kovan. The map grows when the load factor exceeds
//!    3/4 and shrinks below 1/4 (never under its initial capacity), the
//!    same thresholds as `HopscotchMap`.
//! 3. **Node layout**: `hash` and `next` side by side, so a walk passing a
//!    node reads one cache line of it.
//!
//! # Architecture
//! - **Table**: kovan-retired object holding the bucket array (atomic head
//!   pointers) and its entry count. Readers snapshot the table under a guard
//!   and never block.
//! - **Chains**: singly linked, lock-free. Every change of the map's content
//!   is one CAS on one link word (`node`): an insert links a new node at the
//!   tail, a remove marks the node's own `next`, a replace marks it naming
//!   the new node, which names the old successor. That CAS is the
//!   operation's linearization point, and the answer the operation gives is
//!   exact: `insert_if_absent` answers `None` exactly when its CAS linked
//!   the key's node, and otherwise the value of the node it found live.
//! - **Reclamation**: a node is retired only by the thread whose CAS unlinked
//!   it, never while a table can reach it, and a walk steps past a deleted
//!   node only after checking the link it came through still names it
//!   (`walk`), so every node a walk reads is protected by its guard.
//! - **Resize**: a single resizer (latch `resizing`) freezes every link of
//!   the old table in chain order while it copies the live nodes, then
//!   publishes the new table and retires the old one (`resize`). A write
//!   whose CAS landed is final; a write that meets a frozen link waits for
//!   the new table and writes there.
//!
//! The protocol is modelled in `tla/chained/ChainedMap.tla` and checked by TLC
//! with every rule 0.1.20 had put back one at a time (`tla/README.md`).

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

use crate::sync::AtomicBool;
use alloc::boxed::Box;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use foldhash::fast::FixedState;
use kovan::{Atomic, Guard, pin, retire};
use node::{MARK, Node, with, word};
use table::{TableHeader, TableRef};
use walk::Found;

pub use iter::{IntoIter, Iter, Keys, Values};

mod iter;
mod node;
mod resize;
mod std_traits;
mod table;
mod walk;

/// Default number of buckets for `new()`. Matches the previous fixed-table
/// sizing (zero collisions for ~100k items, fits in L3); maps created with
/// `new()` keep exactly the old memory/performance profile and additionally
/// grow past it on demand. Use [`HashMap::with_capacity`] for small elastic
/// maps.
const DEFAULT_CAPACITY: usize = 524_288;

/// Minimum number of buckets; the map never shrinks below this.
const MIN_CAPACITY: usize = 64;

// Load-factor thresholds (implemented as integer comparisons on the hot
// paths): grow when count/capacity > 3/4, shrink when count/capacity < 1/4
// (never below the map's floor capacity). Same thresholds as HopscotchMap.

/// A simple exponential backoff for reducing contention.
struct Backoff {
    step: u32,
}

impl Backoff {
    #[inline(always)]
    fn new() -> Self {
        Self { step: 0 }
    }

    #[inline(always)]
    fn spin(&mut self) {
        for _ in 0..(1 << self.step.min(6)) {
            core::hint::spin_loop();
        }
        if self.step <= 6 {
            self.step += 1;
        }
    }
}

/// High-Performance Lock-Free Map with automatic grow/shrink.
pub struct HashMap<K: 'static, V: 'static, S = FixedState> {
    table: Atomic<TableHeader>,
    /// Single-resizer latch, held by a resize or a clear from before it
    /// freezes the first link until after it publishes the new table.
    resizing: AtomicBool,
    /// Shrink floor: the initial capacity. The map never shrinks below the
    /// size it was created with, preserving the caller's sizing intent (and
    /// the historical fixed-table behavior for `new()`).
    floor: usize,
    hasher: S,
    _marker: core::marker::PhantomData<(K, V)>,
}

/// The node an insert links: its key and value until an attempt needs the allocation, then that
/// allocation, carried across the attempts that fail to link it, so a retry neither clones the
/// key and value nor allocates again, and a losing call drops them exactly once.
enum Pending<K, V> {
    Parts { key: K, value: V },
    Built(Box<Node<K, V>>),
}

impl<K, V> Pending<K, V> {
    #[inline(always)]
    fn key(&self) -> &K {
        match self {
            Self::Parts { key, .. } => key,
            Self::Built(node) => &node.key,
        }
    }

    /// The node, allocated the first time an attempt needs it, its `next` set to `next`.
    #[inline]
    fn into_node(self, hash: u64, next: *mut Node<K, V>) -> *mut Node<K, V> {
        let node = match self {
            Self::Parts { key, value } => Box::new(Node::new(hash, key, value)),
            Self::Built(node) => node,
        };
        // Relaxed: the node is private until the CAS that links it releases it.
        node.next.store(word(next), Ordering::Relaxed);
        Box::into_raw(node)
    }

    /// The node an attempt failed to link, back for the next attempt.
    #[inline]
    fn back(node: *mut Node<K, V>) -> Self {
        // SAFETY: the CAS that would have published `node` failed, so it is still the
        // allocation `into_node` gave this call.
        Self::Built(unsafe { Box::from_raw(node) })
    }
}

/// How a conditional insert ended.
enum Claim<R, V> {
    /// This call linked the key's node; what the caller asked of the value it linked.
    Inserted(R),
    /// The key was present; its value.
    Present(V),
}

// Small accessors that never hash: only the struct's own `'static` bound, as std's equivalent
// block for `with_hasher`/`with_capacity_and_hasher`/`capacity`/`len`/`is_empty`/`hasher` needs
// no `Hash`, `Eq`, `Clone` or `BuildHasher`. A method that hashes or clones a value lives in the
// bound impl block below instead.
impl<K: 'static, V: 'static, S> HashMap<K, V, S> {
    /// Creates a new hash map with custom hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(DEFAULT_CAPACITY, hasher)
    }

    /// Creates a new hash map with at least `capacity` buckets and a custom hasher.
    ///
    /// The map grows when its load factor exceeds 0.75 and shrinks when it
    /// falls below 0.25 - but never below `capacity`.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let table = TableRef::<K, V>::alloc(capacity);
        let floor = table.capacity();
        Self {
            table: Atomic::new(table.as_raw()),
            resizing: AtomicBool::new(false),
            floor,
            hasher,
            _marker: core::marker::PhantomData,
        }
    }

    /// Returns the current number of buckets.
    pub fn capacity(&self) -> usize {
        let guard = pin();
        self.table_ref(&guard).capacity()
    }

    /// Returns true if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of elements in the map.
    ///
    /// O(1): the current table's count, kept by insert/remove and set exactly
    /// by a resize. Approximate while concurrent updates are in flight (a
    /// write counts itself right after its CAS), exact in quiescence.
    pub fn len(&self) -> usize {
        let guard = pin();
        self.table_ref(&guard)
            .count()
            .load(Ordering::Relaxed)
            .max(0) as usize
    }

    /// Get the underlying hasher itself.
    pub fn hasher(&self) -> &S {
        &self.hasher
    }
}

// Construction with the built-in hasher never hashes either: the struct's own `'static` only,
// as std's `HashMap::new`/`with_capacity` carry no bound.
#[cfg(feature = "std")]
impl<K: 'static, V: 'static> HashMap<K, V, FixedState> {
    /// Creates a new empty hash map with FoldHash (FixedState).
    pub fn new() -> Self {
        Self::with_hasher(FixedState::default())
    }

    /// Creates a new empty hash map with at least `capacity` buckets.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, FixedState::default())
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// Returns the value of `key`. Never blocks: reads the current table
    /// under a guard, even while a resize is in flight.
    ///
    /// Linearizable: the answer is the value of a node that was not deleted
    /// at the moment its `next` was read, or `None` when the key's chain
    /// held no such node.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let guard = pin();
        self.lookup(hash, key, &guard)
            .map(|node| node.value.clone())
    }

    /// Checks if the key exists (without cloning its value).
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let guard = pin();
        self.lookup(hash, key, &guard).is_some()
    }

    /// Inserts a key-value pair, returning the value it replaced.
    ///
    /// Linearizable at the one CAS that links the new node (a new key) or
    /// marks the old node naming the new one (a present key); the answer is
    /// exactly the value that CAS replaced.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);
        let mut pending = Pending::Parts { key, value };
        let mut backoff = Backoff::new();
        loop {
            let guard = pin();
            let (table, found) = self.find(hash, pending.key(), &guard);
            match found {
                Found::Frozen => {
                    drop(guard);
                    self.wait_for_resize();
                }
                Found::Miss { tail } => {
                    let node = pending.into_node(hash, core::ptr::null_mut());
                    // Release: a reader that acquires the link sees the node's fields.
                    match tail.compare_exchange(
                        word(core::ptr::null_mut()),
                        word(node),
                        Ordering::Release,
                        Ordering::Relaxed,
                        &guard,
                    ) {
                        Ok(_) => {
                            self.landed(table, guard);
                            return None;
                        }
                        Err(_) => {
                            pending = Pending::back(node);
                            backoff.spin();
                        }
                    }
                }
                Found::Hit {
                    prev,
                    node: old,
                    next,
                } => {
                    // Cloned before the CAS: a panicking clone leaves the map as it was.
                    let previous = old.value.clone();
                    let node = pending.into_node(hash, next);
                    // The replace: the old node deleted, naming the new node, in one CAS.
                    // AcqRel: releases the new node; acquires what the old word carried.
                    match old.next.compare_exchange(
                        word(next),
                        word(with(node, MARK)),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                        &guard,
                    ) {
                        Ok(_) => {
                            self.unlink(prev, old, node, hash, &old.key, &guard);
                            return Some(previous);
                        }
                        Err(_) => {
                            pending = Pending::back(node);
                            backoff.spin();
                        }
                    }
                }
            }
        }
    }

    /// Insert a key-value pair only if the key does not exist.
    /// Returns `None` if inserted, `Some(existing_value)` if the key already exists.
    ///
    /// Exact: `None` exactly when this call linked the key's node (its CAS is
    /// the linearization point), `Some` with the value of the node it found
    /// live otherwise; never both, and a resize in flight changes neither.
    /// A call that finds the key present drops its own key and value, once.
    pub fn insert_if_absent(&self, key: K, value: V) -> Option<V> {
        match self.claim(key, value, |_| ()) {
            Claim::Inserted(()) => None,
            Claim::Present(existing) => Some(existing),
        }
    }

    /// Returns the value corresponding to the key, or inserts the given value if the key is not present.
    ///
    /// Linearizable and exact: of the callers racing for an absent key, the
    /// one whose CAS links the key's node gets its own value back, and every
    /// other gets the value it found live (the winner's, unless a later
    /// write already replaced it). One clone of the value either way.
    pub fn get_or_insert(&self, key: K, value: V) -> V {
        match self.claim(key, value, V::clone) {
            Claim::Inserted(own) | Claim::Present(own) => own,
        }
    }

    /// The conditional insert both `insert_if_absent` and `get_or_insert`
    /// are: link the key's node unless the key is present. `on_insert` reads
    /// the value this call linked, under its guard.
    fn claim<R>(&self, key: K, value: V, on_insert: impl FnOnce(&V) -> R) -> Claim<R, V> {
        let hash = self.hasher.hash_one(&key);
        let mut pending = Pending::Parts { key, value };
        let mut backoff = Backoff::new();
        loop {
            let guard = pin();
            let (table, found) = self.find(hash, pending.key(), &guard);
            match found {
                Found::Frozen => {
                    drop(guard);
                    self.wait_for_resize();
                }
                // Present: the node was not deleted when the walk read its `next`.
                Found::Hit { node, .. } => return Claim::Present(node.value.clone()),
                Found::Miss { tail } => {
                    let node = pending.into_node(hash, core::ptr::null_mut());
                    // Release: a reader that acquires the link sees the node's fields.
                    match tail.compare_exchange(
                        word(core::ptr::null_mut()),
                        word(node),
                        Ordering::Release,
                        Ordering::Relaxed,
                        &guard,
                    ) {
                        Ok(_) => {
                            // SAFETY: this call allocated and linked it under `guard`, which
                            // keeps it from being freed.
                            let answer = on_insert(unsafe { &(*node).value });
                            self.landed(table, guard);
                            return Claim::Inserted(answer);
                        }
                        Err(_) => {
                            pending = Pending::back(node);
                            backoff.spin();
                        }
                    }
                }
            }
        }
    }

    /// Remove a key-value pair, returning its value.
    ///
    /// Linearizable at the CAS that marks the key's node deleted; the answer
    /// is that node's value, and a later `get` of the key (with no insert of
    /// it in between) answers `None`.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut backoff = Backoff::new();
        loop {
            let guard = pin();
            let (table, found) = self.find(hash, key, &guard);
            match found {
                Found::Frozen => {
                    drop(guard);
                    self.wait_for_resize();
                }
                Found::Miss { .. } => return None,
                Found::Hit { prev, node, next } => {
                    // Cloned before the CAS: a panicking clone leaves the map as it was.
                    let value = node.value.clone();
                    // The removal: the node's own word marked, keeping its successor.
                    if node
                        .next
                        .compare_exchange(
                            word(next),
                            word(with(next, MARK)),
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                            &guard,
                        )
                        .is_err()
                    {
                        backoff.spin();
                        continue;
                    }
                    let remaining = table.count().fetch_sub(1, Ordering::Relaxed) - 1;
                    self.unlink(prev, node, next, hash, key, &guard);
                    let capacity = table.capacity();
                    // Integer load-factor check: count/cap < 1/4.
                    if 4 * (remaining.max(0) as usize) < capacity && capacity > self.floor {
                        drop(guard);
                        self.try_resize(capacity / 2);
                    }
                    return Some(value);
                }
            }
        }
    }

    /// Remove the key's entry, returning its value if the key was present.
    ///
    /// A key has at most one live node, so this removes what [`remove`](Self::remove)
    /// removes; it keeps removing until a call answers `None` (the key is
    /// absent at that call's linearization point), and it is kept for
    /// parity with `HopscotchMap::force_remove`.
    ///
    /// Note: a concurrent `insert` of the same key can land after the final
    /// scan, as with any removal under contention.
    pub fn force_remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut newest = None;
        loop {
            match self.remove(key) {
                Some(v) => {
                    if newest.is_none() {
                        newest = Some(v);
                    }
                }
                None => return newest,
            }
        }
    }

    /// Insert all `(K, V)` pairs from `iter`. Takes `&self` (concurrent map).
    pub fn extend<I: IntoIterator<Item = (K, V)>>(&self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }

    /// A new key landed in `table`: count it there, and grow the table past three quarters.
    #[inline]
    fn landed(&self, table: TableRef<K, V>, guard: Guard) {
        let count = table.count().fetch_add(1, Ordering::Relaxed) + 1;
        let capacity = table.capacity();
        // Integer load-factor check: count/cap > 3/4.
        if 4 * (count.max(0) as usize) > 3 * capacity {
            drop(guard);
            self.try_resize(capacity * 2);
        }
    }

    /// Unlink the deleted `node` from `prev`, which named it unmarked, in favour of `succ`. The
    /// thread whose CAS unlinks a node retires it; when this CAS fails (another snip, or the
    /// predecessor deleted too), the cleanup walk makes sure the node is unlinked before the
    /// caller returns.
    #[inline]
    fn unlink<Q>(
        &self,
        prev: &Atomic<Node<K, V>>,
        node: &Node<K, V>,
        succ: *mut Node<K, V>,
        hash: u64,
        key: &Q,
        guard: &Guard,
    ) where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let raw = node as *const Node<K, V> as *mut Node<K, V>;
        // AcqRel: as a snip (see `walk::find`).
        if prev
            .compare_exchange(
                word(raw),
                word(succ),
                Ordering::AcqRel,
                Ordering::Relaxed,
                guard,
            )
            .is_ok()
        {
            // SAFETY: this CAS unlinked it, so no other thread retires it and no link names it
            // again; a walker that loaded it holds a guard that keeps it. Node is #[repr(C)]
            // with its RetiredNode at offset 0.
            unsafe { retire(raw) };
        } else {
            self.cleanup(hash, key, guard);
        }
    }
}

// SAFETY: HashMap is Send if K, V, S are Send (moving ownership between threads).
// HashMap is Sync if K, V, S are Send+Sync. The stronger bound on Sync is needed
// because concurrent `get()` calls clone V through a `&V` reference across threads;
// if V were Send but not Sync, sharing `&HashMap` could transmit a non-Sync V reference
// to another thread via clone(), violating thread-safety.
unsafe impl<K: Send, V: Send, S: Send> Send for HashMap<K, V, S> {}
unsafe impl<K: Send + Sync, V: Send + Sync, S: Send + Sync> Sync for HashMap<K, V, S> {}

impl<K: 'static, V: 'static, S> Drop for HashMap<K, V, S> {
    fn drop(&mut self) {
        // SAFETY: `drop(&mut self)` guarantees exclusive ownership - no concurrent
        // readers can exist. Rust's type system enforces this: `Iter<'a, ...>` borrows
        // `&'a HashMap`, so it cannot outlive the `HashMap`. The Table's destructor
        // frees its chains.
        let guard = pin();
        let table = TableRef::<K, V>::from_raw(self.table.load(Ordering::Relaxed, &guard).as_raw());
        drop(guard);
        // SAFETY: exclusive access; frees remaining chains + the allocation.
        unsafe { table.free() };

        // Flush nodes/tables previously retired by concurrent operations
        kovan::flush();
    }
}

#[cfg(test)]
mod tests;
