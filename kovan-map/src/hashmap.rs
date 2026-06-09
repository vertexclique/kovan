//! High-Performance Lock-Free Concurrent Hash Map (FoldHash + resizable table).
//!
//! # Strategy
//!
//! 1. **FoldHash**: `foldhash::fast::FixedState` for fast, quality hashing.
//! 2. **Resizable bucket table**: the bucket array lives in a single
//!    allocation (header + inline buckets) swapped atomically on resize and
//!    reclaimed through kovan. The map grows when the load factor exceeds
//!    3/4 and shrinks below 1/4 (never under its initial capacity),
//!    mirroring `HopscotchMap`'s resize protocol.
//! 3. **Optimized Node Layout**: fields ordered `hash -> key -> value -> next`
//!    to optimize cache line usage during checks.
//!
//! # Architecture
//! - **Table**: kovan-retired object holding the bucket array (atomic head
//!   pointers). Readers snapshot the table under a guard and never block.
//! - **Nodes**: singly linked chains, CAS-based lock-free insert/remove.
//! - **Resize**: a single resizer (CAS on `resizing`) clones all entries
//!   into a new table, swaps the table pointer, and retires the old table;
//!   the old table's destructor frees its remaining chains exactly once at
//!   reclamation time. Writers wait out an active resize and re-validate
//!   after success so no update is lost to a concurrent migration.

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

use alloc::boxed::Box;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use foldhash::fast::FixedState;
use kovan::{Atomic, RetiredNode, Shared, pin, retire};

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

/// Node in the lock-free linked list.
#[repr(C)]
struct Node<K, V> {
    retired: RetiredNode,
    hash: u64,
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}

// SAFETY (kovan retirement rule): a retired Node's destructor may run on
// any thread, and nodes (with K and V inside) move between threads — hence
// `K: Send, V: Send` for Send. Unlike exclusive-transfer containers,
// lookups DO produce `&K`/`&V` from a shared `&Node` (get() clones V
// through &V under concurrent readers), so Sync additionally requires
// `K: Sync, V: Sync` — the same bounds the map-level Sync impl below has
// always required for sharing the map.
unsafe impl<K: Send, V: Send> Send for Node<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Node<K, V> {}

// ---------------------------------------------------------------------------
// Harris-style logical deletion
// ---------------------------------------------------------------------------
//
// Removing (or replacing) a node first TAGS the victim's `next` pointer
// (low bit set) — the logical delete — and only then unlinks it from its
// predecessor. The thread whose tag-CAS succeeded exclusively owns the node
// and is the only one to `retire()` it. This closes two races a plain
// unlink-CAS protocol has:
//
//  * insert-after-removed-tail: a tail insert CASes `tail.next: null -> new`;
//    if the tail was concurrently unlinked and retired, the new node is
//    spliced onto dead memory — the insert is lost and the node leaks.
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
//  * a node whose `next` is tagged has been retired by its tag owner —
//    `clear()`, the migration sweep, and `Table::drop` must skip it;
//  * every traversal untags before following a `next` pointer.

#[inline(always)]
fn tagged<K, V>(p: *mut Node<K, V>) -> *mut Node<K, V> {
    (p as usize | 1) as *mut Node<K, V>
}

#[inline(always)]
fn untag<K, V>(p: *mut Node<K, V>) -> *mut Node<K, V> {
    (p as usize & !1) as *mut Node<K, V>
}

#[inline(always)]
fn is_tagged<K, V>(p: *const Node<K, V>) -> bool {
    (p as usize) & 1 != 0
}

// ---------------------------------------------------------------------------
// Single-allocation table: [TableHeader][Atomic<Node>; capacity]
// ---------------------------------------------------------------------------
//
// The header and the bucket array share one allocation, so the read path is
// `table ptr -> header line (mask, hot in cache) -> bucket line` — the same
// number of cold dereferences as a fixed embedded array. The table pointer
// itself is swapped atomically on resize.
//
// Reclamation goes through a tiny boxed `TableProxy` (RetiredNode at offset
// 0, as `retire()` requires): retiring the proxy defers until every guard
// that could observe the old table has been released; the proxy's destructor
// then frees the table's remaining chains and the allocation itself.

#[repr(C)]
struct TableHeader {
    mask: usize,
    capacity: usize,
}

/// Borrowed view of a table allocation.
struct TableRef<K: 'static, V: 'static> {
    header: *mut TableHeader,
    _marker: core::marker::PhantomData<(K, V)>,
}

impl<K, V> Clone for TableRef<K, V> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<K, V> Copy for TableRef<K, V> {}

impl<K: 'static, V: 'static> TableRef<K, V> {
    #[inline(always)]
    fn from_raw(header: *mut TableHeader) -> Self {
        Self {
            header,
            _marker: core::marker::PhantomData,
        }
    }

    fn layout(capacity: usize) -> (core::alloc::Layout, usize) {
        let header = core::alloc::Layout::new::<TableHeader>();
        let buckets = core::alloc::Layout::array::<Atomic<Node<K, V>>>(capacity)
            .expect("bucket array layout overflow");
        let (layout, offset) = header.extend(buckets).expect("table layout overflow");
        (layout.pad_to_align(), offset)
    }

    /// Allocate a zero-initialized table (`Atomic` buckets zero == null).
    fn alloc(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(MIN_CAPACITY);
        let (layout, _) = Self::layout(capacity);
        // SAFETY: layout is non-zero sized; zeroed AtomicUsize == null bucket.
        let header = unsafe { alloc::alloc::alloc_zeroed(layout) as *mut TableHeader };
        assert!(!header.is_null(), "table allocation failed");
        unsafe {
            (*header).mask = capacity - 1;
            (*header).capacity = capacity;
        }
        Self::from_raw(header)
    }

    /// Free remaining chains (skipping tagged nodes — already retired by
    /// their tag owners) and the allocation itself.
    ///
    /// # Safety
    /// Caller must have exclusive access (map drop, or proxy reclamation
    /// after guard quiescence).
    unsafe fn free(self) {
        let capacity = unsafe { (*self.header).capacity };
        let guard = pin();
        for i in 0..capacity {
            let mut current = self.bucket(i).load(Ordering::Relaxed, &guard).as_raw();
            while !current.is_null() {
                unsafe {
                    let next = (*current).next.load(Ordering::Relaxed, &guard).as_raw();
                    if !is_tagged(next) {
                        drop(Box::from_raw(current));
                    }
                    current = untag(next);
                }
            }
        }
        drop(guard);
        let (layout, _) = Self::layout(capacity);
        unsafe { alloc::alloc::dealloc(self.header as *mut u8, layout) };
    }

    #[inline(always)]
    fn as_raw(self) -> *mut TableHeader {
        self.header
    }

    #[inline(always)]
    fn capacity(self) -> usize {
        unsafe { (*self.header).capacity }
    }

    #[inline(always)]
    fn buckets(self) -> *const Atomic<Node<K, V>> {
        let (_, offset) = Self::layout_offset();
        unsafe { (self.header as *const u8).add(offset) as *const Atomic<Node<K, V>> }
    }

    /// Header/bucket offset is capacity-independent; compute it once.
    #[inline(always)]
    fn layout_offset() -> ((), usize) {
        let header = core::alloc::Layout::new::<TableHeader>();
        let one = core::alloc::Layout::new::<Atomic<Node<K, V>>>();
        let (_, offset) = header.extend(one).expect("layout");
        ((), offset)
    }

    #[inline(always)]
    fn bucket_index(self, hash: u64) -> usize {
        (hash as usize) & unsafe { (*self.header).mask }
    }

    #[inline(always)]
    fn bucket(self, idx: usize) -> &'static Atomic<Node<K, V>> {
        // SAFETY: idx is masked or bounded by capacity; the 'static is a
        // lie scoped by the caller's guard (same discipline as Shared).
        unsafe { &*self.buckets().add(idx) }
    }
}

/// Reclamation proxy for a table allocation (RetiredNode at offset 0).
#[repr(C)]
struct TableProxy<K: 'static, V: 'static> {
    retired: RetiredNode,
    table: TableRef<K, V>,
}

// SAFETY (kovan retirement rule): the proxy's destructor (running on any
// thread) frees the table's nodes, hence K, V: Send.
unsafe impl<K: Send, V: Send> Send for TableProxy<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for TableProxy<K, V> {}

impl<K, V> Drop for TableProxy<K, V> {
    fn drop(&mut self) {
        // SAFETY: kovan reclaimed the proxy only after every guard that
        // could observe the old table has been released.
        unsafe { self.table.free() };
    }
}

/// High-Performance Lock-Free Map with automatic grow/shrink.
pub struct HashMap<K: 'static, V: 'static, S = FixedState> {
    table: Atomic<TableHeader>,
    /// Approximate live-entry count driving the resize thresholds.
    /// Signed: a transient negative under racing removes is harmless and
    /// avoids a CAS loop (fetch_update) on the hot remove path.
    count: AtomicIsize,
    /// Single-resizer latch; writers wait while a resize is in flight.
    resizing: AtomicBool,
    /// Shrink floor: the initial capacity. The map never shrinks below the
    /// size it was created with, preserving the caller's sizing intent (and
    /// the historical fixed-table behavior for `new()`).
    floor: usize,
    hasher: S,
    _marker: core::marker::PhantomData<(K, V)>,
}

#[cfg(feature = "std")]
impl<K, V> HashMap<K, V, FixedState>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
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
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
    S: BuildHasher,
{
    /// Creates a new hash map with custom hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(DEFAULT_CAPACITY, hasher)
    }

    /// Creates a new hash map with at least `capacity` buckets and a custom hasher.
    ///
    /// The map grows when its load factor exceeds 0.75 and shrinks when it
    /// falls below 0.25 — but never below `capacity`.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let table = TableRef::<K, V>::alloc(capacity);
        let floor = table.capacity();
        Self {
            table: Atomic::new(table.as_raw()),
            count: AtomicIsize::new(0),
            resizing: AtomicBool::new(false),
            floor,
            hasher,
            _marker: core::marker::PhantomData,
        }
    }

    /// Returns the current number of buckets.
    pub fn capacity(&self) -> usize {
        let guard = pin();
        let table = TableRef::<K, V>::from_raw(self.table.load(Ordering::Acquire, &guard).as_raw());
        table.capacity()
    }

    /// Spin until any in-flight resize completes.
    #[inline]
    fn wait_for_resize(&self) {
        while self.resizing.load(Ordering::Acquire) {
            core::hint::spin_loop();
        }
    }

    /// Optimized get operation. Never blocks — reads the current table
    /// snapshot under a guard, even while a resize is in flight.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let guard = pin();
        let table = TableRef::<K, V>::from_raw(self.table.load(Ordering::Acquire, &guard).as_raw());
        let bucket = table.bucket(table.bucket_index(hash));

        let mut current = bucket.load(Ordering::Acquire, &guard).as_raw();
        while !current.is_null() {
            unsafe {
                let node = &*current;
                // Check hash first (integer compare is fast). Matching a
                // logically-deleted node is linearizable (the read happened
                // before the delete), so no tag check on the match path.
                if node.hash == hash && node.key.borrow() == key {
                    return Some(node.value.clone());
                }
                // Untag: the pointer may carry the deletion tag.
                current = untag(node.next.load(Ordering::Acquire, &guard).as_raw());
            }
        }
        None
    }

    /// Checks if the key exists.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);
        let mut backoff = Backoff::new();
        // Count a new key exactly once across re-validation retries
        // (mirrors HopscotchMap: prevents both under-count, which causes
        // cascading resizes, and double-count).
        let mut counted = false;
        // The first successful op's previous value is the linearized result;
        // re-validation retries may replace a migrated clone of it.
        let mut result: Option<Option<V>> = None;

        'outer: loop {
            self.wait_for_resize();

            let guard = pin();
            let table_raw = self.table.load(Ordering::Acquire, &guard).as_raw();
            let table = TableRef::<K, V>::from_raw(table_raw);
            if self.resizing.load(Ordering::Acquire) {
                continue;
            }

            let bucket = table.bucket(table.bucket_index(hash));

            // 1. Search for existing key to update (snip-walk: physically
            //    unlink logically-deleted nodes as we pass them).
            let mut prev_link = bucket;
            let mut current = prev_link.load(Ordering::Acquire, &guard).as_raw();

            while !current.is_null() {
                unsafe {
                    let node = &*current;
                    let next = node.next.load(Ordering::Acquire, &guard).as_raw();

                    if is_tagged(next) {
                        // Logically deleted: snip it out (its tag owner has
                        // already retired it). On contention restart the scan.
                        if prev_link
                            .compare_exchange(
                                Shared::from_raw(current),
                                Shared::from_raw(untag(next)),
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                                &guard,
                            )
                            .is_err()
                        {
                            backoff.spin();
                            continue 'outer;
                        }
                        current = untag(next);
                        continue;
                    }

                    if node.hash == hash && node.key == key {
                        // Replace: logically delete the old node (tag-CAS
                        // makes us its exclusive owner), then swing the
                        // predecessor to the replacement in one step.
                        let old_value = node.value.clone();
                        if node
                            .next
                            .compare_exchange(
                                Shared::from_raw(next),
                                Shared::from_raw(tagged(next)),
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                                &guard,
                            )
                            .is_err()
                        {
                            // Someone else deleted/replaced it first.
                            backoff.spin();
                            continue 'outer;
                        }
                        // We own the old node now — we retire it, exactly once.
                        let new_node = Box::into_raw(Box::new(Node {
                            retired: RetiredNode::new(),
                            hash,
                            key: key.clone(),
                            value: value.clone(),
                            next: Atomic::new(next),
                        }));
                        let swapped = prev_link
                            .compare_exchange(
                                Shared::from_raw(current),
                                Shared::from_raw(new_node),
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                                &guard,
                            )
                            .is_ok();
                        // SAFETY: tag ownership; Node is #[repr(C)] with
                        // RetiredNode at offset 0.
                        retire(current);
                        if result.is_none() {
                            result = Some(Some(old_value));
                        }
                        if !swapped {
                            // A helper snipped the old node before our swing;
                            // the replacement is not installed — retry the
                            // whole op (the removal already linearized).
                            drop(Box::from_raw(new_node));
                            backoff.spin();
                            continue 'outer;
                        }
                        // Re-validate: if a resize started (or completed)
                        // since we loaded the table, the migration may have
                        // cloned the entry before our update — redo the op
                        // on the new table so the update is not lost.
                        if self.resizing.load(Ordering::SeqCst)
                            || self.table.load(Ordering::SeqCst, &guard).as_raw() != table_raw
                        {
                            continue 'outer;
                        }
                        return result.unwrap();
                    }

                    prev_link = &node.next;
                    current = next;
                }
            }

            // 2. Key not found. Insert at TAIL (prev_link). The CAS expects
            //    an untagged null, so it fails if the tail node was
            //    concurrently logically deleted.
            let new_node_ptr = Box::into_raw(Box::new(Node {
                retired: RetiredNode::new(),
                hash,
                key: key.clone(),
                value: value.clone(),
                next: Atomic::null(),
            }));

            match prev_link.compare_exchange(
                unsafe { Shared::from_raw(core::ptr::null_mut()) },
                unsafe { Shared::from_raw(new_node_ptr) },
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => {
                    if !counted {
                        counted = true;
                        self.count.fetch_add(1, Ordering::Relaxed);
                    }
                    if result.is_none() {
                        result = Some(None);
                    }
                    // Re-validate against a concurrent migration (see above).
                    if self.resizing.load(Ordering::SeqCst)
                        || self.table.load(Ordering::SeqCst, &guard).as_raw() != table_raw
                    {
                        continue 'outer;
                    }

                    // Grow check (only when we actually added an entry).
                    let new_count = self.count.load(Ordering::Relaxed).max(0) as usize;
                    let capacity = table.capacity();
                    // Integer load-factor check: count/cap > 3/4.
                    if 4 * new_count > 3 * capacity {
                        drop(guard);
                        self.try_resize(capacity * 2);
                    }
                    return result.unwrap();
                }
                Err(_) => {
                    // Contention at the tail — retry the search/append loop.
                    unsafe {
                        drop(Box::from_raw(new_node_ptr));
                    }
                    backoff.spin();
                    continue 'outer;
                }
            }
        }
    }

    /// Insert a key-value pair only if the key does not exist.
    /// Returns `None` if inserted, `Some(existing_value)` if the key already exists.
    pub fn insert_if_absent(&self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);
        let mut backoff = Backoff::new();
        let mut counted = false;
        let mut inserted = false;

        'outer: loop {
            self.wait_for_resize();

            let guard = pin();
            let table_raw = self.table.load(Ordering::Acquire, &guard).as_raw();
            let table = TableRef::<K, V>::from_raw(table_raw);
            if self.resizing.load(Ordering::Acquire) {
                continue;
            }

            let bucket = table.bucket(table.bucket_index(hash));

            // 1. Search for existing key (snip-walk).
            let mut prev_link = bucket;
            let mut current = prev_link.load(Ordering::Acquire, &guard).as_raw();

            while !current.is_null() {
                unsafe {
                    let node = &*current;
                    let next = node.next.load(Ordering::Acquire, &guard).as_raw();

                    if is_tagged(next) {
                        if prev_link
                            .compare_exchange(
                                Shared::from_raw(current),
                                Shared::from_raw(untag(next)),
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                                &guard,
                            )
                            .is_err()
                        {
                            backoff.spin();
                            continue 'outer;
                        }
                        current = untag(next);
                        continue;
                    }

                    if node.hash == hash && node.key == key {
                        // If WE inserted this entry on a previous
                        // (pre-migration) attempt, the op already succeeded.
                        if inserted {
                            return None;
                        }
                        return Some(node.value.clone());
                    }
                    prev_link = &node.next;
                    current = next;
                }
            }

            // 2. Key not found (or our pre-migration insert was not carried
            //    over) — insert at TAIL. Untagged-null expectation makes the
            //    CAS fail if the tail was concurrently logically deleted.
            let new_node_ptr = Box::into_raw(Box::new(Node {
                retired: RetiredNode::new(),
                hash,
                key: key.clone(),
                value: value.clone(),
                next: Atomic::null(),
            }));

            match prev_link.compare_exchange(
                unsafe { Shared::from_raw(core::ptr::null_mut()) },
                unsafe { Shared::from_raw(new_node_ptr) },
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => {
                    inserted = true;
                    if !counted {
                        counted = true;
                        self.count.fetch_add(1, Ordering::Relaxed);
                    }
                    // Re-validate against a concurrent migration.
                    if self.resizing.load(Ordering::SeqCst)
                        || self.table.load(Ordering::SeqCst, &guard).as_raw() != table_raw
                    {
                        continue 'outer;
                    }

                    let new_count = self.count.load(Ordering::Relaxed).max(0) as usize;
                    let capacity = table.capacity();
                    // Integer load-factor check: count/cap > 3/4.
                    if 4 * new_count > 3 * capacity {
                        drop(guard);
                        self.try_resize(capacity * 2);
                    }
                    return None;
                }
                Err(actual_val) => {
                    // Contention at the tail.
                    unsafe {
                        let appended_ptr = actual_val.as_raw();
                        drop(Box::from_raw(new_node_ptr));
                        if !is_tagged(appended_ptr) && !appended_ptr.is_null() {
                            let appended = &*appended_ptr;
                            if appended.hash == hash && appended.key == key {
                                // Race lost, key exists now.
                                if inserted {
                                    return None;
                                }
                                return Some(appended.value.clone());
                            }
                        }
                    }
                    backoff.spin();
                    continue 'outer;
                }
            }
        }
    }

    /// Returns the value corresponding to the key, or inserts the given value if the key is not present.
    ///
    /// This is linearizable: concurrent callers for the same key are guaranteed to
    /// agree on which value was inserted (exactly one thread's CAS succeeds at the
    /// list tail, and all others see that node on retry).
    pub fn get_or_insert(&self, key: K, value: V) -> V {
        match self.insert_if_absent(key, value.clone()) {
            Some(existing) => existing,
            None => value,
        }
    }

    /// Remove a key-value pair.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut backoff = Backoff::new();
        // First successful removal's value is the linearized result;
        // re-validation retries only evict migrated clones.
        let mut result: Option<V> = None;

        'outer: loop {
            self.wait_for_resize();

            let guard = pin();
            let table_raw = self.table.load(Ordering::Acquire, &guard).as_raw();
            let table = TableRef::<K, V>::from_raw(table_raw);
            if self.resizing.load(Ordering::Acquire) {
                continue;
            }

            let bucket = table.bucket(table.bucket_index(hash));

            let mut prev_link = bucket;
            let mut current = prev_link.load(Ordering::Acquire, &guard).as_raw();

            while !current.is_null() {
                unsafe {
                    let node = &*current;
                    let next = node.next.load(Ordering::Acquire, &guard).as_raw();

                    if is_tagged(next) {
                        // Logically deleted by someone else: snip and move on.
                        if prev_link
                            .compare_exchange(
                                Shared::from_raw(current),
                                Shared::from_raw(untag(next)),
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                                &guard,
                            )
                            .is_err()
                        {
                            backoff.spin();
                            continue 'outer;
                        }
                        current = untag(next);
                        continue;
                    }

                    if node.hash == hash && node.key.borrow() == key {
                        let old_value = node.value.clone();

                        // Logical delete: tag the victim's next. The tag
                        // owner — and only the tag owner — retires the node,
                        // and the tag makes concurrent tail-inserts onto
                        // this node fail.
                        if node
                            .next
                            .compare_exchange(
                                Shared::from_raw(next),
                                Shared::from_raw(tagged(next)),
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                                &guard,
                            )
                            .is_err()
                        {
                            backoff.spin();
                            continue 'outer;
                        }

                        // Physical unlink (best effort — if it fails, a
                        // later walker snips it).
                        let _ = prev_link.compare_exchange(
                            Shared::from_raw(current),
                            Shared::from_raw(next),
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                            &guard,
                        );

                        // SAFETY: tag ownership; Node is #[repr(C)] with
                        // RetiredNode at offset 0.
                        retire(current);
                        if result.is_none() {
                            result = Some(old_value);
                        }

                        // Single atomic decrement (signed counter — cannot
                        // wrap; a transient negative just clamps to 0 below).
                        let new_count =
                            (self.count.fetch_sub(1, Ordering::Relaxed) - 1).max(0) as usize;
                        // Integer load-factor check: count/cap < 1/4.
                        let shrink_to = (4 * new_count < table.capacity()
                            && table.capacity() > self.floor)
                            .then_some(table.capacity() / 2);

                        // Re-validate: a concurrent migration may have cloned
                        // this entry into the new table before we deleted it
                        // here — redo the removal on the current table so the
                        // key does not resurrect.
                        if self.resizing.load(Ordering::SeqCst)
                            || self.table.load(Ordering::SeqCst, &guard).as_raw() != table_raw
                        {
                            continue 'outer;
                        }

                        if let Some(cap) = shrink_to {
                            drop(guard);
                            self.try_resize(cap);
                        }
                        return result;
                    }

                    prev_link = &node.next;
                    current = next;
                }
            }

            // Key not present in the current table.
            return result;
        }
    }

    /// Remove **all** nodes matching `key`, returning the most recent value
    /// if the key was present.
    ///
    /// [`remove`](Self::remove) unlinks only the first matching entry.
    /// Insert/remove races can transiently leave more than one entry for
    /// the same key ("versions"); after a plain `remove()` an older version
    /// would become visible again. This method keeps removing until a full
    /// scan finds no match, so the key is guaranteed absent at the
    /// linearization point of the final scan.
    ///
    /// Use `remove()` for single-version removal semantics and
    /// `force_remove()` when the key must be fully evicted.
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
                    // The first removal unlinks the first match in scan
                    // order — the live (most recent) version.
                    if newest.is_none() {
                        newest = Some(v);
                    }
                }
                None => return newest,
            }
        }
    }

    /// Clear the map.
    pub fn clear(&self) {
        // Take the resize latch so the table cannot be swapped (and no
        // writer is mid-migration) while we unlink the chains.
        while self
            .resizing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }

        let guard = pin();
        let table = TableRef::<K, V>::from_raw(self.table.load(Ordering::Acquire, &guard).as_raw());

        for i in 0..table.capacity() {
            let bucket = table.bucket(i);
            loop {
                let head = bucket.load(Ordering::Acquire, &guard);
                if head.is_null() {
                    break;
                }

                // Try to unlink the whole chain at once
                match bucket.compare_exchange(
                    head,
                    unsafe { Shared::from_raw(core::ptr::null_mut()) },
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                ) {
                    Ok(_) => {
                        // Retire the chain's live nodes. Tagged nodes were
                        // already retired by their tag owners — skip them.
                        unsafe {
                            let mut current = head.as_raw();
                            while !current.is_null() {
                                let next = (*current).next.load(Ordering::Relaxed, &guard).as_raw();
                                if !is_tagged(next) {
                                    // SAFETY: allocated via Box::into_raw;
                                    // Node is #[repr(C)], RetiredNode first.
                                    retire(current);
                                }
                                current = untag(next);
                            }
                        }
                        break;
                    }
                    Err(_) => {
                        // Contention, retry
                        continue;
                    }
                }
            }
        }

        self.count.store(0, Ordering::Release);
        self.resizing.store(false, Ordering::Release);
    }

    /// Returns true if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of elements in the map.
    ///
    /// O(1): maintained by insert/remove. Approximate while concurrent
    /// updates are in flight (exact in quiescence), like `HopscotchMap`.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed).max(0) as usize
    }

    /// Resize the table to `new_capacity` buckets (single resizer wins).
    ///
    /// Clones every entry into a new table, swaps the table pointer, then
    /// retires the old table. The old table's destructor frees whatever
    /// nodes remain in its chains at reclamation time — entries removed or
    /// replaced in the meantime were unlinked and retired individually, so
    /// nothing is freed twice and nothing leaks.
    fn try_resize(&self, new_capacity: usize) {
        if self
            .resizing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let new_capacity = new_capacity
            .next_power_of_two()
            .max(MIN_CAPACITY)
            .max(self.floor);
        let guard = pin();
        let old_raw = self.table.load(Ordering::Acquire, &guard).as_raw();
        let old_table = TableRef::<K, V>::from_raw(old_raw);

        if old_table.capacity() == new_capacity {
            self.resizing.store(false, Ordering::Release);
            return;
        }

        let new_table = TableRef::<K, V>::alloc(new_capacity);

        // Migrate: clone every live entry (logically-deleted nodes — tagged
        // next — are skipped). We are the only writer of the new table (it
        // is unpublished), so plain stores are sufficient.
        for i in 0..old_table.capacity() {
            let bucket = old_table.bucket(i);
            let mut current = bucket.load(Ordering::Acquire, &guard).as_raw();
            while !current.is_null() {
                let node = unsafe { &*current };
                let next = node.next.load(Ordering::Acquire, &guard).as_raw();
                if !is_tagged(next) {
                    let dst = new_table.bucket(new_table.bucket_index(node.hash));
                    let head = dst.load(Ordering::Relaxed, &guard);
                    let clone = Box::into_raw(Box::new(Node {
                        retired: RetiredNode::new(),
                        hash: node.hash,
                        key: node.key.clone(),
                        value: node.value.clone(),
                        next: Atomic::new(head.as_raw()),
                    }));
                    dst.store(unsafe { Shared::from_raw(clone) }, Ordering::Relaxed);
                }
                current = untag(next);
            }
        }

        match self.table.compare_exchange(
            unsafe { Shared::from_raw(old_raw) },
            unsafe { Shared::from_raw(new_table.as_raw()) },
            Ordering::Release,
            Ordering::Relaxed,
            &guard,
        ) {
            Ok(_) => {
                // Retire the old table through a proxy: reclamation (which
                // frees the remaining chains and the allocation) is deferred
                // until every guard that could observe it is gone.
                let proxy = Box::into_raw(Box::new(TableProxy {
                    retired: RetiredNode::new(),
                    table: old_table,
                }));
                // SAFETY: TableProxy is #[repr(C)] with RetiredNode at
                // offset 0, allocated via Box::into_raw.
                unsafe { retire(proxy) };
            }
            Err(_) => {
                // Table changed under us (cannot normally happen — we hold
                // the resize latch). Discard the unpublished new table.
                unsafe { new_table.free() };
            }
        }

        self.resizing.store(false, Ordering::Release);
    }

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

    /// Get the underlying hasher itself.
    pub fn hasher(&self) -> &S {
        &self.hasher
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
                        // Logically deleted — do not yield.
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
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
    S: BuildHasher,
{
    type Item = (K, V);
    type IntoIter = Iter<'a, K, V, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(feature = "std")]
impl<K, V> Default for HashMap<K, V, FixedState>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
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
        // SAFETY: `drop(&mut self)` guarantees exclusive ownership — no concurrent
        // readers can exist.  Rust's type system enforces this: `Iter<'a, …>` borrows
        // `&'a HashMap`, so it cannot outlive the `HashMap`.  The Table's destructor
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
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let map = HashMap::new();
        assert_eq!(map.insert(1, 100), None);
        assert_eq!(map.get(&1), Some(100));
        assert_eq!(map.get(&2), None);
    }

    #[test]
    fn test_insert_replace() {
        let map = HashMap::new();
        assert_eq!(map.insert(1, 100), None);
        assert_eq!(map.insert(1, 200), Some(100));
        assert_eq!(map.get(&1), Some(200));
    }

    #[test]
    fn test_grow() {
        let map = HashMap::with_capacity(64);
        assert_eq!(map.capacity(), 64);
        for i in 0..1000u64 {
            map.insert(i, i * 2);
        }
        assert!(map.capacity() > 64, "map should have grown");
        for i in 0..1000u64 {
            assert_eq!(map.get(&i), Some(i * 2));
        }
        assert_eq!(map.len(), 1000);
    }

    #[test]
    fn test_shrink() {
        let map = HashMap::with_capacity(64);
        for i in 0..1000u64 {
            map.insert(i, i);
        }
        let grown = map.capacity();
        assert!(grown > 64);
        for i in 0..1000u64 {
            map.remove(&i);
        }
        assert!(
            map.capacity() < grown,
            "map should have shrunk (capacity {} -> {})",
            grown,
            map.capacity()
        );
        assert!(map.capacity() >= 64, "never below the initial capacity");
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_no_shrink_below_floor() {
        let map = HashMap::with_capacity(4096);
        for i in 0..100u64 {
            map.insert(i, i);
        }
        for i in 0..100u64 {
            map.remove(&i);
        }
        assert_eq!(map.capacity(), 4096, "floor preserves sizing intent");
    }

    #[test]
    fn test_concurrent_inserts() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::new());
        let mut handles = alloc::vec::Vec::new();

        for thread_id in 0..4 {
            let map_clone = Arc::clone(&map);
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    let key = thread_id * 1000 + i;
                    map_clone.insert(key, key * 2);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for thread_id in 0..4 {
            for i in 0..1000 {
                let key = thread_id * 1000 + i;
                assert_eq!(map.get(&key), Some(key * 2));
            }
        }
    }

    #[test]
    fn test_concurrent_grow() {
        use alloc::sync::Arc;
        extern crate std;
        use std::thread;

        let map = Arc::new(HashMap::with_capacity(64));
        let mut handles = alloc::vec::Vec::new();

        for thread_id in 0..8u64 {
            let map_clone = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                for i in 0..2000u64 {
                    let key = thread_id * 10_000 + i;
                    map_clone.insert(key, key);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        for thread_id in 0..8u64 {
            for i in 0..2000u64 {
                let key = thread_id * 10_000 + i;
                assert_eq!(map.get(&key), Some(key), "lost key {key} during growth");
            }
        }
        assert!(map.capacity() >= 16_000);
    }
}
