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
use core::sync::atomic::{AtomicBool, AtomicIsize, Ordering, fence};
use foldhash::fast::FixedState;
use kovan::{Atomic, RetiredNode, Shared, pin, retire};
use node::{Node, is_tagged, tagged, untag};
use table::{TableHeader, TableRef};

pub use iter::{IntoIter, Iter, Keys, Values};

mod iter;
mod node;
mod table;

// vertexia: `resizing` gates `try_resize` (CAS to claim the resize) and is
// spun on by every other writer via `wait_for_resize`/`clear`'s CAS-retry
// while a resize is in flight. That spin has no *other* yield point in it,
// which is a problem under shuttle two levels deep:
//
// 1. Without any instrumented op in the loop, shuttle can't preempt out of
//    it at all -- a genuine hang once a writer observes `resizing == true`.
// 2. Instrumenting the field itself (an earlier version of this fix swapped
//    `AtomicBool` for shuttle's) fixes (1) but isn't enough for *fairness*:
//    PCT keeps a thread's priority fixed except at a handful of preselected
//    "change points" or an explicit yield, so a plain instrumented `.load()`
//    in a spin loop can still be rescheduled indefinitely if it happens to
//    hold the higher priority, starving the resizer and running out
//    shuttle's step budget ("exceeded max_steps bound", an unfair schedule,
//    not a real bug).
//
// The fix needs a *yield*, not an instrumented load: `resize_spin_hint`
// (below) calls `shuttle::hint::spin_loop`, which also calls
// `shuttle::thread::yield_now`, which PCT treats as an explicit change
// point, demoting the spinner's priority so the resizer is guaranteed a
// turn -- independent of whether the *condition* it's spinning on is
// instrumented. So `resizing` itself stays a plain `AtomicBool` under every
// build, shuttle included: swapping its type is unnecessary for either
// correctness or fairness here, and empirically, doing so anyway
// introduced its own unrelated shuttle-only heap corruption in this crate's
// shuttle test (reproduced independent of any resize ever triggering,
// isolated by bisection, still unexplained -- plausibly a layout hazard
// from shuttle's `AtomicBool` being a much larger `RefCell`-based type
// instead of a 1-byte one; not chased further since the type swap was
// never actually required).
#[inline(always)]
pub(crate) fn resize_spin_hint() {
    #[cfg(feature = "shuttle")]
    {
        shuttle::hint::spin_loop();
    }
    #[cfg(not(feature = "shuttle"))]
    {
        core::hint::spin_loop();
    }
}

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
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
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
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
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
            resize_spin_hint();
        }
    }

    /// Optimized get operation. Never blocks - reads the current table
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
        // Count a new key exactly once across re-validation retries:
        // prevents both under-count, which causes cascading resizes, and
        // double-count.
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
                        // We own the old node now - we retire it, exactly once.
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
                            // the replacement is not installed - retry the
                            // whole op (the removal already linearized).
                            drop(Box::from_raw(new_node));
                            backoff.spin();
                            continue 'outer;
                        }
                        // Dekker/SB fence: pairs with the matching fence in
                        // `try_resize`, placed right after it claims
                        // `resizing`. Without both fences, the swing-CAS
                        // above (a store) and the resizing/table loads just
                        // below are this thread's store-then-load half of a
                        // race against the resizer's own store-then-load
                        // half (claim `resizing`, then read this bucket
                        // during the sweep): plain AcqRel/Acquire lets both
                        // sides observe the pre-update value of the other's
                        // write (the classic store-buffering litmus test),
                        // so the sweep could miss this mutation while we
                        // simultaneously miss that a resize is in flight,
                        // silently orphaning the update in the table being
                        // retired. The fence forces this CAS and the
                        // resizer's `resizing` claim into the same SeqCst
                        // total order, so at least one side is guaranteed to
                        // observe the other. x86 TSO hides the gap (every
                        // CAS there is already a full fence); ARM's AcqRel
                        // is not.
                        fence(Ordering::SeqCst);
                        // Re-validate: if a resize started (or completed)
                        // since we loaded the table, the migration may have
                        // cloned the entry before our update - redo the op
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
                    // Dekker/SB fence pairing with try_resize's claim-side
                    // fence (full justification on the replace path above):
                    // the tail-append CAS above is this thread's store-side
                    // of the same store-load race.
                    fence(Ordering::SeqCst);
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
                    // Contention at the tail - retry the search/append loop.
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
                        // Found on a retry. This may be our own migrated
                        // clone OR another caller's entry that landed while
                        // the table swapped - the two are indistinguishable
                        // here, and reporting None for someone else's entry
                        // would admit a second winner. Return the canonical
                        // value either way (for our own clone that is a
                        // clone of the value we just inserted): under a
                        // concurrent resize a successful insert may report
                        // Some(its own value); callers must treat the
                        // returned value as canonical. (`HopscotchMap`
                        // closes this: its resize holds every home guard, so
                        // an insert that landed never retries.)
                        return Some(node.value.clone());
                    }
                    prev_link = &node.next;
                    current = next;
                }
            }

            // 2. Key not found (or our pre-migration insert was not carried
            //    over) - insert at TAIL. Untagged-null expectation makes the
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
                    if !counted {
                        counted = true;
                        self.count.fetch_add(1, Ordering::Relaxed);
                    }
                    // Dekker/SB fence pairing with try_resize's claim-side
                    // fence (full justification in HashMap::insert's replace
                    // path): the tail-append CAS above is this thread's
                    // store-side of the same store-load race.
                    fence(Ordering::SeqCst);
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
                                // Race lost, key exists now. Same canonical-
                                // value rule as the retry-found path above:
                                // even if a pre-migration attempt of ours
                                // inserted, the surviving entry is what
                                // every caller must converge on.
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
                        // owner - and only the tag owner - retires the node,
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

                        // Physical unlink (best effort - if it fails, a
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

                        // Single atomic decrement (signed counter - cannot
                        // wrap; a transient negative just clamps to 0 below).
                        let new_count =
                            (self.count.fetch_sub(1, Ordering::Relaxed) - 1).max(0) as usize;
                        // Integer load-factor check: count/cap < 1/4.
                        let shrink_to = (4 * new_count < table.capacity()
                            && table.capacity() > self.floor)
                            .then_some(table.capacity() / 2);

                        // Dekker/SB fence pairing with try_resize's
                        // claim-side fence (full justification in
                        // HashMap::insert's replace path): the tag-CAS above
                        // is this thread's store-side of the same
                        // store-load race, so the same "sweep misses the
                        // mutation and we miss the resize" window applies
                        // here, and the resurrection this re-validation
                        // exists to catch would otherwise go undetected.
                        fence(Ordering::SeqCst);
                        // Re-validate: a concurrent migration may have cloned
                        // this entry into the new table before we deleted it
                        // here - redo the removal on the current table so the
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
                    // order - the live (most recent) version.
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
            resize_spin_hint();
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
                        // already retired by their tag owners - skip them.
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
    /// nodes remain in its chains at reclamation time - entries removed or
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

        // Dekker/SB fence: pairs with the matching fence every writer
        // (insert/insert_if_absent/remove) executes right before its
        // resizing/table re-validation check. This claim-CAS and the
        // migration sweep's bucket reads below are this thread's
        // store-then-load half of the same store-load race a writer's
        // data-mutating CAS and its own re-validation load form the other
        // half of; without a fence on both sides, AcqRel/Acquire permits
        // both this sweep and that writer's check to observe the pre-update
        // value of the other's write (the classic store-buffering litmus
        // test), silently losing the writer's update to the table being
        // retired. x86 TSO hides the gap (every CAS there is already a full
        // fence); ARM's AcqRel is not.
        fence(Ordering::SeqCst);

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

        // Migrate: clone every live entry (logically-deleted nodes - tagged
        // next - are skipped). We are the only writer of the new table (it
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
                // Retire the old table through its proxy (built eagerly
                // back when this table was allocated, see `TableRef::alloc`,
                // so its birth_epoch predates every straggler that could
                // have observed this table): reclamation (which frees the
                // remaining chains and the allocation) is deferred until
                // every guard that could observe it is gone.
                let proxy = old_table.take_proxy();
                // SAFETY: TableProxy is #[repr(C)] with RetiredNode at
                // offset 0, allocated via Box::into_raw.
                unsafe { retire(proxy) };
            }
            Err(_) => {
                // Table changed under us (cannot normally happen - we hold
                // the resize latch). Discard the unpublished new table.
                unsafe { new_table.free() };
            }
        }

        self.resizing.store(false, Ordering::Release);
    }

    /// Insert all `(K, V)` pairs from `iter`. Takes `&self` (concurrent map).
    pub fn extend<I: IntoIterator<Item = (K, V)>>(&self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }

    /// Get the underlying hasher itself.
    pub fn hasher(&self) -> &S {
        &self.hasher
    }
}

#[cfg(feature = "std")]
impl<K, V> Default for HashMap<K, V, FixedState>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
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
        // SAFETY: `drop(&mut self)` guarantees exclusive ownership - no concurrent
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
mod tests;
