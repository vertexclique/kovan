//! The map's bucket table: one allocation holding a header and the bucket array, and the
//! reclamation proxy built with it, so a table a resize replaced is freed (with every node its
//! frozen chains still hold) only after every guard that could observe it is released.
//!
//! The table carries its own entry count, on a cache line of its own: a write counts itself in
//! the table it landed in, and a migration gives the new table the exact number of entries it
//! copied, so a write that landed in a table while it was being copied is counted once, in the
//! table that holds it after the copy, never in both and never twice.

extern crate alloc;

use super::MIN_CAPACITY;
use super::node::{Node, ptr};
use crate::sync::AtomicIsize;
use alloc::boxed::Box;
use core::sync::atomic::Ordering;
use kovan::{Atomic, CachePadded, RetiredNode, pin};

// ---------------------------------------------------------------------------
// Single-allocation table: [TableHeader][Atomic<Node>; capacity]
// ---------------------------------------------------------------------------
//
// The header and the bucket array share one allocation, so the read path is
// `table ptr -> header line (mask, hot in cache) -> bucket line` - the same
// number of cold dereferences as a fixed embedded array. The table pointer
// itself is swapped atomically on resize.
//
// Reclamation goes through a tiny boxed `TableProxy` (RetiredNode at offset
// 0, as `retire()` requires): retiring the proxy defers until every guard
// that could observe the old table has been released; the proxy's destructor
// then frees the table's remaining chains and the allocation itself.
//
// The proxy is built eagerly, in `TableRef::alloc`, at the SAME time as the
// table it guards, not lazily when the table is finally retired. See the
// long comment on `alloc` for why: a proxy built at retire time stamps its
// birth_epoch too late, and kovan can then judge a straggler writer as not
// needing protection for a table it is still actively CASing into.

#[repr(C)]
pub(super) struct TableHeader {
    mask: usize,
    capacity: usize,
    /// Type-erased `*mut TableProxy<K, V>` for this table's eventual
    /// retirement (see `TableRef::alloc`). Zero means already taken/freed.
    proxy: usize,
    /// The entries this table holds, on its own cache line: every insert and remove writes it,
    /// every lookup reads the line above. Signed: a remove can count itself before the insert
    /// of the same entry has, and the transient negative is clamped by `len`.
    count: CachePadded<AtomicIsize>,
}

/// Borrowed view of a table allocation.
pub(super) struct TableRef<K: 'static, V: 'static> {
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
    pub(super) fn from_raw(header: *mut TableHeader) -> Self {
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

    /// Allocate a zero-initialized table (`Atomic` buckets zero == null) and
    /// eagerly build its retirement proxy.
    ///
    /// The proxy is built *here*, at the table's own birth, not lazily at
    /// `try_resize` time. `RetiredNode::new()` stamps `birth_epoch` from the
    /// calling thread's cached epoch at construction time (kovan's
    /// contract: "birth_epoch must be set at allocation time, not
    /// retirement time" (see `kovan::RetiredNode::new`). A table can live
    /// through many other threads' operations before it is ever resized
    /// away; if its proxy were only constructed when the resizer finally
    /// retires it, the proxy's birth_epoch would be the *resizer's* current
    /// epoch, which can be arbitrarily newer than the epoch a straggler
    /// writer published the last time it observed this table via
    /// `self.table.load()`. kovan's eligibility check
    /// (`slot.epoch >= min_epoch`) would then wrongly judge that straggler
    /// as not needing protection, and its `TableProxy` could be reclaimed
    /// while the straggler is still mid-CAS on the table's own memory.
    /// Stamping the proxy at the table's own allocation predates every
    /// straggler that could ever see this table, closing the gap: the
    /// same guarantee `HopscotchMap::try_resize` gets for free by retiring
    /// its table struct directly (`RetiredNode` embedded at construction).
    pub(super) fn alloc(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(MIN_CAPACITY);
        let (layout, _) = Self::layout(capacity);
        // SAFETY: layout is non-zero sized; zeroed AtomicUsize == null bucket.
        let header = unsafe { alloc::alloc::alloc_zeroed(layout) as *mut TableHeader };
        if header.is_null() {
            alloc::alloc::handle_alloc_error(layout);
        }
        // SAFETY: `header` is a fresh allocation of the table's layout. The count is written in
        // place (shuttle's atomic is not valid zeroed).
        unsafe {
            (*header).mask = capacity - 1;
            (*header).capacity = capacity;
            core::ptr::write(
                core::ptr::addr_of_mut!((*header).count),
                CachePadded::new(AtomicIsize::new(0)),
            );
        }
        let table = Self::from_raw(header);
        // Shuttle's instrumented pointer word is not valid zeroed either: under the feature every
        // bucket is written in place. A build without it keeps the zeroed allocation as it is.
        #[cfg(feature = "shuttle")]
        for i in 0..capacity {
            // SAFETY: bucket `i` is within the allocation and not shared yet.
            unsafe { core::ptr::write(table.bucket_mut(i), Atomic::null()) };
        }
        let proxy = Box::into_raw(Box::new(TableProxy {
            retired: RetiredNode::new(),
            table,
        }));
        unsafe { (*header).proxy = proxy as usize };
        table
    }

    /// Take this table's pre-built retirement proxy, for a single upcoming
    /// `retire()` call. Must be called at most once per table.
    #[inline(always)]
    pub(super) fn take_proxy(self) -> *mut TableProxy<K, V> {
        let proxy = unsafe { (*self.header).proxy };
        assert_ne!(proxy, 0, "kovan-map: table proxy already taken");
        unsafe { (*self.header).proxy = 0 };
        proxy as *mut TableProxy<K, V>
    }

    /// Free this table's proxy directly (without running its `Drop`, which
    /// would call back into `free`/`free_array_only` on this same table) if
    /// it was never retired. Used on every path where a table dies without
    /// ever being resized away: `HashMap::drop` and `IntoIter`. No-op if
    /// `take_proxy` was already called (the normal resize-retirement path).
    #[inline(always)]
    unsafe fn drop_unused_proxy(self) {
        let proxy = unsafe { (*self.header).proxy };
        if proxy != 0 {
            unsafe {
                (*self.header).proxy = 0;
                alloc::alloc::dealloc(
                    proxy as *mut u8,
                    core::alloc::Layout::new::<TableProxy<K, V>>(),
                );
            }
        }
    }

    /// Free only the table allocation, not the chains (the caller already
    /// drained every node, e.g. `IntoIter`).
    ///
    /// # Safety
    /// Exclusive access; the chains must already be drained.
    pub(super) unsafe fn free_array_only(self) {
        unsafe { self.drop_unused_proxy() };
        let capacity = unsafe { (*self.header).capacity };
        unsafe { self.drop_words(capacity) };
        let (layout, _) = Self::layout(capacity);
        unsafe { alloc::alloc::dealloc(self.header as *mut u8, layout) };
    }

    /// Free every node the chains hold, deleted ones included (a node still in a chain was never
    /// unlinked, so never retired: the table owns it), and the allocation itself.
    ///
    /// # Safety
    /// Caller must have exclusive access (map drop, or proxy reclamation
    /// after guard quiescence).
    pub(super) unsafe fn free(self) {
        unsafe { self.drop_unused_proxy() };
        let capacity = unsafe { (*self.header).capacity };
        let guard = pin();
        for i in 0..capacity {
            let mut current = ptr(self.bucket(i).load(Ordering::Relaxed, &guard).as_raw());
            while !current.is_null() {
                // SAFETY: exclusive access; a node is in one chain, once.
                unsafe {
                    let next = ptr((*current).next.load(Ordering::Relaxed, &guard).as_raw());
                    drop(Box::from_raw(current));
                    current = next;
                }
            }
        }
        drop(guard);
        unsafe { self.drop_words(capacity) };
        let (layout, _) = Self::layout(capacity);
        unsafe { alloc::alloc::dealloc(self.header as *mut u8, layout) };
    }

    /// Drop the table's atomics in place: nothing to do for `core`'s, while shuttle's own their
    /// bookkeeping.
    ///
    /// # Safety
    /// Exclusive access, once, right before the allocation is freed.
    #[inline(always)]
    unsafe fn drop_words(self, _capacity: usize) {
        #[cfg(feature = "shuttle")]
        unsafe {
            core::ptr::drop_in_place(core::ptr::addr_of_mut!((*self.header).count));
            for i in 0.._capacity {
                core::ptr::drop_in_place(self.bucket_mut(i));
            }
        }
    }

    #[inline(always)]
    pub(super) fn as_raw(self) -> *mut TableHeader {
        self.header
    }

    #[inline(always)]
    pub(super) fn capacity(self) -> usize {
        unsafe { (*self.header).capacity }
    }

    /// The table's entry count (see `TableHeader::count`).
    #[inline(always)]
    pub(super) fn count(self) -> &'static AtomicIsize {
        // SAFETY: the 'static is a lie scoped by the caller's guard, as for `bucket`.
        unsafe { &(*self.header).count }
    }

    #[inline(always)]
    fn buckets(self) -> *mut Atomic<Node<K, V>> {
        let (_, offset) = Self::layout_offset();
        unsafe { (self.header as *mut u8).add(offset) as *mut Atomic<Node<K, V>> }
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
    pub(super) fn bucket_index(self, hash: u64) -> usize {
        (hash as usize) & unsafe { (*self.header).mask }
    }

    #[inline(always)]
    pub(super) fn bucket(self, idx: usize) -> &'static Atomic<Node<K, V>> {
        // SAFETY: idx is masked or bounded by capacity; the 'static is a
        // lie scoped by the caller's guard (same discipline as Shared).
        unsafe { &*self.buckets().add(idx) }
    }

    /// Bucket `idx` for its in-place construction or destruction.
    #[cfg(feature = "shuttle")]
    #[inline(always)]
    fn bucket_mut(self, idx: usize) -> *mut Atomic<Node<K, V>> {
        // SAFETY: idx is bounded by capacity.
        unsafe { self.buckets().add(idx) }
    }
}

/// Reclamation proxy for a table allocation (RetiredNode at offset 0).
#[repr(C)]
pub(super) struct TableProxy<K: 'static, V: 'static> {
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
