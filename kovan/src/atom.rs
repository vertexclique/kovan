//! `Atom<T>` — a lock-free single-value container with safe memory reclamation.
//!
//! This is a high-level wrapper around [`Atomic<T>`] that provides a safe API
//! for atomically swapping heap-allocated values with automatic reclamation.
//!
//! Think of it as the kovan equivalent of `ArcSwap`, but using wait-free
//! hazard-pointer-style reclamation instead of reference counting.
//!
//! # Key Properties
//!
//! - **Zero read overhead**: `load()` is a single atomic load
//! - **Safe API**: No `unsafe` at call sites
//! - **Automatic reclamation**: Old values are retired and reclaimed safely
//! - **`no_std` compatible**: Uses only `alloc`
//!
//! # Example
//!
//! ```rust
//! use kovan::Atom;
//!
//! let atom = Atom::new(42);
//!
//! // Zero-cost read — returns a guard that derefs to &T
//! let guard = atom.load();
//! assert_eq!(*guard, 42);
//! drop(guard);
//!
//! // Atomic swap — old value is safely retired
//! atom.store(99);
//! assert_eq!(*atom.load(), 99);
//! ```

use crate::atomic::Atomic;
use crate::guard::{self, Guard};
use crate::retired::RetiredNode;
use alloc::boxed::Box;
use core::fmt;
use core::marker::PhantomData as marker;
use core::mem::ManuallyDrop;
use core::ops::Deref;
use core::sync::atomic::Ordering;

// ---------------------------------------------------------------------------
// AtomNode<T> — internal storage wrapper
// ---------------------------------------------------------------------------
//
// `guard::retire()` casts the incoming pointer to `*mut RetiredNode` and
// writes linked-list metadata (smr_next, batch_next, nref_ptr) into the
// first bytes.  Therefore, **every** type passed to `retire()` MUST have a
// `RetiredNode` at offset 0.
//
// `AtomNode<T>` satisfies this invariant: it is `#[repr(C)]` with
// `RetiredNode` as the first field, followed by the user value `T`.
// `Atom<T>` stores `Atomic<AtomNode<T>>` internally, so the pointer
// used by readers AND the reclamation system is the same allocation.

#[repr(C)]
struct AtomNode<T> {
    _retired: RetiredNode,
    val: T,
}

impl<T> AtomNode<T> {
    fn new(val: T) -> Self {
        Self {
            _retired: RetiredNode::new(),
            val,
        }
    }

    /// Allocate a new node on the heap, returning a raw pointer.
    fn boxed(val: T) -> *mut Self {
        Box::into_raw(Box::new(Self::new(val)))
    }

    /// Get a pointer to the `val` field from a pointer to the node.
    ///
    /// # Safety
    /// `ptr` must be non-null and point to a valid `AtomNode<T>`.
    #[inline]
    unsafe fn val_ptr(ptr: *const Self) -> *const T {
        unsafe { core::ptr::addr_of!((*ptr).val) }
    }
}

// ---------------------------------------------------------------------------
// AtomGuard — RAII borrow returned by Atom::load()
// ---------------------------------------------------------------------------

/// RAII guard returned by [`Atom::load()`].
///
/// Dereferences to `&T`. The referenced value is guaranteed to remain valid
/// (not reclaimed) for the lifetime of this guard.
///
/// This is the cheapest way to read from an `Atom` — it avoids cloning
/// and holds a kovan critical-section guard internally.
pub struct AtomGuard<'a, T> {
    _guard: Guard,
    ptr: *const T,
    marker: marker<&'a T>,
}

impl<T> Deref for AtomGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: ptr is non-null (Atom always holds a valid allocation)
        // and the Guard keeps it alive.
        unsafe { &*self.ptr }
    }
}

impl<T: fmt::Debug> fmt::Debug for AtomGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for AtomGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

// NOTE: AtomGuard is intentionally !Send + !Sync because it contains a Guard,
// which pins the current thread's epoch slot. It must be dropped on the
// same thread that created it.

// ---------------------------------------------------------------------------
// Removed<T> — deferred-drop wrapper from swap/take
// ---------------------------------------------------------------------------

/// A value removed from an [`Atom`] or [`AtomOption`].
///
/// The value can be read via [`Deref`]. When this wrapper is dropped,
/// `T`'s destructor is **deferred** through the reclamation system —
/// it will only run once all concurrent readers have moved on.
///
/// This prevents use-after-free when concurrent readers hold an
/// [`AtomGuard`] that references the same value's internal heap
/// allocations (e.g. a `String`'s buffer or `Vec`'s backing array).
///
/// To take ownership without deferral (e.g. when you know no readers
/// exist), use [`into_inner_unchecked`](Removed::into_inner_unchecked).
pub struct Removed<T: Send + Sync + 'static> {
    value: ManuallyDrop<T>,
    /// Original birth_epoch from the AtomNode, preserved for safe reclamation.
    /// DeferDrop's RetiredNode must use this instead of a fresh epoch to prevent
    /// try_retire from prematurely reclaiming the wrapper while readers may still
    /// reference T's heap internals (e.g. String buffer, Vec backing array).
    birth_epoch: u64,
}

impl<T: Send + Sync + 'static> Deref for Removed<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Send + Sync + 'static> Removed<T> {
    /// Take ownership of the inner value **without** deferring its
    /// destructor.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no concurrent reader can still access
    /// the in-memory copy of this value (e.g. through an [`AtomGuard`]
    /// obtained before the swap/take).
    pub unsafe fn into_inner_unchecked(mut self) -> T {
        let val = unsafe { ManuallyDrop::take(&mut self.value) };
        core::mem::forget(self); // skip Removed::drop
        val
    }
}

impl<T: Send + Sync + 'static> Drop for Removed<T> {
    fn drop(&mut self) {
        // Move T into a heap-allocated wrapper with RetiredNode at offset 0,
        // then retire it. T's destructor will run when the reclamation
        // system confirms no readers exist.
        #[repr(C)]
        struct DeferDrop<U> {
            _retired: RetiredNode,
            value: U,
        }

        let val = unsafe { ManuallyDrop::take(&mut self.value) };
        let wrapper = Box::into_raw(Box::new(DeferDrop {
            _retired: RetiredNode::new(),
            value: val,
        }));
        // Override fresh birth_epoch with the original from the AtomNode.
        // A fresh epoch could be right after a reader's pinned epoch, causing
        // try_retire to skip protection and free while readers still
        // reference T's heap internals (e.g. String buffer, Vec backing).
        unsafe {
            (*(wrapper as *mut RetiredNode)).set_birth_epoch(self.birth_epoch);
        }
        // SAFETY: DeferDrop<U> is #[repr(C)] with RetiredNode at offset 0.
        // Allocated via Box::into_raw above.
        unsafe { guard::retire(wrapper) };
    }
}

impl<T: Send + Sync + 'static + PartialEq> PartialEq for Removed<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<T: Send + Sync + 'static + PartialEq> PartialEq<T> for Removed<T> {
    fn eq(&self, other: &T) -> bool {
        **self == *other
    }
}

impl<T: Send + Sync + 'static + fmt::Debug> fmt::Debug for Removed<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: Send + Sync + 'static + fmt::Display> fmt::Display for Removed<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

// SAFETY: Removed<T> is Send + Sync if T is, because the inner value
// is exclusively owned and only accessed through deref.
unsafe impl<T: Send + Sync + 'static> Send for Removed<T> {}
unsafe impl<T: Send + Sync + 'static> Sync for Removed<T> {}

// ---------------------------------------------------------------------------
// Atom<T> — the main container
// ---------------------------------------------------------------------------

/// A lock-free single-value container with safe memory reclamation.
///
/// Provides zero-overhead reads via [`load()`](Atom::load) and atomic writes
/// via [`store()`](Atom::store), [`swap()`](Atom::swap), and
/// [`rcu()`](Atom::rcu). Old values are automatically retired through
/// kovan's wait-free reclamation.
///
/// Unlike `ArcSwap`, `Atom<T>` works with any `T: Send + Sync + 'static` —
/// there is no requirement to wrap values in `Arc`.
///
/// # Examples
///
/// ```rust
/// use kovan::Atom;
///
/// // Create and read
/// let config = Atom::new(vec![1, 2, 3]);
/// assert_eq!(config.load().len(), 3);
///
/// // Atomic update — readers see old or new, never torn
/// config.store(vec![4, 5, 6, 7]);
/// assert_eq!(config.load().len(), 4);
/// ```
pub struct Atom<T: Send + Sync + 'static> {
    inner: Atomic<AtomNode<T>>,
}

impl<T: Send + Sync + 'static> Atom<T> {
    // ---- Construction ----

    /// Creates a new `Atom` containing `val`.
    ///
    /// The value is heap-allocated and managed by kovan's reclamation system.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    /// let atom = Atom::new(42);
    /// ```
    #[inline]
    pub fn new(val: T) -> Self {
        let ptr = AtomNode::boxed(val);
        Self {
            inner: Atomic::new(ptr),
        }
    }

    /// Consumes the `Atom` and returns the inner value.
    ///
    /// This is safe because consuming `self` guarantees no concurrent access.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    /// let atom = Atom::new(String::from("hello"));
    /// let s = atom.into_inner();
    /// assert_eq!(s, "hello");
    /// ```
    #[inline]
    pub fn into_inner(self) -> T {
        let guard = guard::pin();
        let shared = self.inner.load(Ordering::Acquire, &guard);
        let node_ptr = shared.as_raw();
        // Prevent Drop from also freeing the pointer
        core::mem::forget(self);
        // SAFETY: We own self, no concurrent access after consume.
        // The pointer was allocated via AtomNode::boxed.
        let node = unsafe { Box::from_raw(node_ptr) };
        node.val
    }

    // ---- Reading ----

    /// Loads the current value, returning a guard that dereferences to `&T`.
    ///
    /// This is the cheapest read operation — a single atomic load with
    /// zero overhead. The returned [`AtomGuard`] keeps the value alive;
    /// it will not be reclaimed until the guard is dropped.
    ///
    /// # Consistency
    ///
    /// Call `load()` once and reuse the guard for multiple accesses to
    /// ensure you see a consistent snapshot. Calling `load()` multiple
    /// times may return different values if another thread updated
    /// the `Atom` in between.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// let atom = Atom::new(42);
    /// let val = atom.load();
    /// assert_eq!(*val, 42);
    /// ```
    #[inline]
    pub fn load(&self) -> AtomGuard<'_, T> {
        let guard = guard::pin();
        let shared = self.inner.load(Ordering::Acquire, &guard);
        AtomGuard {
            // SAFETY: shared is non-null for Atom (always contains a value)
            ptr: unsafe { AtomNode::val_ptr(shared.as_raw()) },
            _guard: guard,
            marker,
        }
    }

    /// Loads and clones the current value.
    ///
    /// Equivalent to `atom.load().clone()` but expressed as a single call.
    /// Use this when you need an owned copy and `T: Clone`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// let atom = Atom::new(vec![1, 2, 3]);
    /// let owned: Vec<i32> = atom.load_clone();
    /// ```
    #[inline]
    pub fn load_clone(&self) -> T
    where
        T: Clone,
    {
        self.load().clone()
    }

    /// Calls a closure with a reference to the current value.
    ///
    /// This is useful when you want to inspect the value without
    /// holding onto a guard. The closure runs within a kovan critical
    /// section.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// let atom = Atom::new(vec![1, 2, 3]);
    /// let len = atom.peek(|v| v.len());
    /// assert_eq!(len, 3);
    /// ```
    #[inline]
    pub fn peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let guard = self.load();
        f(&*guard)
    }

    // ---- Writing ----

    /// Atomically replaces the current value.
    ///
    /// The old value is retired for safe reclamation — it will be freed
    /// once no reader holds a guard referencing it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// let atom = Atom::new(1);
    /// atom.store(2);
    /// assert_eq!(*atom.load(), 2);
    /// ```
    #[inline]
    pub fn store(&self, val: T) {
        let new_ptr = AtomNode::boxed(val);
        let guard = guard::pin();
        // SAFETY: new_ptr is a valid heap allocation we just created.
        let new_shared = unsafe { crate::atomic::Shared::from_raw(new_ptr) };
        let old = self.inner.swap(new_shared, Ordering::AcqRel, &guard);
        let old_ptr = old.as_raw();
        if !old_ptr.is_null() {
            // SAFETY: old_ptr was allocated via AtomNode::boxed and has
            // RetiredNode at offset 0. Not retired elsewhere.
            unsafe { guard::retire(old_ptr) };
        }
    }

    /// Atomically replaces the current value and returns the previous one
    /// wrapped in [`Removed<T>`].
    ///
    /// The returned [`Removed<T>`] defers `T`'s destructor through the
    /// reclamation system, preventing use-after-free when concurrent
    /// readers still reference the old value's heap internals. The value
    /// can be read via `Deref`.
    ///
    /// Unlike [`store()`](Atom::store), the old value is **not** retired
    /// immediately — ownership is transferred to the caller via `Removed<T>`.
    /// The memory backing the old node is retired for deallocation only.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// let atom = Atom::new(String::from("hello"));
    /// let old = atom.swap(String::from("world"));
    /// assert_eq!(*old, "hello");
    /// assert_eq!(*atom.load(), "world");
    /// ```
    #[inline]
    pub fn swap(&self, val: T) -> Removed<T> {
        let new_ptr = AtomNode::boxed(val);
        let guard = guard::pin();
        // SAFETY: new_ptr is a valid heap allocation.
        let new_shared = unsafe { crate::atomic::Shared::from_raw(new_ptr) };
        let old = self.inner.swap(new_shared, Ordering::AcqRel, &guard);
        let old_ptr = old.as_raw();
        // SAFETY: old_ptr was allocated via AtomNode::boxed.
        // We read the value out via ptr::read, then schedule the
        // AtomNode shell for deallocation only (no Drop on T).
        let old_val = unsafe { core::ptr::read(AtomNode::val_ptr(old_ptr)) };
        // Capture birth_epoch before retiring. Removed::drop will
        // use it on the DeferDrop wrapper to prevent epoch-skip UAF.
        let birth_epoch = unsafe { (*(old_ptr as *mut RetiredNode)).birth_epoch() };
        // SAFETY: old_ptr was allocated via AtomNode::boxed, val moved out via ptr::read.
        unsafe { retire_node_dealloc_only::<T>(old_ptr) };
        Removed {
            value: ManuallyDrop::new(old_val),
            birth_epoch,
        }
    }

    /// Compare-and-swap: atomically replace the value if it hasn't changed.
    ///
    /// Loads the current pointer, compares it to `current`'s pointer. If they
    /// match, stores `new` and retires the old value. Returns `Ok(guard)` with
    /// the new value on success, or `Err(new)` returning the rejected value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// let atom = Atom::new(1);
    /// let current = atom.load();
    /// // This succeeds because nobody changed it
    /// assert!(atom.compare_and_swap(&current, 2).is_ok());
    /// assert_eq!(*atom.load(), 2);
    /// ```
    #[inline]
    pub fn compare_and_swap<'a>(
        &'a self,
        current: &AtomGuard<'_, T>,
        new: T,
    ) -> Result<AtomGuard<'a, T>, T> {
        let new_ptr = AtomNode::boxed(new);
        let guard = guard::pin();
        let current_shared = self.inner.load(Ordering::Acquire, &guard);

        // Compare by pointer identity: current.ptr points to the val field
        // inside an AtomNode. current_shared.as_raw() points to the AtomNode.
        let current_val_ptr = unsafe { AtomNode::val_ptr(current_shared.as_raw()) };
        if current_val_ptr != current.ptr {
            // Current has changed — CAS fails
            // Reclaim the Box we just allocated
            let rejected = unsafe { Box::from_raw(new_ptr) };
            return Err(rejected.val);
        }

        // SAFETY: new_ptr is a valid heap allocation.
        let new_shared = unsafe { crate::atomic::Shared::from_raw(new_ptr) };

        match self.inner.compare_exchange(
            current_shared,
            new_shared,
            Ordering::AcqRel,
            Ordering::Acquire,
            &guard,
        ) {
            Ok(old) => {
                let old_ptr = old.as_raw();
                if !old_ptr.is_null() {
                    // SAFETY: AtomNode has RetiredNode at offset 0. Not retired elsewhere.
                    unsafe { guard::retire(old_ptr) };
                }
                Ok(AtomGuard {
                    ptr: unsafe { AtomNode::val_ptr(new_ptr) },
                    _guard: guard,
                    marker,
                })
            }
            Err(_) => {
                // CAS failed — someone else changed it between our load and CAS
                let rejected = unsafe { Box::from_raw(new_ptr) };
                Err(rejected.val)
            }
        }
    }

    /// Read-Copy-Update: atomically apply a transformation.
    ///
    /// Loads the current value, calls `f(&T)` to produce a new value,
    /// then attempts a CAS. If another thread modified the value between
    /// the load and CAS, the operation retries automatically.
    ///
    /// The closure `f` may be called multiple times on contention.
    /// Keep it cheap — do expensive work before calling `rcu`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// let counter = Atom::new(0u64);
    ///
    /// // Thread-safe increment (retries on contention)
    /// counter.rcu(|val| val + 1);
    /// assert_eq!(*counter.load(), 1);
    /// ```
    pub fn rcu<F>(&self, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        loop {
            let guard = guard::pin();
            let current = self.inner.load(Ordering::Acquire, &guard);
            // SAFETY: Atom always holds a non-null, valid pointer.
            let current_ref = unsafe { &*AtomNode::val_ptr(current.as_raw()) };
            let new_val = f(current_ref);
            let new_ptr = AtomNode::boxed(new_val);
            // SAFETY: new_ptr is a valid heap allocation.
            let new_shared = unsafe { crate::atomic::Shared::from_raw(new_ptr) };

            match self.inner.compare_exchange(
                current,
                new_shared,
                Ordering::AcqRel,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(old) => {
                    let old_ptr = old.as_raw();
                    if !old_ptr.is_null() {
                        // SAFETY: AtomNode has RetiredNode at offset 0. Not retired elsewhere.
                        unsafe { guard::retire(old_ptr) };
                    }
                    return;
                }
                Err(_) => {
                    // CAS failed — reclaim the allocation we made and retry
                    unsafe {
                        drop(Box::from_raw(new_ptr));
                    }
                    // Loop and try again
                }
            }
        }
    }

    // ---- Projection ----

    /// Create a mapped view that projects into a field of `T`.
    ///
    /// Returns an [`AtomMap`] that loads `T` and applies `f` to project
    /// into a sub-field. Useful for passing configuration sub-sections
    /// to components without exposing the full `T`.
    ///
    /// **Keep `f` cheap** — it runs on every `load`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kovan::Atom;
    ///
    /// struct Config { db_port: u16, workers: usize }
    /// let config = Atom::new(Config { db_port: 5432, workers: 4 });
    ///
    /// let port_view = config.map(|c| &c.db_port);
    /// assert_eq!(*port_view.load(), 5432);
    /// ```
    pub fn map<'a, R: 'a, F>(&'a self, f: F) -> AtomMap<'a, T, R, F>
    where
        F: Fn(&T) -> &R,
    {
        AtomMap {
            atom: self,
            project: f,
            marker,
        }
    }
}

impl<T: Send + Sync + 'static> Drop for Atom<T> {
    fn drop(&mut self) {
        // &mut self guarantees exclusive access — no concurrent readers.
        // No pin() needed; load directly from the underlying atomic.
        let ptr = self.inner.load_raw();
        if !ptr.is_null() {
            // SAFETY: We have exclusive access (dropping), and the pointer
            // was allocated via AtomNode::boxed.
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
        // Flush nodes previously retired by store()/rcu() operations.
        crate::flush();
    }
}

impl<T: Send + Sync + 'static + Default> Default for Atom<T> {
    /// Creates an `Atom` containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Send + Sync + 'static> From<T> for Atom<T> {
    fn from(val: T) -> Self {
        Self::new(val)
    }
}

impl<T: Send + Sync + 'static + fmt::Debug> fmt::Debug for Atom<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Atom")
            .field("value", &*self.load())
            .finish()
    }
}

// SAFETY: Atom is Send + Sync because:
// - The inner Atomic<AtomNode<T>> is Send + Sync (guarded by T: Send + Sync)
// - All mutations are atomic
// - Memory reclamation is thread-safe via kovan
unsafe impl<T: Send + Sync + 'static> Send for Atom<T> {}
unsafe impl<T: Send + Sync + 'static> Sync for Atom<T> {}

// ---------------------------------------------------------------------------
// AtomOption<T> — nullable variant
// ---------------------------------------------------------------------------

/// Like [`Atom<T>`] but allows a null/empty state.
///
/// This is useful for values that may not be present initially
/// or can be removed at runtime.
///
/// # Examples
///
/// ```rust
/// use kovan::AtomOption;
///
/// let opt: AtomOption<String> = AtomOption::none();
/// assert!(opt.load().is_none());
///
/// opt.store_some(String::from("hello"));
/// assert_eq!(&*opt.load().unwrap(), "hello");
///
/// let taken = opt.take();
/// assert_eq!(*taken.unwrap(), "hello");
/// assert!(opt.load().is_none());
/// ```
pub struct AtomOption<T: Send + Sync + 'static> {
    inner: Atomic<AtomNode<T>>,
}

impl<T: Send + Sync + 'static> AtomOption<T> {
    /// Creates an empty `AtomOption`.
    #[inline]
    pub fn none() -> Self {
        Self {
            inner: Atomic::null(),
        }
    }

    /// Creates an `AtomOption` containing `val`.
    #[inline]
    pub fn some(val: T) -> Self {
        let ptr = AtomNode::boxed(val);
        Self {
            inner: Atomic::new(ptr),
        }
    }

    /// Loads the current value, if present.
    ///
    /// Returns `None` if the atom is empty, or `Some(guard)` where
    /// the guard dereferences to `&T`.
    #[inline]
    pub fn load(&self) -> Option<AtomGuard<'_, T>> {
        let guard = guard::pin();
        let shared = self.inner.load(Ordering::Acquire, &guard);
        if shared.is_null() {
            None
        } else {
            Some(AtomGuard {
                ptr: unsafe { AtomNode::val_ptr(shared.as_raw()) },
                _guard: guard,
                marker,
            })
        }
    }

    /// Returns `true` if the atom is empty.
    #[inline]
    pub fn is_none(&self) -> bool {
        let guard = guard::pin();
        self.inner.load(Ordering::Acquire, &guard).is_null()
    }

    /// Returns `true` if the atom contains a value.
    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    /// Stores a value, retiring any previous value.
    #[inline]
    pub fn store_some(&self, val: T) {
        let new_ptr = AtomNode::boxed(val);
        let guard = guard::pin();
        let new_shared = unsafe { crate::atomic::Shared::from_raw(new_ptr) };
        let old = self.inner.swap(new_shared, Ordering::AcqRel, &guard);
        let old_ptr = old.as_raw();
        if !old_ptr.is_null() {
            // SAFETY: AtomNode has RetiredNode at offset 0. Not retired elsewhere.
            unsafe { guard::retire(old_ptr) };
        }
    }

    /// Clears the atom, retiring any current value.
    #[inline]
    pub fn store_none(&self) {
        let guard = guard::pin();
        let null_shared = unsafe { crate::atomic::Shared::from_raw(core::ptr::null_mut()) };
        let old = self.inner.swap(null_shared, Ordering::AcqRel, &guard);
        let old_ptr = old.as_raw();
        if !old_ptr.is_null() {
            // SAFETY: AtomNode has RetiredNode at offset 0. Not retired elsewhere.
            unsafe { guard::retire(old_ptr) };
        }
    }

    /// Takes the value out, leaving the atom empty.
    ///
    /// Returns `Some(Removed<T>)` if there was a value, `None` otherwise.
    /// The returned [`Removed<T>`] defers `T`'s destructor through the
    /// reclamation system. See [`Atom::swap`] for details.
    pub fn take(&self) -> Option<Removed<T>> {
        let guard = guard::pin();
        let null_shared = unsafe { crate::atomic::Shared::from_raw(core::ptr::null_mut()) };
        let old = self.inner.swap(null_shared, Ordering::AcqRel, &guard);
        let old_ptr = old.as_raw();
        if old_ptr.is_null() {
            None
        } else {
            // SAFETY: old_ptr was allocated via AtomNode::boxed.
            // We read the value out and schedule the node shell for dealloc only.
            let val = unsafe { core::ptr::read(AtomNode::val_ptr(old_ptr)) };
            // Same as swap.
            let birth_epoch = unsafe { (*(old_ptr as *mut RetiredNode)).birth_epoch() };
            // SAFETY: old_ptr was allocated via AtomNode::boxed, val moved out via ptr::read.
            unsafe { retire_node_dealloc_only::<T>(old_ptr) };
            Some(Removed {
                value: ManuallyDrop::new(val),
                birth_epoch,
            })
        }
    }
}

impl<T: Send + Sync + 'static> Drop for AtomOption<T> {
    fn drop(&mut self) {
        // &mut self guarantees exclusive access — no concurrent readers.
        // No pin() needed; load directly from the underlying atomic.
        let ptr = self.inner.load_raw();
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
        // Flush nodes previously retired by store() operations.
        crate::flush();
    }
}

impl<T: Send + Sync + 'static> Default for AtomOption<T> {
    fn default() -> Self {
        Self::none()
    }
}

impl<T: Send + Sync + 'static + fmt::Debug> fmt::Debug for AtomOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.load() {
            Some(guard) => f.debug_tuple("AtomOption::Some").field(&*guard).finish(),
            None => f.write_str("AtomOption::None"),
        }
    }
}

unsafe impl<T: Send + Sync + 'static> Send for AtomOption<T> {}
unsafe impl<T: Send + Sync + 'static> Sync for AtomOption<T> {}

// ---------------------------------------------------------------------------
// AtomMap — projected view
// ---------------------------------------------------------------------------

/// A projected view into an [`Atom<T>`], created by [`Atom::map()`].
///
/// Loading from an `AtomMap` loads the full `T` and applies the
/// projection function to return a reference to a sub-field.
pub struct AtomMap<'a, T: Send + Sync + 'static, R, F> {
    atom: &'a Atom<T>,
    project: F,
    marker: marker<R>,
}

/// Guard for a mapped/projected load.
pub struct AtomMapGuard<'a, T, R, F>
where
    T: Send + Sync + 'static,
    F: Fn(&T) -> &R,
{
    _inner: AtomGuard<'a, T>,
    projected: *const R,
    _project: marker<F>,
}

impl<T: Send + Sync + 'static, R, F: Fn(&T) -> &R> Deref for AtomMapGuard<'_, T, R, F> {
    type Target = R;

    #[inline]
    fn deref(&self) -> &R {
        // SAFETY: projected points into the AtomGuard's T, which is alive.
        unsafe { &*self.projected }
    }
}

impl<'a, T: Send + Sync + 'static, R, F> AtomMap<'a, T, R, F>
where
    F: Fn(&T) -> &R,
{
    /// Load the projected value.
    ///
    /// Internally loads the full `T` and applies the projection.
    #[inline]
    pub fn load(&self) -> AtomMapGuard<'a, T, R, F> {
        let guard = self.atom.load();
        let projected = (self.project)(&*guard) as *const R;
        AtomMapGuard {
            _inner: guard,
            projected,
            _project: marker,
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: retire an AtomNode without dropping T
// ---------------------------------------------------------------------------
//
// Used by `swap()` and `take()` where the value has been moved out.
// The AtomNode already has RetiredNode at offset 0, but we can't drop
// it normally (that would double-drop T). Instead we schedule deallocation
// of the AtomNode's memory without running T's destructor.

/// Retires an `AtomNode<T>` for deferred deallocation only (does not drop T).
///
/// Reuses the original AtomNode's embedded RetiredNode instead of allocating
/// a wrapper. This preserves the correct birth_epoch from allocation time
/// and avoids an extra heap allocation.
///
/// # Safety
///
/// - `ptr` must have been allocated via `AtomNode::boxed`
/// - The `val` field must have already been moved out (e.g. via `ptr::read`)
unsafe fn retire_node_dealloc_only<T: 'static>(ptr: *mut AtomNode<T>) {
    // Type-erased destructor that only deallocates the AtomNode's memory
    // without running T's destructor (T was already moved out via ptr::read).
    unsafe fn dealloc_destructor<T>(p: *mut RetiredNode) {
        unsafe {
            alloc::alloc::dealloc(p as *mut u8, alloc::alloc::Layout::new::<AtomNode<T>>());
        }
    }

    let node_ptr = ptr as *mut RetiredNode;
    unsafe {
        (*node_ptr).set_destructor(Some(dealloc_destructor::<T>));
    }
    // SAFETY: AtomNode<T> is #[repr(C)] with RetiredNode at offset 0.
    // birth_epoch is preserved from allocation time.
    // destructor set above to dealloc-only.
    unsafe { guard::retire_raw(node_ptr) };
}
