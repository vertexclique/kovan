//! Atomic pointer types with memory reclamation
//!
//! This module provides `Atomic<T>` and `Shared<'g, T>` for safe concurrent
//! access to heap-allocated data with automatic memory reclamation.

use crate::guard::Guard;
use core::marker::PhantomData;
use core::ptr;
use core::sync::atomic::{AtomicUsize, Ordering};

/// A pointer to a heap-allocated value with atomic operations.
///
/// This type provides atomic load, store, and compare-exchange operations
/// on pointers to `T`. Memory reclamation is handled automatically through
/// the guard-based protocol.
///
/// # Examples
///
/// ```ignore
/// use kovan::{Atomic, pin};
/// use std::sync::atomic::Ordering;
///
/// let atomic = Atomic::new(Box::into_raw(Box::new(42)));
/// let guard = pin();
/// let ptr = atomic.load(Ordering::Acquire, &guard);
/// ```
pub struct Atomic<T> {
    data: AtomicUsize,
    _marker: PhantomData<*mut T>,
}

unsafe impl<T: Send + Sync> Send for Atomic<T> {}
unsafe impl<T: Send + Sync> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    /// Creates a new atomic pointer.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use kovan::Atomic;
    ///
    /// let atomic = Atomic::new(std::ptr::null_mut());
    /// ```
    #[inline]
    pub fn new(ptr: *mut T) -> Self {
        Self {
            data: AtomicUsize::new(ptr as usize),
            _marker: PhantomData,
        }
    }

    /// Creates a null atomic pointer.
    #[inline]
    pub fn null() -> Self {
        Self::new(ptr::null_mut())
    }

    /// Loads a pointer from the atomic.
    ///
    /// This operation has zero overhead - it's just a single atomic load.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use kovan::{Atomic, pin};
    /// use std::sync::atomic::Ordering;
    ///
    /// let atomic = Atomic::new(Box::into_raw(Box::new(42)));
    /// let guard = pin();
    /// let ptr = atomic.load(Ordering::Acquire, &guard);
    /// ```
    #[inline]
    pub fn load<'g>(&self, order: Ordering, _guard: &'g Guard) -> Shared<'g, T> {
        let raw = self.data.load(order);
        Shared {
            data: raw as *mut T,
            _marker: PhantomData,
        }
    }

    /// Stores a pointer into the atomic.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use kovan::{Atomic, Shared};
    /// use std::sync::atomic::Ordering;
    ///
    /// let atomic = Atomic::new(std::ptr::null_mut());
    /// let ptr = Box::into_raw(Box::new(42));
    /// atomic.store(Shared::from_raw(ptr), Ordering::Release);
    /// ```
    #[inline]
    pub fn store(&self, ptr: Shared<T>, order: Ordering) {
        self.data.store(ptr.data as usize, order);
    }

    /// Compares and exchanges the pointer.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use kovan::{Atomic, Shared, pin};
    /// use std::sync::atomic::Ordering;
    ///
    /// let atomic = Atomic::new(Box::into_raw(Box::new(42)));
    /// let guard = pin();
    /// let current = atomic.load(Ordering::Acquire, &guard);
    /// let new = Box::into_raw(Box::new(43));
    /// atomic.compare_exchange(
    ///     current,
    ///     Shared::from_raw(new),
    ///     Ordering::AcqRel,
    ///     Ordering::Acquire,
    ///     &guard
    /// );
    /// ```
    #[inline]
    pub fn compare_exchange<'g>(
        &self,
        current: Shared<T>,
        new: Shared<T>,
        success: Ordering,
        failure: Ordering,
        _guard: &'g Guard,
    ) -> Result<Shared<'g, T>, Shared<'g, T>> {
        match self
            .data
            .compare_exchange(current.data as usize, new.data as usize, success, failure)
        {
            Ok(prev) => Ok(Shared {
                data: prev as *mut T,
                _marker: PhantomData,
            }),
            Err(prev) => Err(Shared {
                data: prev as *mut T,
                _marker: PhantomData,
            }),
        }
    }

    /// Compares and exchanges the pointer (weak version).
    ///
    /// This version may spuriously fail even when the comparison succeeds.
    #[inline]
    pub fn compare_exchange_weak<'g>(
        &self,
        current: Shared<T>,
        new: Shared<T>,
        success: Ordering,
        failure: Ordering,
        _guard: &'g Guard,
    ) -> Result<Shared<'g, T>, Shared<'g, T>> {
        match self.data.compare_exchange_weak(
            current.data as usize,
            new.data as usize,
            success,
            failure,
        ) {
            Ok(prev) => Ok(Shared {
                data: prev as *mut T,
                _marker: PhantomData,
            }),
            Err(prev) => Err(Shared {
                data: prev as *mut T,
                _marker: PhantomData,
            }),
        }
    }

    /// Swaps the pointer with a new value.
    #[inline]
    pub fn swap<'g>(&self, new: Shared<T>, order: Ordering, _guard: &'g Guard) -> Shared<'g, T> {
        let prev = self.data.swap(new.data as usize, order);
        Shared {
            data: prev as *mut T,
            _marker: PhantomData,
        }
    }
}

impl<T> Default for Atomic<T> {
    fn default() -> Self {
        Self::null()
    }
}

/// A pointer to a heap-allocated value protected by a guard.
///
/// This type represents a pointer that is guaranteed to remain valid
/// for the lifetime of the guard. It cannot outlive the guard.
///
/// # Safety
///
/// The pointer is only valid while the guard is alive. Dereferencing
/// the pointer after the guard is dropped is undefined behavior.
pub struct Shared<'g, T> {
    data: *mut T,
    _marker: PhantomData<(&'g Guard, *mut T)>,
}

impl<'g, T> Shared<'g, T> {
    /// Creates a shared pointer from a raw pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer is valid and will remain valid
    /// for the lifetime of the guard.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut T) -> Self {
        Self {
            data: ptr,
            _marker: PhantomData,
        }
    }

    /// Returns the raw pointer.
    #[inline]
    pub fn as_raw(&self) -> *mut T {
        self.data
    }

    /// Returns true if the pointer is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.data.is_null()
    }

    /// Converts to an optional reference.
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer is properly aligned and points
    /// to a valid value of type `T`.
    #[inline]
    pub unsafe fn as_ref(&self) -> Option<&'g T> {
        if self.is_null() {
            None
        } else {
            // SAFETY: Caller guarantees pointer validity
            unsafe { Some(&*self.data) }
        }
    }

    /// Converts to a reference without checking for null.
    ///
    /// # Safety
    ///
    /// The pointer must not be null and must point to a valid value.
    #[inline]
    pub unsafe fn deref(&self) -> &'g T {
        // SAFETY: Caller guarantees pointer is non-null and valid
        unsafe { &*self.data }
    }

    /// Converts to a mutable reference.
    ///
    /// # Safety
    ///
    /// The caller must ensure exclusive access to the value.
    #[inline]
    pub unsafe fn as_mut(&mut self) -> Option<&'g mut T> {
        if self.is_null() {
            None
        } else {
            // SAFETY: Caller guarantees exclusive access
            unsafe { Some(&mut *self.data) }
        }
    }
}

impl<'g, T> Clone for Shared<'g, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            _marker: PhantomData,
        }
    }
}

impl<'g, T> Copy for Shared<'g, T> {}

impl<'g, T> PartialEq for Shared<'g, T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<'g, T> Eq for Shared<'g, T> {}

impl<'g, T> core::fmt::Debug for Shared<'g, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Shared({:p})", self.data)
    }
}
