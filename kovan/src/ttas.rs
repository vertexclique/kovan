//! TTAS (Test-Test-And-Set)

use core::cell::UnsafeCell;
use core::hint::spin_loop;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicBool, Ordering};

/// A TTAS (Test-Test-And-Set)
pub(crate) struct TTas<T: ?Sized> {
    acquired: AtomicBool,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for TTas<T> {}
unsafe impl<T: ?Sized + Send> Sync for TTas<T> {}

impl<T> TTas<T> {
    /// Create a new TTAS wrapping `data`.
    pub(crate) const fn new(data: T) -> Self {
        Self {
            acquired: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    /// Acquire the lock
    #[inline]
    pub(crate) fn lock(&self) -> TTasGuard<'_, T> {
        loop {
            // Test phase: spin on relaxed load (stays in cache)
            while self.acquired.load(Ordering::Relaxed) {
                spin_loop();
            }
            // Test-and-set phase: attempt to acquire
            if !self.acquired.swap(true, Ordering::Acquire) {
                return TTasGuard { ttas: self };
            }
        }
    }
}

/// RAII guard for the TTAS. Releases the lock on drop.
pub(crate) struct TTasGuard<'a, T: ?Sized> {
    ttas: &'a TTas<T>,
}

impl<T: ?Sized> Deref for TTasGuard<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.ttas.data.get() }
    }
}

impl<T: ?Sized> DerefMut for TTasGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.ttas.data.get() }
    }
}

impl<T: ?Sized> Drop for TTasGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.ttas.acquired.store(false, Ordering::Release);
    }
}
