use std::ops::{Deref, DerefMut};

// Cache line sizes per architecture.
// x86/x86_64: 64B, aarch64: 128B (Apple M-series / Neoverse), s390x: 256B.
// Fallback: 64B (most common).

// s390 - 256
#[cfg(target_arch = "s390x")]
#[repr(align(256))]
#[derive(Copy, Clone, Default, Debug)]
pub struct CacheAligned<T> {
    pub data: T,
}

// neoverse 128 - Apple M-series
// rest 64
#[cfg(target_arch = "aarch64")]
#[repr(align(128))]
#[derive(Copy, Clone, Default, Debug)]
pub struct CacheAligned<T> {
    pub data: T,
}

// x86_64
#[cfg(not(any(target_arch = "s390x", target_arch = "aarch64")))]
#[repr(align(64))]
#[derive(Copy, Clone, Default, Debug)]
pub struct CacheAligned<T> {
    pub data: T,
}

impl<T> Deref for CacheAligned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

impl<T> CacheAligned<T> {
    pub fn new(t: T) -> Self {
        Self { data: t }
    }
}
