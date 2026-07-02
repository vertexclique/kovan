//! Cache-line padding to prevent false sharing between independently hot
//! fields.

use core::ops::{Deref, DerefMut};

/// Wraps `T`, padding and aligning it to the target's effective cache line
/// size so a value never shares a line with an unrelated, independently
/// accessed neighbor (false sharing).
///
/// Per-architecture sizing, chosen deliberately rather than a single global
/// constant:
///
/// - `x86_64` -> 128 bytes. Intel's L2 adjacent-line prefetcher fetches
///   64-byte lines in pairs, so a plain 64-byte pad can still false-share
///   with whatever the prefetcher pulls in alongside it.
/// - `aarch64` -> 128 bytes. Apple M-series and other modern aarch64 cores
///   (e.g. Neoverse) use a 128-byte L2 cache line.
/// - every other target (`riscv64`, `powerpc64`, ...) -> 64 bytes, the
///   common cache line size where neither of the above applies.
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(128)))]
#[cfg_attr(
    not(any(target_arch = "x86_64", target_arch = "aarch64")),
    repr(align(64))
)]
#[derive(Default, Clone, Copy, Debug)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    /// Pads `value` out to a full cache line.
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Unwraps the padded value.
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> From<T> for CachePadded<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

// Compile-time invariant: the wrapper must actually reach the cache line
// size on every target it builds for, or the whole point of the type is
// silently defeated.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
const _: () = assert!(align_of::<CachePadded<u8>>() == 128);
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const _: () = assert!(align_of::<CachePadded<u8>>() == 64);
const _: () = assert!(size_of::<CachePadded<u8>>() == align_of::<CachePadded<u8>>());
