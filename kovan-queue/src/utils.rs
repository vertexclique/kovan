use std::ops::{Deref, DerefMut};

/// Contention backoff for a spin/retry loop. Under a normal build, plain
/// `crossbeam_utils::Backoff`'s usual exponential spin-then-yield. Under
/// `shuttle`, `Backoff::snooze`'s eventual `std::thread::yield_now` is a
/// no-op for shuttle's cooperative scheduler (every other task is
/// genuinely parked, not merely de-prioritized) and, worse, doesn't tell
/// the scheduler this thread yielded -- a plain instrumented `.load()` in
/// the retry loop is a valid scheduling point but not a *fair* one, so an
/// unlucky priority draw can re-select the same spinning thread until
/// shuttle's step budget is exhausted ("exceeded max_steps bound", an
/// unfair schedule, not a real bug -- see `array_queue.rs` and
/// `seg_queue.rs`, the two callers). `shuttle::hint::spin_loop` (which
/// calls `shuttle::thread::yield_now`) is the explicit signal PCT/random
/// need to guarantee the other side a turn.
#[inline(always)]
pub(crate) fn backoff_hint(backoff: &crossbeam_utils::Backoff) {
    #[cfg(feature = "shuttle")]
    {
        let _ = backoff;
        shuttle::hint::spin_loop();
    }
    #[cfg(not(feature = "shuttle"))]
    {
        backoff.snooze();
    }
}

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
