//! The atomics both maps keep of their own (resize latches, counts, hopscotch control words) and
//! the hint a spinning writer gives while it waits for another thread.
//!
//! Under the `shuttle` feature every one of them is shuttle's instrumented atomic, so shuttle's
//! scheduler can preempt at each load, store and read-modify-write of them, as it can at every
//! pointer operation of `kovan::Atomic` under the same feature. Nothing here costs anything in a
//! build without the feature: the names are `core`'s atomics.

#[cfg(not(feature = "shuttle"))]
pub(crate) use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
#[cfg(feature = "shuttle")]
pub(crate) use shuttle::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

/// The pause of a thread spinning on a condition another thread clears (a resize in flight, a
/// held home guard).
///
/// Under shuttle this yields (`shuttle::hint::spin_loop` calls `shuttle::thread::yield_now`),
/// which PCT treats as a change point: without it a spinning thread can keep the higher priority
/// forever and starve the thread it waits for, running out shuttle's step budget on a schedule
/// that is unfair, not wrong.
#[inline(always)]
pub(crate) fn spin_hint() {
    #[cfg(feature = "shuttle")]
    {
        shuttle::hint::spin_loop();
    }
    #[cfg(not(feature = "shuttle"))]
    {
        core::hint::spin_loop();
    }
}
