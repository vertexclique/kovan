//! Critical sections that begin after this thread's exit is over (a
//! thread-local destructor that runs after kovan's own), on builds whose
//! handle is a thread-local with a destructor: `HANDLE` no longer answers
//! and no exit runs any more, so the section gets a handle of its own,
//! which leaves the protocol as an exit does when its last guard drops (see
//! `Handle::late`).

use super::{Guard, Handle};
use alloc::boxed::Box;
use core::cell::Cell;

std::thread_local! {
    /// The handle of a critical section that began after this thread's exit
    /// was over, null otherwise. A const thread-local with no destructor: it
    /// answers for the thread's whole life, during every thread-local
    /// destructor too.
    static LATE: Cell<*const Handle> = const { Cell::new(core::ptr::null()) };
}

/// Run `f` on the handle of this thread's late critical section, if one is
/// live (see `late_pin`).
#[cold]
pub(super) fn on_late_handle<R>(f: impl FnOnce(&Handle) -> R) -> Option<R> {
    let handle = LATE.try_with(Cell::get).unwrap_or(core::ptr::null());
    // SAFETY: `late_pin` stores a leaked box here and `late_unpin` frees it
    // only after clearing the pointer, both on this thread.
    (!handle.is_null()).then(|| f(unsafe { &*handle }))
}

/// A pin after this thread's exit was over: `HANDLE` is destroyed and no
/// exit runs any more. The section's handle (made by the outermost such pin,
/// shared by the nested ones) claims an ID and publishes the epoch as any
/// outermost pin does; `late_unpin` ends the section.
#[cold]
pub(super) fn late_pin() -> Guard {
    let mut handle = LATE.try_with(Cell::get).unwrap_or(core::ptr::null());
    if handle.is_null() {
        let late = Box::new(Handle::new());
        late.late.set(true);
        handle = Box::into_raw(late);
        // A const thread-local with no destructor answers for the thread's
        // whole life.
        let _ = LATE.try_with(|cell| cell.set(handle));
    }
    // SAFETY: the box lives until `late_unpin` frees it, after this
    // section's last guard drops.
    unsafe { (*handle).pin() }
}

/// The drop of a guard `late_pin` made: the outermost one ends the section.
/// The handle's drop is its exit (submits what the section retired, parks
/// what cannot be placed, releases the ID), unless the drop's escalated
/// path ran that exit already (`unpin_outermost`), and then finds nothing
/// to do.
#[cold]
pub(super) fn late_unpin() {
    let handle = LATE.try_with(Cell::get).unwrap_or(core::ptr::null());
    if handle.is_null() {
        return;
    }
    // SAFETY: see `on_late_handle`.
    unsafe { (*handle).unpin() };
    // SAFETY: as above.
    if unsafe { (*handle).pin_count.get() } == 0 {
        let _ = LATE.try_with(|cell| cell.set(core::ptr::null()));
        // SAFETY: made by `late_pin` with `Box::into_raw`, no longer
        // reachable through `LATE`, its last guard gone.
        drop(unsafe { Box::from_raw(handle as *mut Handle) });
    }
}
