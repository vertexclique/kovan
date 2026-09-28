//! Test builds only: named steps of the reclamation protocol where a unit
//! test runs a hook on the thread that reaches them.
//!
//! A hook is armed for one step and one thread ID and runs when the thread
//! holding that ID reaches the step. It holds the thread there until the
//! test lets it go (a thread stalled at that step for as long as the
//! operations the test runs meanwhile take), or acts at that instant the way
//! another thread could. A hook returns whether it stays armed, so a test
//! can count how many times a loop passes its step. Outside test builds no
//! step exists: every call site is compiled out.

use core::sync::atomic::{AtomicUsize, Ordering};
use std::boxed::Box;
use std::sync::{Mutex, MutexGuard};
use std::vec::Vec;

/// The steps a test can hold a thread at.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Step {
    /// An exiting thread parking orphans has swapped its ID's orphan word to
    /// null and not yet stored the joined chain.
    OrphanPark,
    /// A flushing thread has taken a chain of orphans and not yet merged it.
    OrphanAdopt,
    /// A pin's slow path has published its help request (result pending)
    /// and not yet entered its loop.
    SlowPending,
    /// A thread detaching a slot list at the end of a slow-path cycle has
    /// moved the era seqno odd (the slot closed to new batches) and not yet
    /// emptied the list.
    EraClosed,
    /// A thread detaching a slot list at the end of a slow-path cycle has
    /// read the slot and not yet tried to empty it (one pass of the
    /// detach's loop).
    DetachList,
    /// A protected load's convergence pass has loaded the pointer and not
    /// yet read the epoch again.
    LoadAttempt,
    /// An outermost pin's transition has made one attempt (traversed and
    /// published) and not yet read the epoch again.
    TransitionAttempt,
    /// A pin's slow path is about to make one pass of its loop.
    SlowPass,
    /// A retire's insert has exchanged its node into a slot and not yet
    /// linked the list it displaced behind it.
    InsertExchanged,
    /// A thread holds a slot list it captured and has not yet walked it.
    Traverse,
}

type Hook = Box<dyn FnMut() -> bool + Send>;

/// Hooks armed and not yet spent: lets `at` return with one load in the
/// common case.
static ARMED: AtomicUsize = AtomicUsize::new(0);
static HOOKS: Mutex<Vec<(Step, usize, Hook)>> = Mutex::new(Vec::new());

fn hooks() -> MutexGuard<'static, Vec<(Step, usize, Hook)>> {
    HOOKS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Arm `hook` for the thread holding `tid` at `step`. It runs each time
/// that thread reaches the step, until it returns `false`.
pub(crate) fn arm(step: Step, tid: usize, hook: impl FnMut() -> bool + Send + 'static) {
    hooks().push((step, tid, Box::new(hook)));
    ARMED.fetch_add(1, Ordering::SeqCst);
}

/// Drop the hooks armed for the thread holding `tid` at `step` that have
/// not ended themselves.
pub(crate) fn disarm(step: Step, tid: usize) {
    let mut hooks = hooks();
    let before = hooks.len();
    hooks.retain(|(s, t, _)| !(*s == step && *t == tid));
    ARMED.fetch_sub(before - hooks.len(), Ordering::SeqCst);
}

/// The thread holding `tid` reached `step`: run the hook armed for it, if
/// any. The hook runs outside the registry's lock, so it may block, and
/// other threads reach their own steps meanwhile.
pub(crate) fn at(step: Step, tid: usize) {
    if ARMED.load(Ordering::SeqCst) == 0 {
        return;
    }
    let found = {
        let mut hooks = hooks();
        hooks
            .iter()
            .position(|(s, t, _)| *s == step && *t == tid)
            .map(|i| hooks.swap_remove(i))
    };
    if let Some((s, t, mut hook)) = found {
        if hook() {
            hooks().push((s, t, hook));
        } else {
            ARMED.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
