//! What the colocated tests of both maps' conditional writes share: a counter of closure runs, a
//! value whose clone panics on demand, a runner that expects a panic, and a closure that stops
//! until the test lets it go.

extern crate std;

use core::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{Receiver, SyncSender};

/// Counts the runs of the closures a test hands the map.
#[derive(Default)]
pub(crate) struct Runs(Cell<usize>);

impl Runs {
    pub(crate) fn tick(&self) {
        self.0.set(self.0.get() + 1);
    }

    pub(crate) fn take(&self) -> usize {
        self.0.replace(0)
    }
}

std::thread_local! {
    /// Set while a test wants every clone of a [`Fragile`] on this thread to panic.
    static BREAK_CLONES: Cell<bool> = const { Cell::new(false) };
}

/// A value whose clone panics while `BREAK_CLONES` is set.
#[derive(Debug, PartialEq)]
pub(crate) struct Fragile(pub(crate) u64);

impl Clone for Fragile {
    fn clone(&self) -> Self {
        assert!(!BREAK_CLONES.with(Cell::get), "a clone refused");
        Fragile(self.0)
    }
}

/// Run `op` expecting it to panic, with clones broken when `break_clones`.
pub(crate) fn panics(break_clones: bool, op: impl FnOnce()) {
    BREAK_CLONES.with(|b| b.set(break_clones));
    let unwound = catch_unwind(AssertUnwindSafe(op)).is_err();
    BREAK_CLONES.with(|b| b.set(false));
    assert!(unwound, "the call was to panic");
}

/// A closure that stops until the test lets it go, after it reports its arrival.
pub(crate) fn stalled<T>(arrived: SyncSender<()>, go: Receiver<()>, then: T) -> impl FnOnce() -> T {
    move || {
        arrived.send(()).expect("the test waits for the closure");
        go.recv().expect("the test lets the closure go");
        then
    }
}
