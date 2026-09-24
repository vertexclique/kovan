//! Test-only stops at the points a resize or a displacement interleaving turns on. A thread
//! armed for a point meets the test there once: it reports its arrival and waits until the test
//! lets it go, so a test replays an interleaving step by step instead of hoping a scheduler
//! produces it. Compiled only under `cfg(test)`; the calls in the map's code vanish from every
//! other build.

extern crate std;

use std::cell::RefCell;
use std::sync::mpsc::{Receiver, SyncSender};

/// A point in the map's code a test can stop one thread at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    /// An insert holds its home bucket's writer guard, has scanned for its key and has not
    /// claimed a slot yet.
    InGuardBeforeClaim,
    /// An insert's write landed and its home guard is released; it has not returned yet.
    AfterLanding,
    /// A resize or a clear found a home bucket's writer guard held by a writer.
    ResizerMetHeldGuard,
    /// An insert or a remove found its home bucket's writer guard held by another writer.
    WriterMetHeldGuard,
    /// A lookup read its home's hop bits and has not read a slot yet.
    LookupReadHops,
    /// A displacement linked the entry it moves at its new slot and published the new slot's
    /// hop bit and the advanced move stamp; the entry is still in its old slot too.
    MoveLinkedTwice,
    /// A displacement unlinked the moved entry from its old slot, whose hop bit is still set.
    MoveUnlinked,
}

/// Where an armed thread stops, and the two channels it meets the test on.
pub(super) struct Stop {
    pub(super) point: Point,
    /// Signalled when the thread reaches `point`.
    pub(super) arrived: SyncSender<()>,
    /// Received from before the thread goes on.
    pub(super) go: Receiver<()>,
}

std::thread_local! {
    static ARMED: RefCell<Option<Stop>> = const { RefCell::new(None) };
}

/// Stop this thread at `stop.point` the next time it gets there.
pub(super) fn arm(stop: Stop) {
    ARMED.with(|armed| *armed.borrow_mut() = Some(stop));
}

/// Called by the map at `point`: a thread armed for it reports its arrival and waits to be let
/// go, once; any other thread, or a thread armed for another point, passes straight through.
pub(super) fn at(point: Point) {
    let stop = ARMED.with(|armed| {
        let mut armed = armed.borrow_mut();
        if armed.as_ref().is_some_and(|stop| stop.point == point) {
            armed.take()
        } else {
            None
        }
    });
    if let Some(stop) = stop {
        stop.arrived
            .send(())
            .expect("the test waits for this thread's arrival");
        stop.go.recv().expect("the test lets this thread go");
    }
}
