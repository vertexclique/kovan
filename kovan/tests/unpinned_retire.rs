//! Values retired by a thread that never pinned are freed.
//!
//! The story. Thread A never pins: it only retires values, as a thread
//! does that drops a `Removed` value it got from another thread. Its
//! retires go into its accumulating batch, and A has no thread ID, since
//! only a pin (or a flush or an exit that needs one) claims an ID. A
//! retires fewer values than a batch holds (64), so no retire of its own
//! submits them. Then A either flushes and exits, or just exits.
//!
//! Before the fix, `flush()` returned at once on a thread with no ID while
//! no exited thread had parked anything, and the exit did nothing on a
//! thread with no ID: A's batch was never submitted, its values never
//! freed, whatever other threads did afterwards. With the fix, a flush or
//! an exit on a thread with no ID that holds a batch claims an ID first and
//! submits the batch as any thread does.
//!
//! An integration test, so the library is built as users get it: in the
//! unit-test build a test-only hook claims an ID whenever a submission
//! inserts a value into a slot, so a thread that went through one would have
//! an ID and hide the defect.

use kovan::{RetiredNode, flush, pin, retire};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

/// Fewer than a batch (64 values), so no retire of the thread submits them.
const RETIRED: usize = 10;
/// The pin-and-flush rounds another thread makes afterwards.
const ROUNDS: usize = 100;

static LOCK: Mutex<()> = Mutex::new(());

/// The tests run one at a time: each counts what its own thread retired,
/// and another test's thread pinned meanwhile would hold a reservation the
/// batch had to wait for.
fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// A retirable value counted in `live` while it exists.
#[repr(C)]
struct Counted {
    retired: RetiredNode,
    live: Arc<AtomicUsize>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// On a fresh thread that never pins: retire `RETIRED` counted values,
/// flush if `flushes`, then exit. Returns the count of values still live
/// right after the flush (when it flushes; no other thread holds a
/// reservation, so a flush that submits frees them all at once), and after
/// another thread made `ROUNDS` rounds of pin and flush.
fn retire_without_pinning(flushes: bool) -> (usize, usize) {
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    let after_flush = thread::spawn(move || {
        for _ in 0..RETIRED {
            l.fetch_add(1, Ordering::SeqCst);
            let node = Box::into_raw(Box::new(Counted {
                retired: RetiredNode::new(),
                live: Arc::clone(&l),
            }));
            // SAFETY: `Counted` is `#[repr(C)]` with its `RetiredNode`
            // first, boxed above and retired once.
            unsafe { retire(node) };
        }
        if flushes {
            flush();
        }
        l.load(Ordering::SeqCst)
    })
    .join()
    .unwrap();
    let l = Arc::clone(&live);
    thread::spawn(move || {
        for _ in 0..ROUNDS {
            if l.load(Ordering::SeqCst) == 0 {
                break;
            }
            drop(pin());
            flush();
        }
    })
    .join()
    .unwrap();
    (after_flush, live.load(Ordering::SeqCst))
}

/// A flushes, then exits: the flush itself claims an ID and submits A's
/// batch, so every value is freed before A exits (an exit that submits
/// would free them later, which this checks apart).
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn unpinned_thread_that_flushes_frees_what_it_retired() {
    let _l = lock();
    let (after_flush, left) = retire_without_pinning(true);
    assert_eq!(
        after_flush, 0,
        "{after_flush} values retired by a thread that never pinned were still live after its flush"
    );
    assert_eq!(
        left, 0,
        "{left} values retired by a thread that never pinned, flushed and exited were never freed"
    );
}

/// A just exits: the exit claims an ID and submits A's batch.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded, with deadline waits: not run under Miri
fn unpinned_thread_that_exits_frees_what_it_retired() {
    let _l = lock();
    let (_, left) = retire_without_pinning(false);
    assert_eq!(
        left, 0,
        "{left} values retired by a thread that never pinned and exited were never freed"
    );
}
