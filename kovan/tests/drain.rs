//! A thread's reservation slot takes one node of every batch retired while
//! the slot's published epoch covers the batch, and a batch is freed only
//! after every slot that took one of its nodes has traversed its list. The
//! owner traverses its list at critical-section boundaries.
//!
//! These tests pin that a thread which keeps operating traverses its list
//! as it goes, for every shape of critical section: the number of retired
//! values still alive stays under a constant that does not depend on how
//! many were retired, with no `flush()` and no thread exit needed.
//!
//! Each scenario runs on a fresh thread (its own slot, released at exit)
//! and the scenarios are serialized: the slots of every thread in the
//! process decide which batches wait for which traversal, so a concurrent
//! scenario would perturb the counts.

use kovan::{Atomic, RetiredNode, Shared, flush, pin, retire};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

/// Retired values a thread that keeps operating may leave alive at any
/// point. The protocol holds, per thread: the batch being filled (under 64
/// nodes), the batches retired since the last epoch advance (two per 128
/// retires), and the traversed batches cached for freeing (at most 12
/// traversals' worth, two batches each in these loops): under 2048. The
/// bound is twice that, so it pins "does not grow with the retire count"
/// without pinning those constants.
const LIVE_BOUND: usize = 4096;

/// Operations per scenario: many times the bound, so a list that is never
/// traversed fails the bound long before the loop ends.
const ITERS: usize = 100_000;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn test_lock() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[repr(C)]
struct Node {
    retired: RetiredNode,
    live: Arc<AtomicUsize>,
}

impl Node {
    fn boxed(live: &Arc<AtomicUsize>) -> *mut Node {
        live.fetch_add(1, Ordering::SeqCst);
        Box::into_raw(Box::new(Node {
            retired: RetiredNode::new(),
            live: Arc::clone(live),
        }))
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One operation of the shape under test: in one critical section, replace
/// the head, retire the old head, then load the head through a protected
/// load. The retire can advance the global epoch, and the load after it
/// then raises this thread's published epoch inside the section.
fn replace_retire_load(head: &Atomic<Node>, live: &Arc<AtomicUsize>) {
    let guard = pin();
    let new = unsafe { Shared::from_raw(Node::boxed(live)) };
    let old = head.swap(new, Ordering::AcqRel, &guard);
    unsafe { retire(old.as_raw()) };
    let _ = head.load(Ordering::Acquire, &guard);
}

/// The same operation with the load before the retire.
fn load_replace_retire(head: &Atomic<Node>, live: &Arc<AtomicUsize>) {
    let guard = pin();
    let _ = head.load(Ordering::Acquire, &guard);
    let new = unsafe { Shared::from_raw(Node::boxed(live)) };
    let old = head.swap(new, Ordering::AcqRel, &guard);
    unsafe { retire(old.as_raw()) };
}

fn retire_head(head: &Atomic<Node>) {
    let guard = pin();
    let null = unsafe { Shared::from_raw(core::ptr::null_mut()) };
    let old = head.swap(null, Ordering::AcqRel, &guard);
    unsafe { retire(old.as_raw()) };
}

fn assert_bounded(live: &AtomicUsize, what: &str, done: usize) {
    let n = live.load(Ordering::SeqCst);
    assert!(
        n <= LIVE_BOUND,
        "{what}: {n} retired values still alive after {done} operations (bound {LIVE_BOUND})"
    );
}

/// Runs `scenario` on a fresh thread, then checks that the thread's exit
/// freed everything it retired.
fn on_fresh_thread(scenario: impl FnOnce(&Arc<AtomicUsize>) + Send + 'static) {
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || scenario(&l)).join().unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "thread exit left values alive"
    );
}

/// The load after the retire raises the published epoch inside the section
/// each time the retire advanced the global epoch; the slot list must still
/// be traversed as the loop goes.
#[test]
#[cfg_attr(miri, ignore)] // 100k operations: far too slow under Miri
fn retire_then_load_frees_as_it_goes() {
    let _l = test_lock();
    on_fresh_thread(|live| {
        let head = Atomic::new(Node::boxed(live));
        for i in 0..ITERS {
            replace_retire_load(&head, live);
            assert_bounded(live, "retire then load", i + 1);
        }
        retire_head(&head);
    });
}

/// Control: the load before the retire.
#[test]
#[cfg_attr(miri, ignore)] // 100k operations: far too slow under Miri
fn load_then_retire_frees_as_it_goes() {
    let _l = test_lock();
    on_fresh_thread(|live| {
        let head = Atomic::new(Node::boxed(live));
        for i in 0..ITERS {
            load_replace_retire(&head, live);
            assert_bounded(live, "load then retire", i + 1);
        }
        retire_head(&head);
    });
}

/// Operations nested in one long-lived guard: nothing retired during the
/// long section can be freed before it ends (the outer guard may hold any
/// pointer loaded under it). Once it ends, the next outermost pins must
/// traverse the list the section accumulated, and ordinary operations after
/// it must bring the live count back under the bound.
#[test]
#[cfg_attr(miri, ignore)] // 100k operations: far too slow under Miri
fn long_section_is_drained_after_it_ends() {
    let _l = test_lock();
    on_fresh_thread(|live| {
        let head = Atomic::new(Node::boxed(live));
        let outer = pin();
        for _ in 0..ITERS / 4 {
            replace_retire_load(&head, live);
        }
        drop(outer);
        // Thirteen epoch advances pass through the free-list cache.
        for _ in 0..16 * 128 {
            replace_retire_load(&head, live);
        }
        assert_bounded(live, "after a long section", ITERS / 4 + 16 * 128);
        retire_head(&head);
    });
}

/// A thread that retires and then parks has flushed first, as the idle
/// contract asks: everything it retired is freed while it stays parked,
/// through the traversals of the threads that keep operating. (They load
/// before they retire, so this pins the parked thread's share alone.)
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded and 100k operations
fn flushed_parked_thread_holds_nothing_back() {
    let _l = test_lock();
    on_fresh_thread(|live| {
        let head = Arc::new(Atomic::new(Node::boxed(live)));
        let (parked_tx, parked_rx) = mpsc::channel();
        let (wake_tx, wake_rx) = mpsc::channel::<()>();
        let parked = {
            let head = Arc::clone(&head);
            let live = Arc::clone(live);
            thread::spawn(move || {
                for _ in 0..1000 {
                    replace_retire_load(&head, &live);
                }
                flush();
                parked_tx.send(()).unwrap();
                wake_rx.recv().unwrap();
            })
        };
        parked_rx.recv().unwrap();
        for i in 0..ITERS {
            load_replace_retire(&head, live);
            assert_bounded(live, "beside a flushed parked thread", i + 1);
        }
        wake_tx.send(()).unwrap();
        parked.join().unwrap();
        retire_head(&head);
    });
}

/// A thread parked without flushing keeps its own slot published, so it
/// holds back its unsubmitted batch and the batches that hold a node born
/// before it parked. Nothing born after that is held back: the threads
/// that keep operating free their own garbage as they go.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded and 100k operations
fn unflushed_parked_thread_holds_back_only_older_garbage() {
    let _l = test_lock();
    on_fresh_thread(|live| {
        let head = Arc::new(Atomic::new(Node::boxed(live)));
        let (parked_tx, parked_rx) = mpsc::channel();
        let (wake_tx, wake_rx) = mpsc::channel::<()>();
        let parked = {
            let head = Arc::clone(&head);
            let live = Arc::clone(live);
            thread::spawn(move || {
                for _ in 0..1000 {
                    replace_retire_load(&head, &live);
                }
                parked_tx.send(()).unwrap();
                wake_rx.recv().unwrap();
            })
        };
        parked_rx.recv().unwrap();
        // What the parked thread holds back: at most the 1000 values it
        // retired, plus the head.
        for i in 0..ITERS {
            load_replace_retire(&head, live);
            let n = live.load(Ordering::SeqCst);
            assert!(
                n <= LIVE_BOUND + 1001,
                "beside an unflushed parked thread: {n} retired values still alive after {} operations",
                i + 1
            );
        }
        wake_tx.send(()).unwrap();
        parked.join().unwrap();
        retire_head(&head);
    });
}

/// Retired and counted like `Node`, and it records its own drop, so a test
/// can check that one particular value is still alive before reading it.
#[repr(C)]
struct Canary {
    retired: RetiredNode,
    value: u64,
    dropped: Arc<AtomicBool>,
}

impl Drop for Canary {
    fn drop(&mut self) {
        self.value = 0;
        self.dropped.store(true, Ordering::SeqCst);
    }
}

const CANARY: u64 = 0x5eed_cafe_f00d_d00d;

/// A section that loads a pointer first and keeps using it (as a map
/// operation does with its table) while its own retires advance the epoch
/// and its later protected loads, some under nested guards, raise its
/// reservation: the pointer stays valid for the whole section, because the
/// slot list is only traversed where no guard of the thread is live. Once
/// the section ends, the next outermost pins traverse, and the value is
/// freed as the thread keeps operating.
#[test]
#[cfg_attr(miri, ignore)] // thousands of operations: too slow under Miri
fn section_keeps_what_it_loaded_first() {
    let _l = test_lock();
    on_fresh_thread(|live| {
        let dropped = Arc::new(AtomicBool::new(false));
        let first = Box::into_raw(Box::new(Canary {
            retired: RetiredNode::new(),
            value: CANARY,
            dropped: Arc::clone(&dropped),
        }));
        let shared = Atomic::new(first);
        let head = Atomic::new(Node::boxed(live));

        let guard = pin();
        let loaded = shared.load(Ordering::Acquire, &guard);
        assert_eq!(loaded.as_raw(), first);
        // Unlink and retire the value this section still reads, then retire
        // enough behind it to submit its batch and advance the epoch many
        // times, with protected loads after each retire (the raise) and
        // nested sections in between. Enough advances that a traversal
        // inside the section would also have freed the traversed batches
        // (the free-list cache is freed every thirteenth traversal).
        let null = unsafe { Shared::from_raw(core::ptr::null_mut()) };
        let unlinked = shared.swap(null, Ordering::AcqRel, &guard);
        unsafe { retire(unlinked.as_raw()) };
        for i in 0..32 * 128 {
            if i % 2 == 0 {
                replace_retire_load(&head, live);
            } else {
                let new = unsafe { Shared::from_raw(Node::boxed(live)) };
                let old = head.swap(new, Ordering::AcqRel, &guard);
                unsafe { retire(old.as_raw()) };
                let _ = head.load(Ordering::Acquire, &guard);
            }
        }
        assert!(
            !dropped.load(Ordering::SeqCst),
            "a value loaded in a live section was freed"
        );
        assert_eq!(unsafe { (*loaded.as_raw()).value }, CANARY);
        drop(guard);

        // The section is over: ordinary operations free it.
        for _ in 0..16 * 128 {
            replace_retire_load(&head, live);
        }
        assert!(
            dropped.load(Ordering::SeqCst),
            "a value retired in an ended section was never freed"
        );
        retire_head(&head);
    });
}
