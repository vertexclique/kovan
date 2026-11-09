//! A thread's reservation ends with the drop of its outermost guard: a
//! thread with no live guard is counted by no batch, so a parked thread
//! holds nothing back, a held guard holds what it loaded and nothing more,
//! and a nested guard's drop keeps the outer section's protection. Nothing
//! here calls `flush()` on, pins again on, or exits the thread whose
//! reservation is at stake: its drop is the only thing that can free.
//!
//! Each scenario runs on fresh threads and the scenarios are serialized:
//! the slots of every thread in the process decide which batches wait for
//! which traversal.

use kovan::{Atom, Atomic, RetiredNode, flush, pin, retire};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

static LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// How long a test waits for another thread's work before it fails.
const DEADLINE: Duration = Duration::from_secs(60);

/// Waits for `done` with a bounded backoff (a yield, then sleeps that
/// double up to 10 ms); false if `DEADLINE` passes first.
fn eventually(done: impl Fn() -> bool) -> bool {
    let end = Instant::now() + DEADLINE;
    let mut pause = Duration::from_micros(50);
    while !done() {
        if Instant::now() > end {
            return false;
        }
        thread::yield_now();
        thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_millis(10));
    }
    true
}

/// A flag another thread waits for.
fn flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// A thread that pins once, drops its guard and parks until the test lets
/// it go: its slot was active once and its thread never pins again.
fn parked_after_a_pin() -> (Arc<AtomicBool>, thread::JoinHandle<()>) {
    let (parked, go) = (flag(), flag());
    let (p, g) = (Arc::clone(&parked), Arc::clone(&go));
    let t = thread::spawn(move || {
        drop(pin());
        p.store(true, Ordering::SeqCst);
        assert!(eventually(|| g.load(Ordering::SeqCst)));
    });
    assert!(eventually(|| parked.load(Ordering::SeqCst)));
    (go, t)
}

/// A retirable value counted in `live` while it exists.
#[repr(C)]
struct Counted {
    retired: RetiredNode,
    payload: Vec<u8>,
    live: Arc<AtomicUsize>,
}

impl Counted {
    fn boxed(live: &Arc<AtomicUsize>, bytes: usize) -> *mut Counted {
        live.fetch_add(1, Ordering::SeqCst);
        Box::into_raw(Box::new(Counted {
            retired: RetiredNode::new(),
            payload: vec![0xA5; bytes],
            live: Arc::clone(live),
        }))
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A value whose drop is counted, for `Atom`.
struct Val {
    live: Arc<AtomicUsize>,
    tag: u64,
}

impl Val {
    fn new(live: &Arc<AtomicUsize>, tag: u64) -> Self {
        live.fetch_add(1, Ordering::SeqCst);
        Self {
            live: Arc::clone(live),
            tag,
        }
    }
}

impl Drop for Val {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// On a fresh thread: replace the atom's value with a new one (retiring the
/// old) and flush, so the old value's batch is submitted.
fn replace_and_flush(atom: &Arc<Atom<Val>>, live: &Arc<AtomicUsize>, tag: u64) {
    let (a, l) = (Arc::clone(atom), Arc::clone(live));
    thread::spawn(move || {
        a.store(Val::new(&l, tag));
        flush();
    })
    .join()
    .unwrap();
}

/// A pins, drops its guard and parks for good. B allocates values born
/// no later than A's reservation, retires one full batch and one partial
/// batch of them, and flushes. Every destructor runs while A stays parked:
/// A's slot is inactive, so no batch is placed in it.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_parked_thread_holds_nothing_back() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let (go, parked) = parked_after_a_pin();
    let l = Arc::clone(&live);
    thread::spawn(move || {
        // Allocated before any retire of this thread moves the epoch: born
        // no later than the parked thread's reservation.
        let values: Vec<*mut Counted> = (0..64 + 3).map(|_| Counted::boxed(&l, 64)).collect();
        for value in values {
            unsafe { retire(value) };
        }
        flush();
    })
    .join()
    .unwrap();
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 0),
        "{} values waited for a thread parked with no guard",
        live.load(Ordering::SeqCst)
    );
    go.store(true, Ordering::SeqCst);
    parked.join().unwrap();
}

/// A holds a guard over a value it loaded: B replaces and retires the
/// value and flushes, and the value stays allocated. A drops its guard and
/// parks: the value is freed, with no flush, no pin and no exit anywhere
/// after that.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_held_guard_keeps_its_value_and_its_drop_frees_it() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let atom = Arc::new(Atom::new(Val::new(&live, 1)));
    let (loaded, go, done) = (flag(), flag(), flag());
    let reader = {
        let (atom, live) = (Arc::clone(&atom), Arc::clone(&live));
        let (loaded, go, done) = (Arc::clone(&loaded), Arc::clone(&go), Arc::clone(&done));
        thread::spawn(move || {
            let held = atom.load();
            assert_eq!(held.tag, 1);
            loaded.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.load(Ordering::SeqCst)));
            // Still allocated: the count says so, and the read through the
            // guard is what AddressSanitizer reports otherwise.
            assert_eq!(live.load(Ordering::SeqCst), 2);
            assert_eq!(held.tag, 1);
            drop(held);
            // Parked after the drop: the thread's exit is not what frees.
            assert!(eventually(|| done.load(Ordering::SeqCst)));
        })
    };
    assert!(eventually(|| loaded.load(Ordering::SeqCst)));
    replace_and_flush(&atom, &live, 2);
    assert_eq!(
        live.load(Ordering::SeqCst),
        2,
        "the value was freed while the reader held it"
    );
    go.store(true, Ordering::SeqCst);
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 1),
        "the reader's drop did not free the value its slot held back"
    );
    done.store(true, Ordering::SeqCst);
    reader.join().unwrap();
    drop(atom);
    flush();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// Nested guards: the drop of an inner guard leaves the slot active, so the
/// outer section's value stays protected; the outer drop frees it, the
/// thread parked after it.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn an_inner_drop_keeps_the_outer_sections_protection() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let atom = Arc::new(Atom::new(Val::new(&live, 1)));
    let (inner_dropped, go, done) = (flag(), flag(), flag());
    let reader = {
        let (atom, live) = (Arc::clone(&atom), Arc::clone(&live));
        let (inner_dropped, go, done) = (
            Arc::clone(&inner_dropped),
            Arc::clone(&go),
            Arc::clone(&done),
        );
        thread::spawn(move || {
            let outer = atom.load();
            let inner = pin();
            drop(inner);
            inner_dropped.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.load(Ordering::SeqCst)));
            assert_eq!(live.load(Ordering::SeqCst), 2);
            assert_eq!(
                outer.tag, 1,
                "the inner drop released the outer section's value"
            );
            drop(outer);
            assert!(eventually(|| done.load(Ordering::SeqCst)));
        })
    };
    assert!(eventually(|| inner_dropped.load(Ordering::SeqCst)));
    replace_and_flush(&atom, &live, 2);
    assert_eq!(
        live.load(Ordering::SeqCst),
        2,
        "the inner guard's drop left the outer section's value unprotected"
    );
    go.store(true, Ordering::SeqCst);
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 1),
        "the outer drop did not free the value"
    );
    done.store(true, Ordering::SeqCst);
    reader.join().unwrap();
    drop(atom);
    flush();
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

/// A partial batch of three large values born before a reader's
/// reservation, then a flush, then the thread's exit, while the reader holds
/// a guard under which it loaded (its slot publishes an epoch the values'
/// births do not exceed): the batch is placed (padded) into the reader's
/// slot, and the reader's drop frees it. The reader then parks: no flush,
/// pin or exit runs on it.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_partial_batch_of_large_values_is_freed_after_flush_and_exit() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    // Born before the reader's reservation (an allocation may advance the
    // epoch; a reservation covers the nodes born up to the epoch it
    // publishes).
    let values: Vec<usize> = (0..3)
        .map(|_| Counted::boxed(&live, 1 << 20) as usize)
        .collect();
    let (pinned, go, done) = (flag(), flag(), flag());
    let reader = {
        let (pinned, go, done) = (Arc::clone(&pinned), Arc::clone(&go), Arc::clone(&done));
        thread::spawn(move || {
            let guard = pin();
            // A load under the guard publishes the epoch the section
            // reserves; a section that loaded nothing reserves nothing.
            let _ = Atomic::<Counted>::null().load(Ordering::Acquire, &guard);
            pinned.store(true, Ordering::SeqCst);
            assert!(eventually(|| go.load(Ordering::SeqCst)));
            drop(guard);
            assert!(eventually(|| done.load(Ordering::SeqCst)));
        })
    };
    assert!(eventually(|| pinned.load(Ordering::SeqCst)));
    thread::spawn(move || {
        for value in values {
            unsafe { retire(value as *mut Counted) };
        }
        flush();
    })
    .join()
    .unwrap();
    // Placed, not kept: the exiting thread holds nothing, and the values
    // wait for the pinned reader alone.
    assert_eq!(live.load(Ordering::SeqCst), 3);
    go.store(true, Ordering::SeqCst);
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 0),
        "{} large values outlived the reader's drop",
        live.load(Ordering::SeqCst)
    );
    done.store(true, Ordering::SeqCst);
    reader.join().unwrap();
}
