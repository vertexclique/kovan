//! Shuttle model-checked tests for `quiesce()`: racing pins, loads, retires,
//! flushes, other quiesces and thread exits it frees every retired value
//! exactly once and never a value a live guard loaded.
//!
//! Detection records the address of every value freed and forgets it when the
//! address is allocated again: an address found in the record after a load was
//! freed while the loading guard was live (the check never dereferences the
//! pointer), and `created != dropped` at the end shows a double free or a leak.
//!
//! Replay a failure with `shuttle::replay` and the schedule shuttle prints.

#![cfg(feature = "shuttle")]

use kovan::{Atomic, RetiredNode, Shared, flush, pin, quiesce, retire};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

static LOCK: Mutex<()> = Mutex::new(());

/// The tests run one at a time: kovan's thread IDs, epoch and parked batches
/// are process-wide, so another test's flush could adopt and free this
/// test's values before it counts them.
fn serialized() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone)]
struct Tally {
    freed: Arc<Mutex<HashSet<usize>>>,
    created: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    violations: Arc<AtomicUsize>,
}

impl Tally {
    fn new() -> Self {
        Self {
            freed: Arc::new(Mutex::new(HashSet::new())),
            created: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
            violations: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn value(&self) -> *mut Value {
        self.created.fetch_add(1, Ordering::SeqCst);
        let p = Box::into_raw(Box::new(Value {
            retired: RetiredNode::new(),
            tally: self.clone(),
        }));
        self.freed.lock().unwrap().remove(&(p as usize));
        p
    }

    /// Counts a violation if a pointer a guard loaded was freed meanwhile.
    fn check(&self, p: usize) {
        if self.freed.lock().unwrap().contains(&p) {
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn assert_clean(&self) {
        assert_eq!(
            self.violations.load(Ordering::SeqCst),
            0,
            "a value a live guard loaded was freed"
        );
        assert_eq!(
            self.created.load(Ordering::SeqCst),
            self.dropped.load(Ordering::SeqCst),
            "a retired value was freed twice or never"
        );
    }
}

#[repr(C)]
struct Value {
    retired: RetiredNode,
    tally: Tally,
}

impl Drop for Value {
    fn drop(&mut self) {
        let addr = self as *mut Value as usize;
        self.tally.dropped.fetch_add(1, Ordering::SeqCst);
        // A plain mutex, never held across a yield: this runs in thread-exit
        // destructors, where shuttle's own primitives are not available.
        self.tally.freed.lock().unwrap().insert(addr);
    }
}

/// Replaces the shared value and retires the old one.
fn replace(shared: &Atomic<Value>, tally: &Tally) {
    let guard = pin();
    let new = unsafe { Shared::from_raw(tally.value()) };
    let old = shared.swap(new, Ordering::AcqRel, &guard);
    unsafe { retire(old.as_raw()) };
}

/// Loads the shared value in a guard, yields, and checks it was not freed.
fn read(shared: &Atomic<Value>, tally: &Tally) {
    let guard = pin();
    let p = shared.load(Ordering::Acquire, &guard);
    shuttle::thread::yield_now();
    tally.check(p.as_raw() as usize);
    drop(guard);
}

/// Retires the shared value for good and drains every slot this thread owns.
fn finish(shared: &Atomic<Value>) {
    let guard = pin();
    let last = shared.swap(
        unsafe { Shared::from_raw(core::ptr::null_mut()) },
        Ordering::AcqRel,
        &guard,
    );
    unsafe { retire(last.as_raw()) };
    drop(guard);
    for _ in 0..4 {
        flush();
    }
}

/// A reader quiescing between its reads, a writer that replaces the value
/// and quiesces, a flusher, a thread that only quiesces and a thread that
/// retires a value it never published and exits.
fn quiesce_races_everything() {
    let tally = Tally::new();
    let shared = Arc::new(Atomic::new(tally.value()));
    let reader = {
        let (shared, tally) = (Arc::clone(&shared), tally.clone());
        shuttle::thread::spawn(move || {
            for _ in 0..2 {
                read(&shared, &tally);
                quiesce();
            }
        })
    };
    let writer = {
        let (shared, tally) = (Arc::clone(&shared), tally.clone());
        shuttle::thread::spawn(move || {
            replace(&shared, &tally);
            quiesce();
            replace(&shared, &tally);
            quiesce();
        })
    };
    let flusher = shuttle::thread::spawn(|| {
        flush();
        quiesce();
        flush();
    });
    let idler = shuttle::thread::spawn(|| {
        quiesce();
        quiesce();
        quiesce();
    });
    let retirer = {
        let tally = tally.clone();
        shuttle::thread::spawn(move || {
            unsafe { retire(tally.value()) };
            quiesce();
        })
    };
    for t in [reader, writer, flusher, idler, retirer] {
        t.join().unwrap();
    }
    finish(&shared);
    tally.assert_clean();
}

#[test]
fn shuttle_quiesce_races_everything() {
    let _l = serialized();
    shuttle::check_random(quiesce_races_everything, 20_000);
}

/// A reader quiescing while it holds a loaded value (the call must leave its
/// reservation alone) against a writer that replaces the value, retires the
/// old one and flushes.
fn quiesce_under_a_live_guard() {
    let tally = Tally::new();
    let shared = Arc::new(Atomic::new(tally.value()));
    let reader = {
        let (shared, tally) = (Arc::clone(&shared), tally.clone());
        shuttle::thread::spawn(move || {
            let guard = pin();
            let p = shared.load(Ordering::Acquire, &guard);
            quiesce();
            shuttle::thread::yield_now();
            tally.check(p.as_raw() as usize);
            quiesce();
            shuttle::thread::yield_now();
            tally.check(p.as_raw() as usize);
            drop(guard);
            quiesce();
        })
    };
    let writer = {
        let (shared, tally) = (Arc::clone(&shared), tally.clone());
        shuttle::thread::spawn(move || {
            replace(&shared, &tally);
            flush();
        })
    };
    reader.join().unwrap();
    writer.join().unwrap();
    finish(&shared);
    tally.assert_clean();
}

#[test]
fn shuttle_quiesce_under_a_live_guard() {
    let _l = serialized();
    shuttle::check_random(quiesce_under_a_live_guard, 20_000);
}

/// Quiesces on threads that never pin, with parked batches left by exits: the
/// adoption quiesce makes as flush does, racing the exits that park.
fn quiesce_adopts_racing_exits() {
    let tally = Tally::new();
    let holder = {
        let tally = tally.clone();
        shuttle::thread::spawn(move || {
            let guard = pin();
            shuttle::thread::yield_now();
            drop(guard);
            drop(tally);
        })
    };
    let exiting = {
        let tally = tally.clone();
        shuttle::thread::spawn(move || unsafe { retire(tally.value()) })
    };
    let adopting = {
        let tally = tally.clone();
        shuttle::thread::spawn(move || {
            unsafe { retire(tally.value()) };
            quiesce();
            quiesce();
        })
    };
    for t in [holder, exiting, adopting] {
        t.join().unwrap();
    }
    for _ in 0..4 {
        flush();
    }
    tally.assert_clean();
}

#[test]
fn shuttle_quiesce_adopts_racing_exits() {
    let _l = serialized();
    shuttle::check_random(quiesce_adopts_racing_exits, 20_000);
}

/// A quiescer whose one-value batch cannot be placed (readers hold reservations)
/// parks it; the readers' quiesces and the exits that follow adopt and free it.
fn quiesce_hands_a_small_batch_over() {
    let tally = Tally::new();
    let shared = Arc::new(Atomic::new(tally.value()));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (shared, tally) = (Arc::clone(&shared), tally.clone());
            shuttle::thread::spawn(move || {
                read(&shared, &tally);
                quiesce();
                quiesce();
            })
        })
        .collect();
    let quiescer = {
        let tally = tally.clone();
        shuttle::thread::spawn(move || {
            unsafe { retire(tally.value()) };
            quiesce();
        })
    };
    for t in readers {
        t.join().unwrap();
    }
    quiescer.join().unwrap();
    finish(&shared);
    tally.assert_clean();
}

#[test]
fn shuttle_quiesce_hands_a_small_batch_over() {
    let _l = serialized();
    shuttle::check_random(quiesce_hands_a_small_batch_over, 20_000);
}
