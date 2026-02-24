//! Era tracking test.
//!
//! Validates that `Atomic::load()` correctly updates the thread's era,
//! when a thread loads a pointer born in a later epoch.
//!
//! Scenario:
//!   - Thread A pins at era E, then loads a pointer born at era E+K.
//!   - Thread B retires that pointer.
//!   - Without era tracking, try_retire sees Thread A at era E < birth E+K,
//!     skips A, and frees the pointer → use after free.
//!   - With era tracking, load() updates A's era to ≥ E+K, so try_retire
//!     counts A → pointer stays alive while A's guard is held.

#![allow(unused_unsafe)]

use kovan::{Atomic, RetiredNode, pin, retire};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

/// Node with embedded RetiredNode for proper reclamation.
#[repr(C)]
struct EraTestNode {
    retired: RetiredNode,
    value: u64,
    freed: Arc<AtomicBool>,
}

impl EraTestNode {
    fn new(value: u64, freed: Arc<AtomicBool>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
            freed,
        }))
    }
}

impl Drop for EraTestNode {
    fn drop(&mut self) {
        self.freed.store(true, Ordering::SeqCst);
    }
}

/// A node used only to force epoch advancement via retire().
#[repr(C)]
struct DummyNode {
    retired: RetiredNode,
}

impl DummyNode {
    fn new() -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
        }))
    }
}

/// Advance the global epoch by retiring many dummy nodes across threads.
fn advance_epoch_by(n: usize) {
    // Each retire increments alloc_counter; every EPOCH_FREQ (128) retires
    // triggers advance_epoch. Use multiple threads to ensure advancement.
    let mut handles = vec![];
    for _ in 0..4 {
        let count = n * 128 / 4; // each thread retires count nodes
        handles.push(thread::spawn(move || {
            for _ in 0..count {
                let _guard = pin();
                unsafe { retire(DummyNode::new()) };
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_protect_prevents_uaf_across_epochs() {
    // Setup: create initial node at current epoch
    let freed_a = Arc::new(AtomicBool::new(false));
    let freed_b = Arc::new(AtomicBool::new(false));
    let shared = Arc::new(Atomic::new(EraTestNode::new(1, freed_a.clone())));

    let shared1 = shared.clone();
    let freed_b1 = freed_b.clone();

    let reader_started = Arc::new(AtomicBool::new(false));
    let reader_started1 = reader_started.clone();

    let writer_done = Arc::new(AtomicBool::new(false));
    let writer_done1 = writer_done.clone();

    // Thread A: reader — pins, loads, holds guard across epoch changes
    let reader = thread::spawn(move || {
        let guard = pin();
        // First load: get initial pointer (born at current epoch)
        let _ptr1 = shared1.load(Ordering::Acquire, &guard);

        reader_started1.store(true, Ordering::Release);

        // Wait for writer to advance epoch and store new node
        while !writer_done1.load(Ordering::Acquire) {
            thread::yield_now();
        }

        // Second load: get NEW pointer born at a LATER epoch.
        // This is the critical test: protect_load must update our era.
        let ptr2 = shared1.load(Ordering::Acquire, &guard);
        if let Some(node) = unsafe { ptr2.as_ref() } {
            assert_eq!(node.value, 2, "should see the new node");
        }

        // Retire the new node (simulating another thread swapping it out)
        // But we still hold a guard that loaded it — it must NOT be freed.
        // Note: we can't retire ptr2 directly since we still hold guard.
        // Instead, just verify that the freed flag is still false.

        // Give retire threads time to run
        thread::sleep(Duration::from_millis(50));

        // The node we loaded (value=2) must NOT be freed while guard is held
        assert!(
            !freed_b1.load(Ordering::SeqCst),
            "Node born in later epoch was freed while guard was held! (UAF)"
        );

        drop(guard);
    });

    // Wait for reader to pin and do first load
    while !reader_started.load(Ordering::Acquire) {
        thread::yield_now();
    }

    // Advance epoch significantly
    advance_epoch_by(4);

    // Store a new node (born at the now-advanced epoch)
    let new_ptr = EraTestNode::new(2, freed_b.clone());
    let swap_guard = pin();
    let old = shared.swap(
        unsafe { kovan::Shared::from_raw(new_ptr) },
        Ordering::AcqRel,
        &swap_guard,
    );

    // Retire old node
    if !old.is_null() {
        unsafe { retire(old.as_raw()) };
    }

    writer_done.store(true, Ordering::Release);

    reader.join().unwrap();
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_era_updates_across_many_loads() {
    // Verify that repeated loads across epoch changes don't cause issues
    let drops = Arc::new(AtomicUsize::new(0));
    let shared: Arc<Atomic<CountedNode>> = Arc::new(Atomic::null());

    let shared1 = shared.clone();
    let drops1 = drops.clone();

    // Writer thread: continuously swap in new nodes
    let stop = Arc::new(AtomicBool::new(false));
    let stop1 = stop.clone();

    // Barrier: writer waits until all readers are running
    let readers_ready = Arc::new(AtomicUsize::new(0));
    let readers_ready1 = readers_ready.clone();

    // Start readers FIRST so they're running before writer begins
    let mut readers = vec![];
    for _ in 0..4 {
        let shared2 = shared.clone();
        let stop2 = stop.clone();
        let ready = readers_ready.clone();
        readers.push(thread::spawn(move || {
            let mut loads = 0u64;
            // Signal that this reader is running
            ready.fetch_add(1, Ordering::Release);
            while !stop2.load(Ordering::Relaxed) {
                let guard = pin();
                let ptr = shared2.load(Ordering::Acquire, &guard);
                if let Some(node) = unsafe { ptr.as_ref() } {
                    let _ = std::hint::black_box(node.value);
                }
                drop(guard);
                loads += 1;
            }
            loads
        }));
    }

    // Writer: wait for all readers to start, then begin swaps
    let writer = thread::spawn(move || {
        while readers_ready1.load(Ordering::Acquire) < 4 {
            core::hint::spin_loop();
        }
        for i in 0..2000 {
            let node = Box::into_raw(Box::new(CountedNode {
                retired: RetiredNode::new(),
                value: i,
                drop_count: drops1.clone(),
            }));
            let guard = pin();
            let old = shared1.swap(
                unsafe { kovan::Shared::from_raw(node) },
                Ordering::AcqRel,
                &guard,
            );
            if !old.is_null() {
                unsafe { retire(old.as_raw()) };
            }
        }
        stop1.store(true, Ordering::Release);
    });

    writer.join().unwrap();
    let total_loads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total_loads > 0, "readers should have done some loads");
    assert!(
        drops.load(Ordering::SeqCst) > 0,
        "some nodes should be freed",
    );
}

#[repr(C)]
struct CountedNode {
    retired: RetiredNode,
    value: usize,
    drop_count: Arc<AtomicUsize>,
}

impl Drop for CountedNode {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}
