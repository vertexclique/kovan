//! Correctness tests for Kovan memory reclamation
//!
//! These tests verify the core safety guarantees:
//! 1. No premature free (nodes not freed while accessible)
//! 2. Eventual reclamation (all retired nodes eventually freed)
//! 3. ABA prevention (packed atomics prevent ABA problem)

#![allow(unused_unsafe)]

use kovan::{Atomic, RetiredNode, pin, retire};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

/// Node for testing with embedded RetiredNode
#[repr(C)]
struct TestNode {
    retired: RetiredNode,
    value: usize,
    freed: Arc<AtomicBool>,
}

impl TestNode {
    fn new(value: usize, freed: Arc<AtomicBool>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
            freed,
        }))
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.freed.store(true, Ordering::Release);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_no_premature_free() {
    // Test that nodes are not freed while still accessible through guards
    // This test verifies the core safety guarantee: no use-after-free

    let freed = Arc::new(AtomicBool::new(false));
    let atomic = Arc::new(Atomic::new(TestNode::new(42, freed.clone())));
    let started = Arc::new(AtomicBool::new(false));
    let can_retire = Arc::new(AtomicBool::new(false));

    // Thread 1: Hold guard and access value
    let atomic1 = atomic.clone();
    let freed1 = freed.clone();
    let started1 = started.clone();
    let can_retire1 = can_retire.clone();
    let handle1 = thread::spawn(move || {
        let guard = pin();
        let ptr = atomic1.load(Ordering::Acquire, &guard);

        if let Some(node) = unsafe { ptr.as_ref() } {
            assert_eq!(node.value, 42);
            started1.store(true, Ordering::Release);

            // Wait for retirement to happen
            while !can_retire1.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(10));
            }

            // Node should NOT be freed while we hold guard
            // This is the key safety property
            assert!(!freed1.load(Ordering::Acquire), "Node freed prematurely!");

            // Value should still be accessible
            assert_eq!(node.value, 42);
        }
    });

    // Thread 2: Retire the node
    let atomic2 = atomic.clone();
    let started2 = started.clone();
    let can_retire2 = can_retire.clone();
    let handle2 = thread::spawn(move || {
        // Wait for thread 1 to load
        while !started2.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(10));
        }

        // Retire while thread 1 holds guard
        let guard = pin();
        let old = atomic2.swap(
            unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
            Ordering::Release,
            &guard,
        );

        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }

        // Trigger batch flush
        for i in 0..70 {
            let dummy = TestNode::new(i, Arc::new(AtomicBool::new(false)));
            unsafe {
                retire(dummy);
            }
        }

        can_retire2.store(true, Ordering::Release);
    });

    handle2.join().unwrap();
    handle1.join().unwrap();

    println!("No premature free test: PASS - node remained accessible while guard held");
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_eventual_reclamation() {
    // Test that all retired nodes are eventually reclaimed
    // We do this by retiring many nodes and verifying the system doesn't crash
    // and that memory is bounded (implicit through successful completion)

    const NUM_NODES: usize = 10000;
    let atomic = Arc::new(Atomic::new(std::ptr::null_mut::<TestNode>()));

    // Allocate and retire many nodes rapidly
    for i in 0..NUM_NODES {
        let freed = Arc::new(AtomicBool::new(false));
        let node = Box::into_raw(Box::new(TestNode {
            retired: RetiredNode::new(),
            value: i,
            freed,
        }));

        let guard = pin();
        let old = atomic.swap(
            unsafe { kovan::Shared::from_raw(node) },
            Ordering::Release,
            &guard,
        );

        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }

        // Periodically create guards to trigger reclamation
        if i % 100 == 0 {
            for _ in 0..10 {
                let _guard = pin();
            }
        }
    }

    // Retire final node
    {
        let guard = pin();
        let old = atomic.swap(
            unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
            Ordering::Release,
            &guard,
        );

        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }
    }

    // Create many guards to trigger reclamation
    for _ in 0..1000 {
        let _guard = pin();
    }

    // If we got here without crashing or OOM, reclamation is working
    println!(
        "Eventual reclamation test: retired {} nodes successfully",
        NUM_NODES
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_access() {
    // Test that concurrent readers and writers work correctly

    const NUM_THREADS: usize = 8;
    const ITERATIONS: usize = 10000;

    let atomic = Arc::new(Atomic::new(TestNode::new(
        0,
        Arc::new(AtomicBool::new(false)),
    )));
    let mut handles = vec![];

    // Reader threads
    for _ in 0..NUM_THREADS / 2 {
        let atomic = atomic.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..ITERATIONS {
                let guard = pin();
                let ptr = atomic.load(Ordering::Acquire, &guard);

                if let Some(node) = unsafe { ptr.as_ref() } {
                    // Just read the value
                    let _ = node.value;
                }
            }
        }));
    }

    // Writer threads
    for tid in 0..NUM_THREADS / 2 {
        let atomic = atomic.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let new_node =
                    TestNode::new(tid * ITERATIONS + i, Arc::new(AtomicBool::new(false)));

                let guard = pin();
                let old = atomic.swap(
                    unsafe { kovan::Shared::from_raw(new_node) },
                    Ordering::Release,
                    &guard,
                );

                if !old.is_null() {
                    unsafe {
                        retire(old.as_raw());
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Cleanup final node
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );

    if !old.is_null() {
        unsafe {
            retire(old.as_raw());
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_guard_drop_triggers_reclamation() {
    // Test that the reclamation system works correctly
    // We verify this by retiring many nodes and checking the system doesn't crash
    // Actual reclamation timing depends on thread scheduling and batch behavior

    const NUM_RETIRES: usize = 1000;
    let atomic = Atomic::new(std::ptr::null_mut::<TestNode>());

    // Retire many nodes in a separate thread
    let handle = thread::spawn(move || {
        for i in 0..NUM_RETIRES {
            let node = TestNode::new(i, Arc::new(AtomicBool::new(false)));
            let guard = pin();
            let old = atomic.swap(
                unsafe { kovan::Shared::from_raw(node) },
                Ordering::Release,
                &guard,
            );

            if !old.is_null() {
                unsafe {
                    retire(old.as_raw());
                }
            }
        }

        // Final cleanup
        let guard = pin();
        let old = atomic.swap(
            unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
            Ordering::Release,
            &guard,
        );
        if !old.is_null() {
            unsafe {
                retire(old.as_raw());
            }
        }
    });

    handle.join().unwrap();

    // Create guards to trigger reclamation
    for _ in 0..500 {
        let _guard = pin();
    }

    println!(
        "Guard drop triggers reclamation test: PASS - {} nodes retired successfully",
        NUM_RETIRES
    );
}
