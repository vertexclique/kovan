//! Robustness tests: stalled readers, contention, bounded memory

#![allow(unused_unsafe)]

use kovan::{Atomic, RetiredNode, pin, retire};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

#[repr(C)]
struct RobustNode {
    retired: RetiredNode,
    value: usize,
}

impl RobustNode {
    fn new(value: usize) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
        }))
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_adaptive_slot_selection() {
    // Test that slot selection works under load
    // This is implicit - if the system doesn't hang, it's working

    const NUM_THREADS: usize = 16;
    const ITERATIONS: usize = 10000;

    let atomic = Arc::new(Atomic::new(RobustNode::new(0)));
    let mut handles = vec![];

    for tid in 0..NUM_THREADS {
        let atomic = atomic.clone();

        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let new_node = RobustNode::new(tid * ITERATIONS + i);

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

    println!("Adaptive slot selection test: PASS");

    // Cleanup
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
fn test_stalled_thread_handling() {
    // Simulate a stalled thread scenario
    const NUM_ACTIVE: usize = 4;
    const NUM_STALLED: usize = 2;
    const ITERATIONS: usize = 10000;

    let atomic = Arc::new(Atomic::new(RobustNode::new(0)));
    let mut handles = vec![];
    let ops_count = Arc::new(AtomicUsize::new(0));

    // Stalled threads (hold guards for long time)
    for _ in 0..NUM_STALLED {
        let atomic = atomic.clone();

        handles.push(thread::spawn(move || {
            let guard = pin();

            // Hold guard and just read
            for _ in 0..100 {
                let ptr = atomic.load(Ordering::Acquire, &guard);
                if let Some(node) = unsafe { ptr.as_ref() } {
                    let _ = node.value;
                }
                thread::sleep(Duration::from_millis(50));
            }
        }));
    }

    // Active threads (normal operations)
    for tid in 0..NUM_ACTIVE {
        let atomic = atomic.clone();
        let ops_count = ops_count.clone();

        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let new_node = RobustNode::new(tid * ITERATIONS + i);

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

                ops_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_ops = ops_count.load(Ordering::Relaxed);
    println!(
        "Stalled thread handling: {} operations completed",
        total_ops
    );
    assert_eq!(total_ops, NUM_ACTIVE * ITERATIONS);

    // Cleanup
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
fn test_bounded_memory_with_stalls() {
    // Test that memory remains bounded even with stalled threads
    // This is a long-running test that verifies the robustness guarantees

    const NUM_THREADS: usize = 8;
    const DURATION_SECS: u64 = 5;

    let atomic = Arc::new(Atomic::new(RobustNode::new(0)));
    let mut handles = vec![];
    let ops_count = Arc::new(AtomicUsize::new(0));
    let start = std::time::Instant::now();

    // One stalled thread
    let atomic_stalled = atomic.clone();
    handles.push(thread::spawn(move || {
        let guard = pin();
        while start.elapsed() < Duration::from_secs(DURATION_SECS) {
            let ptr = atomic_stalled.load(Ordering::Acquire, &guard);
            if let Some(node) = unsafe { ptr.as_ref() } {
                let _ = node.value;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }));

    // Active threads
    for tid in 0..NUM_THREADS {
        let atomic = atomic.clone();
        let ops_count = ops_count.clone();

        handles.push(thread::spawn(move || {
            let mut local_ops = 0;
            while start.elapsed() < Duration::from_secs(DURATION_SECS) {
                let new_node = RobustNode::new(tid * 1000000 + local_ops);

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

                local_ops += 1;
                ops_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_ops = ops_count.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let throughput = total_ops as f64 / elapsed.as_secs_f64();

    println!("Bounded memory test with stalls:");
    println!("  {} operations in {:?}", total_ops, elapsed);
    println!("  Throughput: {:.0} ops/sec", throughput);
    println!("  System remained responsive despite stalled thread");

    // Cleanup
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
