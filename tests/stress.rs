//! Stress tests for Kovan memory reclamation
//!
//! These tests push the system to its limits to find edge cases

use kovan::{Atomic, RetiredNode, pin, retire};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[repr(C)]
struct StressNode {
    retired: RetiredNode,
    value: usize,
}

impl StressNode {
    fn new(value: usize) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
        }))
    }
}

#[test]
fn test_high_contention() {
    // Many threads hammering the same atomic
    const NUM_THREADS: usize = 16;
    const ITERATIONS: usize = 50000;

    let atomic = Arc::new(Atomic::new(StressNode::new(0)));
    let ops_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    let start = Instant::now();

    for tid in 0..NUM_THREADS {
        let atomic = atomic.clone();
        let ops_count = ops_count.clone();

        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let new_node = StressNode::new(tid * ITERATIONS + i);

                let guard = pin();
                let old = atomic.swap(
                    unsafe { kovan::Shared::from_raw(new_node) },
                    Ordering::Release,
                    &guard,
                );

                if !old.is_null() {
                    unsafe {
                        retire(old.as_raw() as *mut StressNode);
                    }
                }

                ops_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = ops_count.load(Ordering::Relaxed);
    let throughput = total_ops as f64 / elapsed.as_secs_f64();

    println!("High contention test:");
    println!("  {} operations in {:?}", total_ops, elapsed);
    println!("  Throughput: {:.0} ops/sec", throughput);

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw() as *mut StressNode);
        }
    }
}

#[test]
fn test_read_heavy_workload() {
    // 95% reads, 5% writes
    const NUM_THREADS: usize = 8;
    const ITERATIONS: usize = 100000;
    const WRITE_RATIO: usize = 20; // 1 in 20 = 5%

    let atomic = Arc::new(Atomic::new(StressNode::new(0)));
    let mut handles = vec![];

    let start = Instant::now();

    for tid in 0..NUM_THREADS {
        let atomic = atomic.clone();

        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let guard = pin();

                if i % WRITE_RATIO == 0 {
                    // Write operation
                    let new_node = StressNode::new(tid * ITERATIONS + i);
                    let old = atomic.swap(
                        unsafe { kovan::Shared::from_raw(new_node) },
                        Ordering::Release,
                        &guard,
                    );

                    if !old.is_null() {
                        unsafe {
                            retire(old.as_raw() as *mut StressNode);
                        }
                    }
                } else {
                    // Read operation
                    let ptr = atomic.load(Ordering::Acquire, &guard);
                    if let Some(node) = unsafe { ptr.as_ref() } {
                        let _ = node.value;
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = NUM_THREADS * ITERATIONS;
    let throughput = total_ops as f64 / elapsed.as_secs_f64();

    println!("Read-heavy workload (95% reads):");
    println!("  {} operations in {:?}", total_ops, elapsed);
    println!("  Throughput: {:.0} ops/sec", throughput);

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw() as *mut StressNode);
        }
    }
}

#[test]
fn test_oversubscription() {
    // More threads than cores (2x oversubscription, matching paper's methodology)
    let num_cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    // 2x oversubscription as tested in the paper
    // We go beyond that and do 4x oversub here
    let num_threads = num_cores * 4;
    const ITERATIONS: usize = 10000;

    let atomic = Arc::new(Atomic::new(StressNode::new(0)));
    let mut handles = vec![];

    let start = Instant::now();

    for tid in 0..num_threads {
        let atomic = atomic.clone();

        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let new_node = StressNode::new(tid * ITERATIONS + i);

                let guard = pin();
                let old = atomic.swap(
                    unsafe { kovan::Shared::from_raw(new_node) },
                    Ordering::Release,
                    &guard,
                );

                if !old.is_null() {
                    unsafe {
                        retire(old.as_raw() as *mut StressNode);
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = num_threads * ITERATIONS;
    let throughput = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "Oversubscription test ({} threads on {} cores):",
        num_threads, num_cores
    );
    println!("  {} operations in {:?}", total_ops, elapsed);
    println!("  Throughput: {:.0} ops/sec", throughput);

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw() as *mut StressNode);
        }
    }
}

#[test]
fn test_rapid_guard_creation() {
    // Rapidly create and drop guards
    const NUM_THREADS: usize = 8;
    const ITERATIONS: usize = 100000;

    let mut handles = vec![];
    let start = Instant::now();

    for _ in 0..NUM_THREADS {
        handles.push(thread::spawn(move || {
            for _ in 0..ITERATIONS {
                let _guard = pin();
                // Immediately drop
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = NUM_THREADS * ITERATIONS;
    let throughput = total_ops as f64 / elapsed.as_secs_f64();

    println!("Rapid guard creation:");
    println!("  {} guards in {:?}", total_ops, elapsed);
    println!("  Throughput: {:.0} guards/sec", throughput);
}

#[test]
fn test_long_running_guards() {
    // Some threads hold guards for extended periods
    const NUM_LONG: usize = 2;
    const NUM_SHORT: usize = 6;
    const SHORT_ITERATIONS: usize = 10000;

    let atomic = Arc::new(Atomic::new(StressNode::new(0)));
    let mut handles = vec![];
    let done = Arc::new(AtomicBool::new(false));

    // Long-running guard threads
    for _ in 0..NUM_LONG {
        let atomic = atomic.clone();
        let done = done.clone();

        handles.push(thread::spawn(move || {
            let guard = pin();

            while !done.load(Ordering::Relaxed) {
                let ptr = atomic.load(Ordering::Acquire, &guard);
                if let Some(node) = unsafe { ptr.as_ref() } {
                    let _ = node.value;
                }
                thread::sleep(Duration::from_millis(10));
            }
        }));
    }

    // Short-lived operation threads
    for tid in 0..NUM_SHORT {
        let atomic = atomic.clone();

        handles.push(thread::spawn(move || {
            for i in 0..SHORT_ITERATIONS {
                let new_node = StressNode::new(tid * SHORT_ITERATIONS + i);

                let guard = pin();
                let old = atomic.swap(
                    unsafe { kovan::Shared::from_raw(new_node) },
                    Ordering::Release,
                    &guard,
                );

                if !old.is_null() {
                    unsafe {
                        retire(old.as_raw() as *mut StressNode);
                    }
                }
            }
        }));
    }

    // Wait for short threads
    for handle in handles.drain(NUM_LONG..) {
        handle.join().unwrap();
    }

    // Signal long threads to stop
    done.store(true, Ordering::Relaxed);

    // Wait for long threads
    for handle in handles {
        handle.join().unwrap();
    }

    println!("Long-running guards test: PASS");

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw() as *mut StressNode);
        }
    }
}

#[test]
fn test_burst_workload() {
    // Alternating periods of high and low activity
    const NUM_THREADS: usize = 8;
    const BURSTS: usize = 10;
    const OPS_PER_BURST: usize = 10000;

    let atomic = Arc::new(Atomic::new(StressNode::new(0)));

    for burst in 0..BURSTS {
        let mut handles = vec![];

        for tid in 0..NUM_THREADS {
            let atomic = atomic.clone();

            handles.push(thread::spawn(move || {
                for i in 0..OPS_PER_BURST {
                    let new_node = StressNode::new(
                        burst * NUM_THREADS * OPS_PER_BURST + tid * OPS_PER_BURST + i,
                    );

                    let guard = pin();
                    let old = atomic.swap(
                        unsafe { kovan::Shared::from_raw(new_node) },
                        Ordering::Release,
                        &guard,
                    );

                    if !old.is_null() {
                        unsafe {
                            retire(old.as_raw() as *mut StressNode);
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Quiet period
        thread::sleep(Duration::from_millis(100));
    }

    println!("Burst workload test: PASS");

    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw() as *mut StressNode);
        }
    }
}
