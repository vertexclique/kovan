//! Comparison benchmarks: Kovan vs Crossbeam-Epoch

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::thread;

// Kovan implementation
mod kovan_bench {
    use super::*;
    use kovan::{pin, retire, Atomic, RetiredNode};
    
    #[repr(C)]
    pub struct Node {
        retired: RetiredNode,
        pub value: usize,
        pub next: AtomicPtr<Node>,
    }
    
    impl Node {
        pub fn new(value: usize) -> *mut Self {
            Box::into_raw(Box::new(Self {
                retired: RetiredNode::new(),
                value,
                next: AtomicPtr::new(std::ptr::null_mut()),
            }))
        }
    }
    
    pub fn bench_treiber_stack(num_threads: usize, ops_per_thread: usize) {
        let stack = Arc::new(Atomic::new(std::ptr::null_mut::<Node>()));
        
        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let stack = stack.clone();
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        // Push
                        let node = Node::new(tid * ops_per_thread + i);
                        loop {
                            let guard = pin();
                            let head = stack.load(Ordering::Acquire, &guard);
                            unsafe {
                                (*node).next.store(head.as_raw() as *mut Node, Ordering::Relaxed);
                            }
                            
                            match stack.compare_exchange(
                                head,
                                unsafe { kovan::Shared::from_raw(node) },
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            ) {
                                Ok(_) => break,
                                Err(_) => continue,
                            }
                        }
                        
                        // Pop
                        loop {
                            let guard = pin();
                            let head = stack.load(Ordering::Acquire, &guard);
                            if head.is_null() {
                                break;
                            }
                            
                            let next = unsafe {
                                let next_ptr = (*head.as_raw()).next.load(Ordering::Relaxed);
                                kovan::Shared::from_raw(next_ptr)
                            };
                            
                            match stack.compare_exchange(
                                head,
                                next,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            ) {
                                Ok(_) => {
                                    unsafe {
                                        retire(head.as_raw() as *mut Node);
                                    }
                                    break;
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Cleanup
        let guard = pin();
        while let Some(head) = unsafe { stack.load(Ordering::Acquire, &guard).as_ref() } {
            let next = unsafe {
                let next_ptr = head.next.load(Ordering::Relaxed);
                kovan::Shared::from_raw(next_ptr)
            };
            
            match stack.compare_exchange(
                unsafe { kovan::Shared::from_raw(head as *const Node as *mut Node) },
                next,
                Ordering::Release,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(old) => unsafe {
                    retire(old.as_raw() as *mut Node);
                },
                Err(_) => continue,
            }
        }
    }
}

// Crossbeam-Epoch implementation
mod crossbeam_bench {
    use super::*;
    use crossbeam_epoch::{self as epoch, Atomic, Owned};
    
    pub struct Node {
        pub value: usize,
        pub next: Atomic<Node>,
    }
    
    impl Node {
        pub fn new(value: usize) -> Self {
            Self {
                value,
                next: Atomic::null(),
            }
        }
    }
    
    pub fn bench_treiber_stack(num_threads: usize, ops_per_thread: usize) {
        let stack = Arc::new(Atomic::null());
        
        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let stack = stack.clone();
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        // Push
                        let mut node = Owned::new(Node::new(tid * ops_per_thread + i));
                        loop {
                            let guard = epoch::pin();
                            let head = stack.load(Ordering::Acquire, &guard);
                            node.next.store(head, Ordering::Relaxed);
                            
                            match stack.compare_exchange(
                                head,
                                node,
                                Ordering::Release,
                                Ordering::Acquire,
                                &guard,
                            ) {
                                Ok(_) => break,
                                Err(e) => {
                                    // Retry with returned node
                                    node = e.new;
                                    continue;
                                }
                            }
                        }
                        
                        // Pop
                        loop {
                            let guard = epoch::pin();
                            let head = stack.load(Ordering::Acquire, &guard);
                            
                            match unsafe { head.as_ref() } {
                                Some(h) => {
                                    let next = h.next.load(Ordering::Relaxed, &guard);
                                    
                                    match stack.compare_exchange(
                                        head,
                                        next,
                                        Ordering::Release,
                                        Ordering::Acquire,
                                        &guard,
                                    ) {
                                        Ok(_) => {
                                            unsafe {
                                                guard.defer_destroy(head);
                                            }
                                            break;
                                        }
                                        Err(_) => continue,
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Cleanup
        let guard = epoch::pin();
        while let Some(head) = unsafe { stack.load(Ordering::Acquire, &guard).as_ref() } {
            let next = head.next.load(Ordering::Relaxed, &guard);
            match stack.compare_exchange(
                unsafe { epoch::Shared::from(head as *const Node) },
                next,
                Ordering::Release,
                Ordering::Acquire,
                &guard,
            ) {
                Ok(old) => unsafe {
                    guard.defer_destroy(old);
                },
                Err(_) => continue,
            }
        }
    }
}

fn bench_treiber_stack_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("treiber_stack");
    group.sample_size(20);
    
    for threads in [1, 2, 4, 8].iter() {
        let ops_per_thread = 5000;
        group.throughput(Throughput::Elements((threads * ops_per_thread * 2) as u64));
        
        group.bench_with_input(
            BenchmarkId::new("kovan", threads),
            threads,
            |b, &num_threads| {
                b.iter(|| {
                    kovan_bench::bench_treiber_stack(num_threads, ops_per_thread);
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("crossbeam", threads),
            threads,
            |b, &num_threads| {
                b.iter(|| {
                    crossbeam_bench::bench_treiber_stack(num_threads, ops_per_thread);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_pin_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("pin_overhead");
    
    group.bench_function("kovan", |b| {
        b.iter(|| {
            let _guard = kovan::pin();
            black_box(&_guard);
        });
    });
    
    group.bench_function("crossbeam", |b| {
        b.iter(|| {
            let _guard = crossbeam_epoch::pin();
            black_box(&_guard);
        });
    });
    
    group.finish();
}

fn bench_read_heavy_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy");
    group.sample_size(20);
    
    for threads in [2, 4, 8].iter() {
        let ops_per_thread = 10000;
        group.throughput(Throughput::Elements((threads * ops_per_thread) as u64));
        
        // Kovan
        group.bench_with_input(
            BenchmarkId::new("kovan", threads),
            threads,
            |b, &num_threads| {
                b.iter(|| {
                    let atomic = Arc::new(kovan::Atomic::new(kovan_bench::Node::new(42)));
                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let atomic = atomic.clone();
                            thread::spawn(move || {
                                for i in 0..ops_per_thread {
                                    if i % 20 == 0 {
                                        // 5% writes
                                        let new_node = kovan_bench::Node::new(tid * ops_per_thread + i);
                                        let guard = kovan::pin();
                                        let old = atomic.swap(
                                            unsafe { kovan::Shared::from_raw(new_node) },
                                            Ordering::Release,
                                            &guard,
                                        );
                                        if !old.is_null() {
                                            unsafe {
                                                kovan::retire(old.as_raw() as *mut kovan_bench::Node);
                                            }
                                        }
                                    } else {
                                        // 95% reads
                                        let guard = kovan::pin();
                                        let ptr = atomic.load(Ordering::Acquire, &guard);
                                        black_box(ptr);
                                    }
                                }
                            })
                        })
                        .collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    
                    // Cleanup
                    let guard = kovan::pin();
                    let old = atomic.swap(
                        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
                        Ordering::Release,
                        &guard,
                    );
                    if !old.is_null() {
                        unsafe {
                            kovan::retire(old.as_raw() as *mut kovan_bench::Node);
                        }
                    }
                });
            },
        );
        
        // Crossbeam
        group.bench_with_input(
            BenchmarkId::new("crossbeam", threads),
            threads,
            |b, &num_threads| {
                b.iter(|| {
                    let atomic = Arc::new(crossbeam_epoch::Atomic::new(crossbeam_bench::Node::new(42)));
                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let atomic = atomic.clone();
                            thread::spawn(move || {
                                for i in 0..ops_per_thread {
                                    if i % 20 == 0 {
                                        // 5% writes
                                        let new_node = crossbeam_epoch::Owned::new(
                                            crossbeam_bench::Node::new(tid * ops_per_thread + i)
                                        );
                                        let guard = crossbeam_epoch::pin();
                                        let old = atomic.swap(new_node, Ordering::Release, &guard);
                                        unsafe {
                                            guard.defer_destroy(old);
                                        }
                                    } else {
                                        // 95% reads
                                        let guard = crossbeam_epoch::pin();
                                        let ptr = atomic.load(Ordering::Acquire, &guard);
                                        black_box(ptr);
                                    }
                                }
                            })
                        })
                        .collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_pin_overhead,
    bench_treiber_stack_comparison,
    bench_read_heavy_workload
);
criterion_main!(benches);
