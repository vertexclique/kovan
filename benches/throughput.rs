//! Throughput benchmarks for Kovan memory reclamation

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kovan::{pin, retire, Atomic, RetiredNode};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

#[repr(C)]
struct Node {
    retired: RetiredNode,
    value: usize,
}

impl Node {
    fn new(value: usize) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
        }))
    }
}

fn bench_pin_unpin(c: &mut Criterion) {
    let mut group = c.benchmark_group("pin_unpin");
    
    group.bench_function("single_thread", |b| {
        b.iter(|| {
            let _guard = pin();
            black_box(&_guard);
        });
    });
    
    group.finish();
}

fn bench_retire(c: &mut Criterion) {
    let mut group = c.benchmark_group("retire");
    
    for batch_size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    for i in 0..size {
                        let node = Node::new(i);
                        retire(node);
                    }
                });
            },
        );
    }
    
    group.finish();
}

fn bench_atomic_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_load");
    let atomic = Arc::new(Atomic::new(Node::new(42)));
    
    group.bench_function("single_thread", |b| {
        b.iter(|| {
            let guard = pin();
            let ptr = atomic.load(Ordering::Acquire, &guard);
            black_box(ptr);
        });
    });
    
    for threads in [2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent", threads),
            threads,
            |b, &num_threads| {
                b.iter(|| {
                    let atomic = atomic.clone();
                    let handles: Vec<_> = (0..num_threads)
                        .map(|_| {
                            let atomic = atomic.clone();
                            thread::spawn(move || {
                                for _ in 0..1000 {
                                    let guard = pin();
                                    let ptr = atomic.load(Ordering::Acquire, &guard);
                                    black_box(ptr);
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
    
    // Cleanup
    let guard = pin();
    let old = atomic.swap(
        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
        Ordering::Release,
        &guard,
    );
    if !old.is_null() {
        unsafe {
            retire(old.as_raw() as *mut Node);
        }
    }
    
    group.finish();
}

fn bench_atomic_swap(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_swap");
    
    for threads in [1, 2, 4, 8].iter() {
        group.throughput(Throughput::Elements(1000 * *threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            threads,
            |b, &num_threads| {
                b.iter(|| {
                    let atomic = Arc::new(Atomic::new(Node::new(0)));
                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let atomic = atomic.clone();
                            thread::spawn(move || {
                                for i in 0..1000 {
                                    let new_node = Node::new(tid * 1000 + i);
                                    let guard = pin();
                                    let old = atomic.swap(
                                        unsafe { kovan::Shared::from_raw(new_node) },
                                        Ordering::Release,
                                        &guard,
                                    );
                                    if !old.is_null() {
                                        unsafe {
                                            retire(old.as_raw() as *mut Node);
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
                    let old = atomic.swap(
                        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
                        Ordering::Release,
                        &guard,
                    );
                    if !old.is_null() {
                        unsafe {
                            retire(old.as_raw() as *mut Node);
                        }
                    }
                });
            },
        );
    }
    
    group.finish();
}

fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");
    group.sample_size(20); // Reduce sample size for long-running benchmarks
    
    for threads in [4, 8, 16].iter() {
        group.throughput(Throughput::Elements(10000 * *threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            threads,
            |b, &num_threads| {
                b.iter(|| {
                    let atomic = Arc::new(Atomic::new(Node::new(0)));
                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let atomic = atomic.clone();
                            thread::spawn(move || {
                                for i in 0..10000 {
                                    let new_node = Node::new(tid * 10000 + i);
                                    let guard = pin();
                                    let old = atomic.swap(
                                        unsafe { kovan::Shared::from_raw(new_node) },
                                        Ordering::Release,
                                        &guard,
                                    );
                                    if !old.is_null() {
                                        unsafe {
                                            retire(old.as_raw() as *mut Node);
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
                    let old = atomic.swap(
                        unsafe { kovan::Shared::from_raw(std::ptr::null_mut()) },
                        Ordering::Release,
                        &guard,
                    );
                    if !old.is_null() {
                        unsafe {
                            retire(old.as_raw() as *mut Node);
                        }
                    }
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_pin_unpin,
    bench_retire,
    bench_atomic_load,
    bench_atomic_swap,
    bench_contention
);
criterion_main!(benches);
