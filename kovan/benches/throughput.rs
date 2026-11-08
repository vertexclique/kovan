//! Throughput benchmarks for Kovan memory reclamation

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use kovan::{Atomic, RetiredNode, pin, retire};
use std::sync::Arc;
use std::sync::atomic::Ordering;
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

    // A nested pin and its drop, inside an outer guard: counter updates only.
    group.bench_function("nested", |b| {
        let _outer = pin();
        b.iter(|| {
            let _guard = pin();
            black_box(&_guard);
        });
    });

    group.finish();
}

/// Threads that pinned once, dropped the guard and park for the whole
/// measurement: slots that were active once and whose threads hold no guard.
struct Parked {
    stop: Arc<std::sync::atomic::AtomicBool>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Parked {
    fn spawn(n: usize) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ready = Arc::new(std::sync::Barrier::new(n + 1));
        let threads = (0..n)
            .map(|_| {
                let (stop, ready) = (Arc::clone(&stop), Arc::clone(&ready));
                thread::spawn(move || {
                    drop(pin());
                    ready.wait();
                    while !stop.load(Ordering::Relaxed) {
                        thread::park_timeout(std::time::Duration::from_millis(50));
                    }
                })
            })
            .collect();
        ready.wait();
        Self { stop, threads }
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            t.thread().unpark();
            t.join().unwrap();
        }
    }
}

/// Retire throughput with `active` threads each retiring 1000 values per
/// iteration under a guard, beside `parked` threads that pinned once and
/// park: the parked slots take a node of every batch while they stay active
/// (0.2.3), or none (0.2.4).
fn bench_retire_threads(c: &mut Criterion) {
    let mut group = c.benchmark_group("retire_threads");
    group.sample_size(20);
    for (active, parked) in [(1, 0), (4, 0), (16, 0), (1, 36), (4, 36), (16, 36)] {
        let parked_threads = Parked::spawn(parked);
        group.throughput(Throughput::Elements(1000 * active as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("active_{active}"), format!("parked_{parked}")),
            &active,
            |b, &active| {
                b.iter(|| {
                    let handles: Vec<_> = (0..active)
                        .map(|t| {
                            thread::spawn(move || {
                                let guard = pin();
                                for i in 0..1000 {
                                    let node = Node::new(t * 1000 + i);
                                    unsafe { retire(node) };
                                }
                                drop(guard);
                                kovan::flush();
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
        drop(parked_threads);
    }
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
                        unsafe { retire(node) };
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
        unsafe { retire(old.as_raw()) };
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
                                        unsafe { retire(old.as_raw()) };
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
                        unsafe { retire(old.as_raw()) };
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
                                        unsafe { retire(old.as_raw()) };
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
                        unsafe { retire(old.as_raw()) };
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
    bench_retire_threads,
    bench_atomic_load,
    bench_atomic_swap,
    bench_contention
);
criterion_main!(benches);
