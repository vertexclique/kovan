//! Benchmark: kovan-map performance
//!
//! This benchmark measures the performance of kovan-map.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::thread;

// Number of operations per benchmark
const SMALL_OPS: usize = 1_000;
const MEDIUM_OPS: usize = 10_000;
const LARGE_OPS: usize = 100_000;

// Thread counts to test
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

/// Benchmark: Single-threaded insert operations
fn bench_single_thread_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread_insert");

    for &size in &[SMALL_OPS, MEDIUM_OPS, LARGE_OPS] {
        group.throughput(Throughput::Elements(size as u64));

        // kovan-map
        group.bench_with_input(BenchmarkId::new("kovan-map", size), &size, |b, &size| {
            b.iter(|| {
                let map = kovan_map::HashMap::new();
                for i in 0..size {
                    map.insert(black_box(i), black_box(i * 2));
                }
                map
            });
        });
    }

    group.finish();
}

/// Benchmark: Single-threaded get operations
fn bench_single_thread_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread_get");

    for &size in &[SMALL_OPS, MEDIUM_OPS, LARGE_OPS] {
        group.throughput(Throughput::Elements(size as u64));

        // kovan-map
        group.bench_with_input(BenchmarkId::new("kovan-map", size), &size, |b, &size| {
            let map = kovan_map::HashMap::new();
            for i in 0..size {
                map.insert(i, i * 2);
            }
            b.iter(|| {
                let mut sum = 0;
                for i in 0..size {
                    if let Some(v) = map.get(&black_box(i)) {
                        sum += v;
                    }
                }
                sum
            });
        });
    }

    group.finish();
}

/// Benchmark: Concurrent insert operations
fn bench_concurrent_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_insert");
    group.sample_size(20);

    for &threads in THREAD_COUNTS {
        let ops_per_thread = MEDIUM_OPS / threads;
        let total_ops = ops_per_thread * threads;
        group.throughput(Throughput::Elements(total_ops as u64));

        // kovan-map
        group.bench_with_input(
            BenchmarkId::new("kovan-map", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                b.iter(|| {
                    let map = Arc::new(kovan_map::HashMap::new());
                    let handles: Vec<_> = (0..threads)
                        .map(|tid| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                for i in 0..ops {
                                    let key = tid * ops + i;
                                    map.insert(black_box(key), black_box(key * 2));
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                    map
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Concurrent read operations
fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    group.sample_size(20);

    for &threads in THREAD_COUNTS {
        let ops_per_thread = MEDIUM_OPS / threads;
        let total_ops = ops_per_thread * threads;
        group.throughput(Throughput::Elements(total_ops as u64));

        // kovan-map
        group.bench_with_input(
            BenchmarkId::new("kovan-map", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(kovan_map::HashMap::new());
                for i in 0..total_ops {
                    map.insert(i, i * 2);
                }
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    if let Some(v) = map.get(&black_box(i)) {
                                        sum += v;
                                    }
                                }
                                sum
                            })
                        })
                        .collect();
                    let sum: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
                    sum
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Mixed read-write workload (90% reads, 10% writes)
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_90read_10write");
    group.sample_size(20);

    for &threads in THREAD_COUNTS {
        let ops_per_thread = MEDIUM_OPS / threads;
        let total_ops = ops_per_thread * threads;
        group.throughput(Throughput::Elements(total_ops as u64));

        // kovan-map
        group.bench_with_input(
            BenchmarkId::new("kovan-map", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(kovan_map::HashMap::new());
                for i in 0..total_ops {
                    map.insert(i, i * 2);
                }
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|tid| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    if i % 10 == 0 {
                                        // 10% writes
                                        let key = tid * ops + i;
                                        map.insert(black_box(key), black_box(key));
                                    } else {
                                        // 90% reads
                                        if let Some(v) = map.get(&black_box(i)) {
                                            sum += v;
                                        }
                                    }
                                }
                                sum
                            })
                        })
                        .collect();
                    let sum: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
                    sum
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Read-heavy workload (99% reads, 1% writes)
fn bench_read_heavy_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy_99read_1write");
    group.sample_size(20);

    for &threads in THREAD_COUNTS {
        let ops_per_thread = MEDIUM_OPS / threads;
        let total_ops = ops_per_thread * threads;
        group.throughput(Throughput::Elements(total_ops as u64));

        // kovan-map
        group.bench_with_input(
            BenchmarkId::new("kovan-map", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(kovan_map::HashMap::new());
                for i in 0..total_ops {
                    map.insert(i, i * 2);
                }
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|tid| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    if i % 100 == 0 {
                                        // 1% writes
                                        let key = tid * ops + i;
                                        map.insert(black_box(key), black_box(key));
                                    } else {
                                        // 99% reads
                                        if let Some(v) = map.get(&black_box(i)) {
                                            sum += v;
                                        }
                                    }
                                }
                                sum
                            })
                        })
                        .collect();
                    let sum: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
                    sum
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_thread_insert,
    bench_single_thread_get,
    bench_concurrent_insert,
    bench_concurrent_reads,
    bench_mixed_workload,
    bench_read_heavy_workload,
);

criterion_main!(benches);
