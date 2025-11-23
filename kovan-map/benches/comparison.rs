//! Benchmark comparison: kovan-map vs dashmap vs flurry vs evmap
//!
//! This benchmark compares the performance of different concurrent hash map implementations:
//! - kovan-map: Lock-free with kovan memory reclamation
//! - dashmap: Sharded lock-based hash map
//! - flurry: Java's ConcurrentHashMap port to Rust
//! - evmap: Eventually-consistent lock-free hash map (optimized for reads)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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

        // dashmap
        group.bench_with_input(BenchmarkId::new("dashmap", size), &size, |b, &size| {
            b.iter(|| {
                let map = dashmap::DashMap::new();
                for i in 0..size {
                    map.insert(black_box(i), black_box(i * 2));
                }
                map
            });
        });

        // flurry
        group.bench_with_input(BenchmarkId::new("flurry", size), &size, |b, &size| {
            b.iter(|| {
                let map = flurry::HashMap::new();
                {
                    let guard = map.guard();
                    for i in 0..size {
                        map.insert(black_box(i), black_box(i * 2), &guard);
                    }
                }
                map.len()
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

        // dashmap
        group.bench_with_input(BenchmarkId::new("dashmap", size), &size, |b, &size| {
            let map = dashmap::DashMap::new();
            for i in 0..size {
                map.insert(i, i * 2);
            }
            b.iter(|| {
                let mut sum = 0;
                for i in 0..size {
                    if let Some(v) = map.get(&black_box(i)) {
                        sum += *v;
                    }
                }
                sum
            });
        });

        // flurry
        group.bench_with_input(BenchmarkId::new("flurry", size), &size, |b, &size| {
            let map = flurry::HashMap::new();
            {
                let guard = map.guard();
                for i in 0..size {
                    map.insert(i, i * 2, &guard);
                }
            }
            b.iter(|| {
                let guard = map.guard();
                let mut sum = 0;
                for i in 0..size {
                    if let Some(v) = map.get(&black_box(i), &guard) {
                        sum += *v;
                    }
                }
                sum
            });
        });

        // evmap
        group.bench_with_input(BenchmarkId::new("evmap", size), &size, |b, &size| {
            let (reader, mut writer) = evmap::new::<usize, usize>();
            for i in 0..size {
                writer.update(i, i * 2);
            }
            writer.refresh();
            b.iter(|| {
                let mut sum = 0;
                for i in 0..size {
                    if let Some(v) = reader.get_one(&black_box(i)) {
                        sum += *v;
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

        // dashmap
        group.bench_with_input(
            BenchmarkId::new("dashmap", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                b.iter(|| {
                    let map = Arc::new(dashmap::DashMap::new());
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

        // flurry
        group.bench_with_input(
            BenchmarkId::new("flurry", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                b.iter(|| {
                    let map = Arc::new(flurry::HashMap::new());
                    let handles: Vec<_> = (0..threads)
                        .map(|tid| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                let guard = map.guard();
                                for i in 0..ops {
                                    let key = tid * ops + i;
                                    map.insert(black_box(key), black_box(key * 2), &guard);
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

        // dashmap
        group.bench_with_input(
            BenchmarkId::new("dashmap", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(dashmap::DashMap::new());
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
                                        sum += *v;
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

        // flurry
        group.bench_with_input(
            BenchmarkId::new("flurry", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(flurry::HashMap::new());
                {
                    let guard = map.guard();
                    for i in 0..total_ops {
                        map.insert(i, i * 2, &guard);
                    }
                }
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                let guard = map.guard();
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    if let Some(v) = map.get(&black_box(i), &guard) {
                                        sum += *v;
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

        // evmap (needs factory for thread-local readers)
        group.bench_with_input(
            BenchmarkId::new("evmap", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let (reader, mut writer) = evmap::new::<usize, usize>();
                for i in 0..total_ops {
                    writer.update(i, i * 2);
                }
                writer.refresh();
                // evmap uses thread-local readers
                let factory = reader.factory();
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let reader = factory.handle();
                            thread::spawn(move || {
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    if let Some(v) = reader.get_one(&black_box(i)) {
                                        sum += *v;
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

        // dashmap
        group.bench_with_input(
            BenchmarkId::new("dashmap", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(dashmap::DashMap::new());
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
                                        let key = tid * ops + i;
                                        map.insert(black_box(key), black_box(key));
                                    } else {
                                        if let Some(v) = map.get(&black_box(i)) {
                                            sum += *v;
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

        // flurry
        group.bench_with_input(
            BenchmarkId::new("flurry", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(flurry::HashMap::new());
                {
                    let guard = map.guard();
                    for i in 0..total_ops {
                        map.insert(i, i * 2, &guard);
                    }
                }
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|tid| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                let guard = map.guard();
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    if i % 10 == 0 {
                                        let key = tid * ops + i;
                                        map.insert(black_box(key), black_box(key), &guard);
                                    } else {
                                        if let Some(v) = map.get(&black_box(i), &guard) {
                                            sum += *v;
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

        // dashmap
        group.bench_with_input(
            BenchmarkId::new("dashmap", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(dashmap::DashMap::new());
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
                                        let key = tid * ops + i;
                                        map.insert(black_box(key), black_box(key));
                                    } else {
                                        if let Some(v) = map.get(&black_box(i)) {
                                            sum += *v;
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

        // flurry
        group.bench_with_input(
            BenchmarkId::new("flurry", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let map = Arc::new(flurry::HashMap::new());
                {
                    let guard = map.guard();
                    for i in 0..total_ops {
                        map.insert(i, i * 2, &guard);
                    }
                }
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|tid| {
                            let map = Arc::clone(&map);
                            thread::spawn(move || {
                                let guard = map.guard();
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    if i % 100 == 0 {
                                        let key = tid * ops + i;
                                        map.insert(black_box(key), black_box(key), &guard);
                                    } else {
                                        if let Some(v) = map.get(&black_box(i), &guard) {
                                            sum += *v;
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

        // evmap (excellent for read-heavy workloads)
        group.bench_with_input(
            BenchmarkId::new("evmap", threads),
            &(threads, ops_per_thread),
            |b, &(threads, ops)| {
                let (reader, mut writer) = evmap::new::<usize, usize>();
                for i in 0..total_ops {
                    writer.update(i, i * 2);
                }
                writer.refresh();
                let factory = reader.factory();
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let reader = factory.handle();
                            thread::spawn(move || {
                                let mut sum: usize = 0;
                                for i in 0..ops {
                                    // evmap is read-only from reader perspective
                                    // so we only do reads here
                                    if let Some(v) = reader.get_one(&black_box(i)) {
                                        sum += *v;
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
