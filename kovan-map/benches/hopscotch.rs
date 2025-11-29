use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use kovan_map::HopscotchMap;
use std::sync::Arc;
use std::thread;

// Sequential operations benchmarks

fn bench_insert_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_insert_sequential");

    for size in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let map = HopscotchMap::with_capacity(size * 2);
                for i in 0..size {
                    map.insert(black_box(i), black_box(i * 2));
                }
                map
            });
        });
    }
    group.finish();
}

fn bench_get_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_get_sequential");

    for size in [100, 1_000, 10_000, 100_000] {
        let map = HopscotchMap::with_capacity(size * 2);
        for i in 0..size {
            map.insert(i, i * 2);
        }

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                for i in 0..size {
                    black_box(map.get(&black_box(i)));
                }
            });
        });
    }
    group.finish();
}

fn bench_mixed_operations_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_mixed_sequential");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let map = HopscotchMap::with_capacity(size * 2);
                // 50% inserts, 40% gets, 10% removes
                for i in 0..size {
                    if i % 10 < 5 {
                        map.insert(black_box(i), black_box(i * 2));
                    } else if i % 10 < 9 {
                        black_box(map.get(&black_box(i / 2)));
                    } else {
                        black_box(map.remove(&black_box(i / 3)));
                    }
                }
                map
            });
        });
    }
    group.finish();
}

fn bench_remove_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_remove_sequential");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let map = HopscotchMap::with_capacity(size * 2);
                    for i in 0..size {
                        map.insert(i, i * 2);
                    }
                    map
                },
                |map| {
                    for i in 0..size {
                        black_box(map.remove(&black_box(i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// Concurrent operations benchmarks

fn bench_insert_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_insert_concurrent");

    for threads in [2, 4, 8] {
        let size_per_thread = 10_000;
        let total_ops = threads * size_per_thread;

        group.throughput(Throughput::Elements(total_ops as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}threads", threads)),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let map = Arc::new(HopscotchMap::with_capacity(total_ops * 2));
                    let mut handles = vec![];

                    for t in 0..threads {
                        let map_clone = Arc::clone(&map);
                        let handle = thread::spawn(move || {
                            for i in 0..size_per_thread {
                                let key = t * size_per_thread + i;
                                map_clone.insert(black_box(key), black_box(key * 2));
                            }
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    map
                });
            },
        );
    }
    group.finish();
}

fn bench_get_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_get_concurrent");

    for threads in [2, 4, 8] {
        let size = 50_000;
        let reads_per_thread = 10_000;
        let total_ops = threads * reads_per_thread;

        // Pre-populate map
        let map = Arc::new(HopscotchMap::with_capacity(size * 2));
        for i in 0..size {
            map.insert(i, i * 2);
        }

        group.throughput(Throughput::Elements(total_ops as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}threads", threads)),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let map_clone = Arc::clone(&map);
                        let handle = thread::spawn(move || {
                            for i in 0..reads_per_thread {
                                black_box(map_clone.get(&black_box(i % size)));
                            }
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_mixed_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_mixed_concurrent");

    for threads in [2, 4, 8] {
        let ops_per_thread = 5_000;
        let total_ops = threads * ops_per_thread;

        group.throughput(Throughput::Elements(total_ops as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}threads", threads)),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let map = Arc::new(HopscotchMap::with_capacity(total_ops * 2));

                    // Pre-populate with some data
                    for i in 0..1000 {
                        map.insert(i, i * 2);
                    }

                    let mut handles = vec![];

                    for t in 0..threads {
                        let map_clone = Arc::clone(&map);
                        let handle = thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                let key = t * ops_per_thread + i;
                                match i % 10 {
                                    0..=5 => {
                                        // 60% inserts
                                        map_clone.insert(black_box(key), black_box(key * 2));
                                    }
                                    6..=8 => {
                                        // 30% gets
                                        black_box(map_clone.get(&black_box(key / 2)));
                                    }
                                    _ => {
                                        // 10% removes
                                        black_box(map_clone.remove(&black_box(key / 3)));
                                    }
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    map
                });
            },
        );
    }
    group.finish();
}

// Growth benchmarks

fn bench_growth(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_growth");

    for initial_capacity in [64, 256, 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("init_{}", initial_capacity)),
            &initial_capacity,
            |b, &initial_capacity| {
                b.iter(|| {
                    let map = HopscotchMap::with_capacity(initial_capacity);
                    // Insert enough to trigger multiple growths
                    for i in 0..(initial_capacity * 4) {
                        map.insert(black_box(i), black_box(i * 2));
                    }
                    // Verify all items present
                    for i in 0..(initial_capacity * 4) {
                        assert!(map.get(&i).is_some());
                    }
                    map
                });
            },
        );
    }
    group.finish();
}

// Load factor benchmarks

fn bench_different_load_factors(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_load_factors");

    let capacity = 10_000;
    for load_percent in [25, 50, 75, 90] {
        let num_items = (capacity * load_percent) / 100;

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_percent_load", load_percent)),
            &num_items,
            |b, &num_items| {
                b.iter_batched(
                    || {
                        let map = HopscotchMap::with_capacity(capacity);
                        for i in 0..num_items {
                            map.insert(i, i * 2);
                        }
                        map
                    },
                    |map| {
                        // Benchmark lookup performance at this load factor
                        for i in 0..1000 {
                            black_box(map.get(&black_box(i % num_items)));
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// Contention benchmarks

fn bench_high_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("hopscotch_contention");

    // Benchmark with all threads accessing the same small key range
    for threads in [2, 4, 8] {
        let key_range = 100; // Small range = high contention
        let ops_per_thread = 5_000;

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}threads_range{}", threads, key_range)),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let map = Arc::new(HopscotchMap::with_capacity(1024));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let map_clone = Arc::clone(&map);
                        let handle = thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                let key = i % key_range; // All threads hit same keys
                                if i % 2 == 0 {
                                    map_clone.insert(black_box(key), black_box(key * 2));
                                } else {
                                    black_box(map_clone.get(&black_box(key)));
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    map
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert_sequential,
    bench_get_sequential,
    bench_mixed_operations_sequential,
    bench_remove_sequential,
    bench_insert_concurrent,
    bench_get_concurrent,
    bench_mixed_concurrent,
    bench_growth,
    bench_different_load_factors,
    bench_high_contention,
);
criterion_main!(benches);
