//! Every operation of `HashMap` and `HopscotchMap`, the same bench for both:
//! the conditional writes, `compute`, iteration, `clear`, the constructors and
//! accessors; inserts, replaces and gets on values from 16 bytes to 64 KiB;
//! grow-and-empty churn (each growth retires a table); and mixed reads and
//! writes from several threads.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kovan_map::{HashMap, HopscotchMap};
use std::hash::RandomState;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

type Group<'a> = criterion::BenchmarkGroup<'a, criterion::measurement::WallTime>;

fn on_threads(
    threads: usize,
    iters: u64,
    op: impl Fn(usize, u64) + Send + Sync + 'static,
) -> Duration {
    let op = Arc::new(op);
    let start = Arc::new(std::sync::Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (op, start) = (Arc::clone(&op), Arc::clone(&start));
            std::thread::spawn(move || {
                start.wait();
                for i in 0..iters {
                    op(t, i);
                }
                kovan::flush();
            })
        })
        .collect();
    start.wait();
    let began = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    began.elapsed()
}

macro_rules! map_bench {
    ($Map:ident, $api:ident, $sized:ident, $name:expr) => {
        fn $sized<const S: usize>(g: &mut Group<'_>) {
            g.throughput(Throughput::Bytes(S as u64));
            let map = $Map::<u64, [u8; S]>::new();
            for k in 0..64 {
                map.insert(k, [0u8; S]);
            }
            g.bench_function(BenchmarkId::new("replace", S), |b| {
                let mut k = 0u64;
                b.iter(|| {
                    k = (k + 1) % 64;
                    black_box(map.insert(k, [1u8; S]).is_some())
                })
            });
            g.bench_function(BenchmarkId::new("get", S), |b| {
                let mut k = 0u64;
                b.iter(|| {
                    k = (k + 1) % 64;
                    black_box(map.get(&k).map(|v| v[0]))
                })
            });
            g.bench_function(BenchmarkId::new("insert_remove", S), |b| {
                b.iter(|| {
                    map.insert(1000, [2u8; S]);
                    black_box(map.remove(&1000).is_some())
                })
            });
            g.bench_function(BenchmarkId::new("grow_empty_256", S), |b| {
                b.iter(|| {
                    let m = $Map::<u64, [u8; S]>::new();
                    for k in 0..256 {
                        m.insert(k, [3u8; S]);
                    }
                    for k in 0..256 {
                        black_box(m.remove(&k).is_some());
                    }
                })
            });
            for threads in [2usize, 4, 8] {
                let map = Arc::new($Map::<u64, [u8; S]>::new());
                for k in 0..1024 {
                    map.insert(k, [0u8; S]);
                }
                g.bench_function(
                    BenchmarkId::new(format!("mixed_90r_threads_{threads}"), S),
                    |b| {
                        b.iter_custom(|iters| {
                            let map = Arc::clone(&map);
                            on_threads(threads, iters, move |t, i| {
                                let k = (i * 7 + t as u64 * 131) % 1024;
                                if i % 10 == 0 {
                                    map.insert(k, [i as u8; S]);
                                } else {
                                    black_box(map.get(&k).map(|v| v[0]));
                                }
                            })
                        })
                    },
                );
            }
        }

        fn $api(c: &mut Criterion) {
            let mut group = c.benchmark_group(concat!($name, "_size"));
            $sized::<16>(&mut group);
            $sized::<256>(&mut group);
            $sized::<4096>(&mut group);
            $sized::<65536>(&mut group);
            group.finish();

            let mut group = c.benchmark_group(concat!($name, "_api"));
            group.bench_function("new", |b| b.iter(|| black_box($Map::<u64, u64>::new())));
            group.bench_function("with_capacity_1024", |b| {
                b.iter(|| black_box($Map::<u64, u64>::with_capacity(1024)))
            });
            group.bench_function("with_hasher", |b| {
                b.iter(|| black_box($Map::<u64, u64, _>::with_hasher(RandomState::new())))
            });
            group.bench_function("with_capacity_and_hasher_1024", |b| {
                b.iter(|| {
                    black_box($Map::<u64, u64, _>::with_capacity_and_hasher(
                        1024,
                        RandomState::new(),
                    ))
                })
            });
            let map = $Map::<u64, u64>::new();
            for k in 0..1024 {
                map.insert(k, k);
            }
            group.bench_function("len", |b| b.iter(|| black_box(map.len())));
            group.bench_function("is_empty", |b| b.iter(|| black_box(map.is_empty())));
            group.bench_function("capacity", |b| b.iter(|| black_box(map.capacity())));
            group.bench_function("hasher", |b| b.iter(|| black_box(map.hasher() as *const _)));
            group.bench_function("contains_key", |b| {
                b.iter(|| black_box(map.contains_key(&black_box(512))))
            });
            group.bench_function("get_or_insert_present", |b| {
                b.iter(|| black_box(map.get_or_insert(512, 0)))
            });
            group.bench_function("insert_if_absent_present", |b| {
                b.iter(|| black_box(map.insert_if_absent(512, 0)))
            });
            group.bench_function("insert_if_absent_new_then_remove", |b| {
                b.iter(|| {
                    black_box(map.insert_if_absent(5000, 1));
                    black_box(map.remove(&5000));
                })
            });
            group.bench_function("force_remove_reinsert", |b| {
                b.iter(|| {
                    black_box(map.force_remove(&7));
                    map.insert(7, 7);
                })
            });
            group.bench_function("remove_if_false", |b| {
                b.iter(|| black_box(map.remove_if(&8, |v| *v == u64::MAX)))
            });
            group.bench_function("remove_if_true_reinsert", |b| {
                b.iter(|| {
                    black_box(map.remove_if(&9, |_| true));
                    map.insert(9, 9);
                })
            });
            group.bench_function("compare_and_remove_reinsert", |b| {
                b.iter(|| {
                    black_box(map.compare_and_remove(&10, &10));
                    map.insert(10, 10);
                })
            });
            group.bench_function("replace_if", |b| {
                b.iter(|| black_box(map.replace_if(11, 11, |_| true).is_ok()))
            });
            group.bench_function("compare_and_swap", |b| {
                b.iter(|| black_box(map.compare_and_swap(12, &12, 12).is_ok()))
            });
            group.bench_function("compute", |b| {
                b.iter(|| black_box(map.compute(13, |v| v.map(|v| *v))))
            });
            group.bench_function("iter_1024", |b| b.iter(|| black_box(map.iter().count())));
            group.bench_function("keys_1024", |b| b.iter(|| black_box(map.keys().count())));
            group.bench_function("values_1024", |b| {
                b.iter(|| black_box(map.values().count()))
            });
            group.bench_function("extend_64_clear", |b| {
                let m = $Map::<u64, u64>::new();
                b.iter(|| {
                    m.extend((0..64).map(|k| (k, k)));
                    m.clear();
                })
            });
            group.finish();
        }
    };
}

map_bench!(HashMap, hashmap_api, hashmap_sized, "hashmap");
map_bench!(HopscotchMap, hopscotch_api, hopscotch_sized, "hopscotch");

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = hashmap_api, hopscotch_api
}
criterion_main!(benches);
