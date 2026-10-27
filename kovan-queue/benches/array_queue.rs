//! `ArrayQueue` operations, single-threaded and between producers and
//! consumers, and `SegQueue` push/pop and producer/consumer throughput on
//! values from 16 bytes to 64 KiB (the existing `seg_queue` bench uses
//! `u64` values only).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kovan_queue::array_queue::ArrayQueue;
use kovan_queue::seg_queue::SegQueue;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

type Group<'a> = criterion::BenchmarkGroup<'a, criterion::measurement::WallTime>;

/// `producers` threads push `iters` values each and `consumers` threads pop
/// them all, through `push` and `pop` spinning on full and empty.
fn pipe(
    producers: usize,
    consumers: usize,
    iters: u64,
    push: impl Fn(u64) + Send + Sync + 'static,
    pop: impl Fn() -> bool + Send + Sync + 'static,
) -> Duration {
    let (push, pop) = (Arc::new(push), Arc::new(pop));
    let start = Arc::new(Barrier::new(producers + consumers + 1));
    let total = iters * producers as u64;
    let share = total / consumers as u64;
    let mut handles = Vec::new();
    for _ in 0..producers {
        let (push, start) = (Arc::clone(&push), Arc::clone(&start));
        handles.push(std::thread::spawn(move || {
            start.wait();
            for i in 0..iters {
                push(i);
            }
        }));
    }
    for c in 0..consumers {
        let (pop, start) = (Arc::clone(&pop), Arc::clone(&start));
        let n = if c == consumers - 1 {
            total - share * (consumers as u64 - 1)
        } else {
            share
        };
        handles.push(std::thread::spawn(move || {
            start.wait();
            let mut got = 0;
            while got < n {
                if pop() {
                    got += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    start.wait();
    let began = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    began.elapsed()
}

fn array_sized<const S: usize>(g: &mut Group<'_>) {
    let q = ArrayQueue::<[u8; S]>::new(64);
    g.throughput(Throughput::Bytes(S as u64));
    g.bench_function(BenchmarkId::new("push_pop", S), |b| {
        b.iter(|| {
            q.push(black_box([1u8; S])).ok();
            black_box(q.pop());
        })
    });
    for (p, c) in [(1usize, 1usize), (4, 4)] {
        let q = Arc::new(ArrayQueue::<[u8; S]>::new(1024));
        g.bench_function(BenchmarkId::new(format!("pipe_{p}x{c}"), S), |b| {
            b.iter_custom(|iters| {
                let (qp, qc) = (Arc::clone(&q), Arc::clone(&q));
                pipe(
                    p,
                    c,
                    iters,
                    move |i| {
                        let mut v = [i as u8; S];
                        while let Err(back) = qp.push(v) {
                            v = back;
                            std::hint::spin_loop();
                        }
                    },
                    move || qc.pop().is_some(),
                )
            })
        });
    }
}

fn seg_sized<const S: usize>(g: &mut Group<'_>) {
    let q = SegQueue::<[u8; S]>::new();
    g.throughput(Throughput::Bytes(S as u64));
    g.bench_function(BenchmarkId::new("push_pop", S), |b| {
        b.iter(|| {
            q.push(black_box([1u8; S]));
            black_box(q.pop());
        })
    });
    for (p, c) in [(1usize, 1usize), (4, 4)] {
        let q = Arc::new(SegQueue::<[u8; S]>::new());
        g.bench_function(BenchmarkId::new(format!("pipe_{p}x{c}"), S), |b| {
            b.iter_custom(|iters| {
                let (qp, qc) = (Arc::clone(&q), Arc::clone(&q));
                pipe(
                    p,
                    c,
                    iters,
                    move |i| qp.push([i as u8; S]),
                    move || qc.pop().is_some(),
                )
            })
        });
    }
}

fn bench_queues(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_queue");
    group.bench_function("new_64", |b| {
        b.iter(|| black_box(ArrayQueue::<u64>::new(64)))
    });
    let q = ArrayQueue::<u64>::new(64);
    group.bench_function("push_pop", |b| {
        b.iter(|| {
            q.push(black_box(1)).ok();
            black_box(q.pop());
        })
    });
    group.bench_function("pop_empty", |b| b.iter(|| black_box(q.pop())));
    group.bench_function("is_empty", |b| b.iter(|| black_box(q.is_empty())));
    group.bench_function("is_full", |b| b.iter(|| black_box(q.is_full())));
    group.bench_function("capacity", |b| b.iter(|| black_box(q.capacity())));
    let full = ArrayQueue::<u64>::new(1);
    full.push(1).ok();
    group.bench_function("push_full", |b| b.iter(|| black_box(full.push(2).is_err())));
    group.finish();

    let mut group = c.benchmark_group("array_queue_size");
    array_sized::<16>(&mut group);
    array_sized::<256>(&mut group);
    array_sized::<4096>(&mut group);
    array_sized::<65536>(&mut group);
    group.finish();

    let mut group = c.benchmark_group("seg_queue_size");
    seg_sized::<16>(&mut group);
    seg_sized::<256>(&mut group);
    seg_sized::<4096>(&mut group);
    seg_sized::<65536>(&mut group);
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_queues
}
criterion_main!(benches);
