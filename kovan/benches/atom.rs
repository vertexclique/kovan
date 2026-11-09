//! `Atom`, `AtomOption`, `AtomMap` and `Removed`: every operation, on values
//! from 16 bytes to 64 KiB, and stores and loads from several threads at once.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use kovan::{Atom, AtomOption};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Runs `op(thread, i)` `iters` times on each of `threads` threads started
/// once, and answers the time it took them all.
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

macro_rules! per_size {
    ($c:expr, $group:expr, $body:ident) => {{
        let mut group = $c.benchmark_group($group);
        $body::<16>(&mut group);
        $body::<256>(&mut group);
        $body::<4096>(&mut group);
        $body::<65536>(&mut group);
        group.finish();
    }};
}

fn store<const S: usize>(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    let atom = Atom::new([0u8; S]);
    g.throughput(Throughput::Bytes(S as u64));
    g.bench_function(BenchmarkId::from_parameter(S), |b| {
        b.iter(|| atom.store(black_box([1u8; S])))
    });
}

fn swap<const S: usize>(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    let atom = Atom::new([0u8; S]);
    g.throughput(Throughput::Bytes(S as u64));
    g.bench_function(BenchmarkId::from_parameter(S), |b| {
        b.iter(|| black_box(atom.swap(black_box([1u8; S]))[0]))
    });
}

fn load<const S: usize>(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    let atom = Atom::new([7u8; S]);
    g.bench_function(BenchmarkId::from_parameter(S), |b| {
        b.iter(|| black_box(atom.load()[S - 1]))
    });
}

fn load_clone<const S: usize>(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
) {
    let atom = Atom::new([7u8; S]);
    g.throughput(Throughput::Bytes(S as u64));
    g.bench_function(BenchmarkId::from_parameter(S), |b| {
        b.iter(|| black_box(atom.load_clone()[S - 1]))
    });
}

fn contended<const S: usize>(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
) {
    for threads in [2usize, 4, 8] {
        let atom = Arc::new(Atom::new([0u8; S]));
        g.throughput(Throughput::Elements(threads as u64));
        g.bench_function(BenchmarkId::new(format!("{S}B"), threads), |b| {
            b.iter_custom(|iters| {
                let atom = Arc::clone(&atom);
                on_threads(threads, iters, move |t, i| {
                    if t % 2 == 0 {
                        atom.store([i as u8; S]);
                    } else {
                        black_box(atom.load()[0]);
                    }
                })
            })
        });
    }
}

fn bench_atom(c: &mut Criterion) {
    per_size!(c, "atom/store", store);
    per_size!(c, "atom/swap", swap);
    per_size!(c, "atom/load", load);
    per_size!(c, "atom/load_clone", load_clone);
    per_size!(c, "atom/store_load_threads", contended);

    let mut group = c.benchmark_group("atom");
    let atom = Atom::new(5u64);
    group.bench_function("peek", |b| b.iter(|| black_box(atom.peek(|v| *v + 1))));
    group.bench_function("compare_and_swap", |b| {
        b.iter(|| {
            let current = atom.load();
            let next = *current + 1;
            black_box(atom.compare_and_swap(&current, next).is_ok())
        })
    });
    group.bench_function("rcu", |b| b.iter(|| atom.rcu(|v| v + 1)));
    let pair = Atom::new((1u64, 2u64));
    let view = pair.map(|p| &p.1);
    group.bench_function("map_load", |b| b.iter(|| black_box(*view.load())));
    group.bench_function("new_into_inner", |b| {
        b.iter(|| black_box(Atom::new(black_box(9u64)).into_inner()))
    });
    group.bench_function("removed_into_inner_unchecked", |b| {
        b.iter(|| {
            let removed = atom.swap(3);
            // SAFETY: no other thread loads this atom, so nothing else
            // reaches the removed value.
            black_box(unsafe { removed.into_inner_unchecked() })
        })
    });
    group.finish();

    let mut group = c.benchmark_group("atom_option");
    let opt = AtomOption::some(1u64);
    group.bench_function("load", |b| b.iter(|| black_box(opt.load().map(|g| *g))));
    group.bench_function("is_some", |b| b.iter(|| black_box(opt.is_some())));
    group.bench_function("is_none", |b| b.iter(|| black_box(opt.is_none())));
    group.bench_function("store_some", |b| b.iter(|| opt.store_some(black_box(2))));
    group.bench_function("store_none_store_some", |b| {
        b.iter(|| {
            opt.store_none();
            opt.store_some(3);
        })
    });
    group.bench_function("take_store_some", |b| {
        b.iter(|| {
            black_box(opt.take().map(|r| *r));
            opt.store_some(4);
        })
    });
    group.bench_function("some_none", |b| {
        b.iter(|| {
            black_box(AtomOption::some(black_box(5u64)));
            black_box(AtomOption::<u64>::none());
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_atom
}
criterion_main!(benches);
