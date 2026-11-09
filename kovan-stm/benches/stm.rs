//! Every STM operation: the engine and variable constructors, `atomically`
//! and `try_atomically` (on an engine and on the global one), loads and
//! stores on values from 16 bytes to 64 KiB (each committed store retires the
//! variable's old node), the commit and rollback hooks, and transactions from
//! several threads on one hot variable and on variables of their own.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kovan_stm::{Stm, StmError, TVar, Transaction, atomically, try_atomically};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

type Group<'a> = criterion::BenchmarkGroup<'a, criterion::measurement::WallTime>;

/// Moves one unit from `from` to `to`.
fn transfer(tx: &mut Transaction<'_>, from: &TVar<u64>, to: &TVar<u64>) -> Result<(), StmError> {
    let a = tx.load(from)?;
    let b = tx.load(to)?;
    tx.store(from, a.wrapping_sub(1))?;
    tx.store(to, b.wrapping_add(1))
}

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

fn sized<const S: usize>(g: &mut Group<'_>) {
    g.throughput(Throughput::Bytes(S as u64));
    let stm = Stm::new();
    let var = stm.tvar([0u8; S]);
    g.bench_function(BenchmarkId::new("load", S), |b| {
        b.iter(|| black_box(stm.atomically(|tx| tx.load(&var).map(|v| v[0]))))
    });
    g.bench_function(BenchmarkId::new("store", S), |b| {
        b.iter(|| stm.atomically(|tx| tx.store(&var, black_box([1u8; S]))))
    });
    g.bench_function(BenchmarkId::new("load_store", S), |b| {
        b.iter(|| {
            stm.atomically(|tx| {
                let mut v = tx.load(&var)?;
                v[0] = v[0].wrapping_add(1);
                tx.store(&var, v)
            })
        })
    });
    for threads in [2usize, 4, 8] {
        let stm = Arc::new(Stm::new());
        let hot = Arc::new(TVar::new([0u8; S]));
        g.bench_function(
            BenchmarkId::new(format!("hot_store_threads_{threads}"), S),
            |b| {
                b.iter_custom(|iters| {
                    let (stm, hot) = (Arc::clone(&stm), Arc::clone(&hot));
                    on_threads(threads, iters, move |_, i| {
                        stm.atomically(|tx| tx.store(&hot, [i as u8; S]))
                    })
                })
            },
        );
        let own: Arc<Vec<TVar<[u8; S]>>> =
            Arc::new((0..threads).map(|_| TVar::new([0u8; S])).collect());
        g.bench_function(
            BenchmarkId::new(format!("own_store_threads_{threads}"), S),
            |b| {
                b.iter_custom(|iters| {
                    let (stm, own) = (Arc::clone(&stm), Arc::clone(&own));
                    on_threads(threads, iters, move |t, i| {
                        stm.atomically(|tx| tx.store(&own[t], [i as u8; S]))
                    })
                })
            },
        );
    }
}

fn bench_stm(c: &mut Criterion) {
    let mut group = c.benchmark_group("stm_size");
    sized::<16>(&mut group);
    sized::<256>(&mut group);
    sized::<4096>(&mut group);
    sized::<65536>(&mut group);
    group.finish();

    let mut group = c.benchmark_group("stm_api");
    group.bench_function("stm_new", |b| b.iter(|| black_box(Stm::new())));
    let stm = Stm::new();
    group.bench_function("stm_tvar", |b| {
        b.iter(|| black_box(stm.tvar(black_box(1u64))))
    });
    group.bench_function("tvar_new", |b| {
        b.iter(|| black_box(TVar::new(black_box(2u64))))
    });
    group.bench_function("atomically_empty", |b| {
        b.iter(|| stm.atomically(|_| Ok(())))
    });
    let var = stm.tvar(0u64);
    group.bench_function("try_atomically_load", |b| {
        b.iter(|| black_box(stm.try_atomically(|tx| tx.load(&var)).ok()))
    });
    group.bench_function("try_atomically_user_error", |b| {
        b.iter(|| {
            black_box(
                stm.try_atomically(|_| Err::<(), _>(StmError::User(String::new())))
                    .is_err(),
            )
        })
    });
    let global = TVar::new(0u64);
    group.bench_function("global_atomically_increment", |b| {
        b.iter(|| atomically(|tx| tx.load(&global).and_then(|v| tx.store(&global, v + 1))))
    });
    group.bench_function("global_try_atomically_load", |b| {
        b.iter(|| black_box(try_atomically(|tx| tx.load(&global)).ok()))
    });
    let vars: Vec<_> = (0..8u64).map(|i| stm.tvar(i)).collect();
    group.bench_function("transfer_across_8_vars", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = (i + 1) % 8;
            stm.atomically(|tx| transfer(tx, &vars[i], &vars[(i + 3) % 8]))
        })
    });
    group.bench_function("on_commit", |b| {
        b.iter(|| {
            stm.atomically(|tx| {
                tx.on_commit(|| {
                    black_box(());
                });
                tx.store(&var, 3)
            })
        })
    });
    group.bench_function("on_rollback", |b| {
        b.iter(|| {
            black_box(
                stm.try_atomically(|tx| {
                    tx.on_rollback(|| {
                        black_box(());
                    });
                    Err::<(), _>(StmError::User(String::new()))
                })
                .is_err(),
            )
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_stm
}
criterion_main!(benches);
