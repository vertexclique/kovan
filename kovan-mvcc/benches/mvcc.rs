//! Every MVCC operation: transactions at each isolation level (begin, read,
//! write, delete, commit) on values from 16 bytes to 64 KiB, writers from
//! several threads on keys of their own and on one hot key, the
//! constructors and accessors of the engine, the watermarks, both
//! registries, the lock table, the oracles, the backoff, and every method of
//! the storage trait, garbage collection included.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kovan_mvcc::{
    ActiveTxnRegistry, BackoffStrategy, DefaultBackoff, InMemoryStorage, InflightWriteRegistry,
    IsolationLevel, KovanMVCC, LocalTimestampOracle, LockInfo, LockTable, LockType,
    MockTimestampOracle, Storage, TimestampOracle, Txn, WriteInfo, WriteKind,
};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

fn lock(txn_id: u128, start_ts: u64, primary: &Arc<[u8]>) -> LockInfo {
    LockInfo {
        txn_id,
        start_ts,
        primary_key: Arc::clone(primary),
        lock_type: LockType::Put,
        short_value: None,
    }
}

/// Commits `value` under `key`, and every 64th call collects the versions
/// below the watermark, as an embedder's collector would: without it every
/// commit keeps its version and the bench measures a growing history.
fn put(db: &KovanMVCC, key: &[u8], value: Vec<u8>, i: u64) {
    let mut txn: Txn = db.begin();
    // A conflict with another writer is part of what the bench measures.
    if txn.write(key, value).is_ok() {
        black_box(txn.commit().ok());
    }
    if i.is_multiple_of(64) {
        let watermark = db.gc_watermark();
        black_box(db.storage().gc_writes(key, watermark) + db.storage().gc_data(key, watermark));
    }
}

fn sized<const S: usize>(g: &mut Group<'_>) {
    g.throughput(Throughput::Bytes(S as u64));
    let db = KovanMVCC::new();
    put(&db, b"k", vec![0u8; S], 0);
    let mut i = 0u64;
    g.bench_function(BenchmarkId::new("write_commit", S), |b| {
        b.iter(|| {
            i += 1;
            put(&db, b"k", vec![1u8; S], i);
        })
    });
    for level in [
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ] {
        g.bench_function(BenchmarkId::new(format!("read_{level:?}"), S), |b| {
            b.iter(|| {
                let txn = db.begin_with_isolation(level);
                black_box(txn.read(b"k").ok().flatten().map(|v| v.len()))
            })
        });
        g.bench_function(
            BenchmarkId::new(format!("read_write_commit_{level:?}"), S),
            |b| {
                b.iter(|| {
                    let mut txn = db.begin_with_isolation(level);
                    let v = txn.read(b"k").ok().flatten();
                    if txn.write(b"k", v.unwrap_or_default()).is_ok() {
                        black_box(txn.commit().ok());
                    }
                    i += 1;
                    if i.is_multiple_of(64) {
                        let w = db.gc_watermark();
                        black_box(db.storage().gc_writes(b"k", w) + db.storage().gc_data(b"k", w));
                    }
                })
            },
        );
    }
    for threads in [2usize, 4, 8] {
        let db = Arc::new(KovanMVCC::new());
        g.bench_function(
            BenchmarkId::new(format!("own_key_threads_{threads}"), S),
            |b| {
                b.iter_custom(|iters| {
                    let db = Arc::clone(&db);
                    on_threads(threads, iters, move |t, i| {
                        put(&db, &(t as u64).to_le_bytes(), vec![i as u8; S], i)
                    })
                })
            },
        );
        g.bench_function(
            BenchmarkId::new(format!("hot_key_threads_{threads}"), S),
            |b| {
                b.iter_custom(|iters| {
                    let db = Arc::clone(&db);
                    on_threads(threads, iters, move |_, i| {
                        put(&db, b"hot", vec![i as u8; S], i)
                    })
                })
            },
        );
    }
}

fn bench_mvcc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_size");
    sized::<16>(&mut group);
    sized::<256>(&mut group);
    sized::<4096>(&mut group);
    sized::<65536>(&mut group);
    group.finish();

    let mut group = c.benchmark_group("mvcc_engine");
    group.bench_function("new", |b| b.iter(|| black_box(KovanMVCC::new())));
    group.bench_function("with_oracle", |b| {
        b.iter(|| {
            black_box(KovanMVCC::with_oracle(
                Arc::new(LocalTimestampOracle::new()),
            ))
        })
    });
    group.bench_function("with_storage", |b| {
        b.iter(|| black_box(KovanMVCC::with_storage(Arc::new(InMemoryStorage::new()))))
    });
    group.bench_function("with_storage_and_oracle", |b| {
        b.iter(|| {
            black_box(KovanMVCC::with_storage_and_oracle(
                Arc::new(InMemoryStorage::new()),
                Arc::new(LocalTimestampOracle::with_initial(10)),
            ))
        })
    });
    let mut db = KovanMVCC::new();
    group.bench_function("set_backoff", |b| {
        b.iter(|| db.set_backoff(Arc::new(DefaultBackoff)))
    });
    group.bench_function("set_external_apply", |b| {
        b.iter(|| db.set_external_apply(black_box(false)))
    });
    group.bench_function("accessors", |b| {
        b.iter(|| {
            black_box(Arc::as_ptr(db.active_txns()));
            black_box(Arc::as_ptr(db.inflight_writes()));
            black_box(Arc::as_ptr(db.ts_oracle()) as *const ());
            black_box(Arc::as_ptr(db.storage()) as *const ());
        })
    });
    group.bench_function("begin_drop", |b| b.iter(|| black_box(db.begin())));
    group.bench_function("txn_accessors", |b| {
        let txn = db.begin();
        b.iter(|| black_box((txn.txn_id(), txn.start_ts(), txn.isolation_level())))
    });
    group.bench_function("write_delete_commit", |b| {
        b.iter(|| {
            let mut txn = db.begin();
            txn.write(b"d", vec![1]).ok();
            black_box(txn.commit().ok());
            let mut txn = db.begin();
            txn.delete(b"d").ok();
            black_box(txn.commit().ok());
        })
    });
    group.bench_function("gc_watermark", |b| b.iter(|| black_box(db.gc_watermark())));
    group.bench_function("safe_read_ts_idle", |b| {
        b.iter(|| black_box(db.safe_read_ts(black_box(100))))
    });
    let mut ext = KovanMVCC::new();
    ext.set_external_apply(true);
    group.bench_function("external_apply_commit_mark_applied", |b| {
        b.iter(|| {
            let mut txn = ext.begin();
            txn.write(b"e", vec![2]).ok();
            let id = txn.txn_id();
            black_box(txn.commit().ok());
            ext.mark_txn_applied(id);
        })
    });
    group.finish();

    let mut group = c.benchmark_group("mvcc_registry");
    group.bench_function("active_new", |b| {
        b.iter(|| black_box(ActiveTxnRegistry::new()))
    });
    let active = ActiveTxnRegistry::new();
    for id in 0..64u128 {
        active.register(id, id as u64);
    }
    group.bench_function("active_register_unregister", |b| {
        b.iter(|| {
            active.register(1000, 5);
            active.unregister(1000);
        })
    });
    group.bench_function("active_min_active_ts_64", |b| {
        b.iter(|| black_box(active.min_active_ts()))
    });
    group.bench_function("active_min_active_ts_bounded_64", |b| {
        b.iter(|| black_box(active.min_active_ts_bounded(100, 50)))
    });
    group.bench_function("active_len_is_empty", |b| {
        b.iter(|| black_box((active.len(), active.is_empty())))
    });
    group.bench_function("inflight_new", |b| {
        b.iter(|| black_box(InflightWriteRegistry::new()))
    });
    let inflight = InflightWriteRegistry::new();
    for id in 0..64u128 {
        inflight.register(id, id as u64);
    }
    group.bench_function("inflight_register_unregister", |b| {
        b.iter(|| {
            inflight.register(1000, 5);
            inflight.unregister(1000);
        })
    });
    group.bench_function("inflight_min_ts_64", |b| {
        b.iter(|| black_box(inflight.min_ts()))
    });
    group.bench_function("inflight_len_is_empty", |b| {
        b.iter(|| black_box((inflight.len(), inflight.is_empty())))
    });
    group.finish();

    let mut group = c.benchmark_group("mvcc_lock_table");
    group.bench_function("new", |b| b.iter(|| black_box(LockTable::new())));
    let table = LockTable::new();
    let primary: Arc<[u8]> = Arc::from(&b"p"[..]);
    table.try_lock(b"held", lock(7, 3, &primary)).ok();
    group.bench_function("try_lock_unlock", |b| {
        b.iter(|| {
            black_box(table.try_lock(b"x", lock(1, 1, &primary)).is_ok());
            black_box(table.unlock(b"x"));
        })
    });
    group.bench_function("try_lock_conflict", |b| {
        b.iter(|| black_box(table.try_lock(b"held", lock(2, 4, &primary)).is_err()))
    });
    group.bench_function("get_lock", |b| {
        b.iter(|| black_box(table.get_lock(b"held")))
    });
    group.bench_function("is_locked_by", |b| {
        b.iter(|| black_box(table.is_locked_by(b"held", 7)))
    });
    group.bench_function("has_lock_before", |b| {
        b.iter(|| black_box(table.has_lock_before(b"held", 10)))
    });
    group.finish();

    let mut group = c.benchmark_group("mvcc_oracle");
    group.bench_function("local_new", |b| {
        b.iter(|| black_box(LocalTimestampOracle::new()))
    });
    group.bench_function("local_with_initial", |b| {
        b.iter(|| black_box(LocalTimestampOracle::with_initial(9)))
    });
    let local = LocalTimestampOracle::new();
    group.bench_function("local_get_timestamp", |b| {
        b.iter(|| black_box(local.get_timestamp()))
    });
    group.bench_function("mock_new", |b| {
        b.iter(|| black_box(MockTimestampOracle::new()))
    });
    let mock = MockTimestampOracle::new();
    group.bench_function("mock_set_get_timestamp", |b| {
        b.iter(|| {
            mock.set(black_box(11));
            black_box(mock.get_timestamp())
        })
    });
    let backoff = DefaultBackoff;
    group.bench_function("default_backoff_first_attempt", |b| {
        b.iter(|| black_box(backoff.backoff(black_box(0))))
    });
    group.finish();

    let mut group = c.benchmark_group("mvcc_storage");
    group.bench_function("new", |b| b.iter(|| black_box(InMemoryStorage::new())));
    let storage = InMemoryStorage::new();
    let value = Arc::new(vec![7u8; 64]);
    let ts = AtomicU64::new(10);
    group.bench_function("put_get_delete_lock", |b| {
        b.iter(|| {
            storage.put_lock(b"l", lock(3, 5, &primary)).ok();
            black_box(storage.get_lock(b"l"));
            storage.delete_lock(b"l");
        })
    });
    group.bench_function("put_get_delete_data", |b| {
        b.iter(|| {
            storage.put_data(b"v", 5, Arc::clone(&value));
            black_box(storage.get_data(b"v", 5));
            storage.delete_data(b"v", 5);
        })
    });
    group.bench_function("put_write_gc", |b| {
        b.iter(|| {
            let t = ts.fetch_add(2, Ordering::Relaxed);
            storage.put_data(b"w", t, Arc::clone(&value));
            storage.put_write(
                b"w",
                t + 1,
                WriteInfo {
                    start_ts: t,
                    kind: WriteKind::Put,
                },
            );
            black_box(storage.gc_writes(b"w", t + 1) + storage.gc_data(b"w", t + 1));
        })
    });
    let now = ts.load(Ordering::Relaxed);
    group.bench_function("get_latest_write", |b| {
        b.iter(|| black_box(storage.get_latest_write(b"w", now)))
    });
    group.bench_function("get_latest_commit", |b| {
        b.iter(|| black_box(storage.get_latest_commit(b"w", now)))
    });
    for k in 0..64u64 {
        storage.put_write(
            &k.to_le_bytes(),
            3,
            WriteInfo {
                start_ts: 2,
                kind: WriteKind::Put,
            },
        );
    }
    group.bench_function("scan_write_keys_65", |b| {
        b.iter(|| black_box(storage.scan_write_keys().len()))
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_mvcc
}
criterion_main!(benches);
