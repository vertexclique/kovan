//! Unbounded MPMC (4 senders x 4 receivers) throughput, and blocking receives: a ping-pong
//! whose receivers park between messages, and a burst received after the receiver parked.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kovan_channel::unbounded;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

const SENDERS: u64 = 4;
const RECEIVERS: u64 = 4;
const PER_SENDER: u64 = 5_000;
const TOTAL: u64 = SENDERS * PER_SENDER;

fn bench_unbounded_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("unbounded_mpmc_4x4");
    group.sample_size(20);
    group.throughput(Throughput::Elements(TOTAL));

    group.bench_function("throughput", |b| {
        b.iter(|| {
            let (tx, rx) = unbounded::<u64>();
            let mut handles = Vec::new();

            for s in 0..SENDERS {
                let tx = tx.clone();
                handles.push(thread::spawn(move || {
                    for i in 0..PER_SENDER {
                        tx.send(s * PER_SENDER + i);
                    }
                }));
            }
            drop(tx);

            let received = Arc::new(AtomicU64::new(0));
            for _ in 0..RECEIVERS {
                let rx = rx.clone();
                let received = received.clone();
                handles.push(thread::spawn(move || {
                    while received.load(Ordering::Relaxed) < TOTAL {
                        if rx.try_recv().is_some() {
                            received.fetch_add(1, Ordering::Relaxed);
                        } else if rx.is_disconnected() {
                            break;
                        } else {
                            thread::yield_now();
                        }
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.finish();
}

const ROUND_TRIPS: u64 = 1_000;
const BURST: u64 = 1_000;

/// One sender and one receiver whose `recv` parks: a ping-pong, each side blocked
/// until the other answers, and a burst the sender makes once the receiver waits.
fn bench_unbounded_blocking(c: &mut Criterion) {
    let mut group = c.benchmark_group("unbounded_blocking");
    group.sample_size(20);

    group.throughput(Throughput::Elements(2 * ROUND_TRIPS));
    group.bench_function("ping_pong", |b| {
        b.iter(|| {
            let (ping_tx, ping_rx) = unbounded::<u64>();
            let (pong_tx, pong_rx) = unbounded::<u64>();
            let echo = thread::spawn(move || {
                while let Some(v) = ping_rx.recv() {
                    pong_tx.send(v);
                }
            });
            for i in 0..ROUND_TRIPS {
                ping_tx.send(i);
                assert_eq!(pong_rx.recv(), Some(i));
            }
            drop(ping_tx);
            echo.join().unwrap();
        });
    });

    group.throughput(Throughput::Elements(BURST));
    group.bench_function("burst_recv", |b| {
        b.iter(|| {
            let (tx, rx) = unbounded::<u64>();
            let sender = thread::spawn(move || {
                for i in 0..BURST {
                    tx.send(i);
                }
            });
            for i in 0..BURST {
                assert_eq!(rx.recv(), Some(i));
            }
            sender.join().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_unbounded_mpmc, bench_unbounded_blocking);
criterion_main!(benches);
