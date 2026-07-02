//! Unbounded MPMC (4 senders x 4 receivers) throughput.

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

criterion_group!(benches, bench_unbounded_mpmc);
criterion_main!(benches);
