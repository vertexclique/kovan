//! Bounded single-producer single-consumer throughput.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kovan_channel::bounded;
use std::thread;

const N: u64 = 20_000;

fn bench_bounded_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("bounded_spsc");
    group.sample_size(20);
    group.throughput(Throughput::Elements(N));

    for cap in [1usize, 1024].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(cap), cap, |b, &cap| {
            b.iter(|| {
                let (tx, rx) = bounded::<u64>(cap);
                let producer = thread::spawn(move || {
                    for i in 0..N {
                        tx.send(i);
                    }
                });
                for _ in 0..N {
                    rx.recv().unwrap();
                }
                producer.join().unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_bounded_spsc);
criterion_main!(benches);
