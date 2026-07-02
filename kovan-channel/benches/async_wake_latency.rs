//! Async `recv_async` wake latency: single ping-pong round trip between two
//! persistent tasks on two threads. Each measured iteration is one full
//! round trip (ping wake + pong wake), reported as such -- not halved into
//! an estimated one-way number.

use criterion::{Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use kovan_channel::bounded;
use std::thread;

fn bench_async_ping_pong(c: &mut Criterion) {
    let (ping_tx, ping_rx) = bounded::<u64>(1);
    let (pong_tx, pong_rx) = bounded::<u64>(1);

    let responder = thread::spawn(move || {
        block_on(async {
            while let Some(v) = ping_rx.recv_async().await {
                pong_tx.send_async(v).await;
            }
        });
    });

    c.bench_function("async_recv_wake_ping_pong_roundtrip", |b| {
        b.iter(|| {
            block_on(async {
                ping_tx.send_async(1).await;
                pong_rx.recv_async().await
            })
        });
    });

    drop(ping_tx);
    responder.join().unwrap();
}

criterion_group!(benches, bench_async_ping_pong);
criterion_main!(benches);
