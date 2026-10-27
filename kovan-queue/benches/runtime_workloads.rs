//! The time per round of the runtime workloads (`tests/support/runtime.rs`): worker inboxes,
//! connection queues, messages, a watched value, a broadcast ring, a waiter registry and a
//! blocking channel, on 2 and 8 workers, with values from 64 bytes to 64 KiB.

#[path = "../tests/support/runtime.rs"]
mod runtime;

use criterion::{Criterion, criterion_group, criterion_main};
use runtime::*;
use std::sync::Arc;
use std::time::Duration;

fn bench_runtime(c: &mut Criterion) {
    macro_rules! each {
        ($name:expr, $Load:ty, $size:expr) => {{
            let mut group = c.benchmark_group(format!("{}_{}", $name, $size));
            for threads in THREADS {
                group.bench_function(format!("threads_{threads}"), |b| {
                    b.iter_custom(|iters| {
                        let load = Arc::new(<$Load>::new(threads));
                        run_workers::<_, ()>(&load, threads, iters as usize, Idle::Hold, || ()).took
                    })
                });
            }
            group.finish();
        }};
    }
    every_workload!(each);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_runtime
}
criterion_main!(benches);
