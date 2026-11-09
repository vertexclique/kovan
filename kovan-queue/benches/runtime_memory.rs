//! What the runtime workloads (`tests/support/runtime.rs`) hold, in bytes: the peak over the
//! quiet point before a run, and what the workers hold while they are parked (the bytes live
//! then over those left once they flushed), parked as they are and parked after a flush. Each
//! sample is one run; its reading is the sample (see `support/units.rs`).

#[path = "../../kovan/tests/support/retained.rs"]
mod retained;
#[path = "../tests/support/runtime.rs"]
mod runtime;
#[path = "support/units.rs"]
mod units;

use criterion::{Criterion, criterion_group, criterion_main};
use runtime::*;
use std::sync::Arc;
use std::time::Duration;
use units::{BYTES, Reading};

/// The peak of a run, and what its workers held while parked, both over the quiet point before.
fn held<L: Workload>(load: L, threads: usize, rounds: usize, idle: Idle) -> (f64, f64) {
    let load = Arc::new(load);
    let base = retained::begin();
    let mut parked = 0;
    run_workers::<L, ()>(&load, threads, rounds, idle, || {
        parked = retained::live() - base
    });
    let peak = retained::peak_over(base);
    let left = retained::quiesce() - base;
    drop(load);
    (peak.max(0) as f64, (parked - left).max(0) as f64)
}

fn bench_memory(c: &mut Criterion<Reading>) {
    macro_rules! each {
        ($name:expr, $Load:ty, $size:expr) => {{
            let mut group = c.benchmark_group(format!("{}_{}", $name, $size));
            let rounds = rounds_for($size) / 2;
            for threads in THREADS {
                group.bench_function(format!("peak_threads_{threads}"), |b| {
                    b.iter_custom(|iters| {
                        held(<$Load>::new(threads), threads, rounds, Idle::Hold).0 * iters as f64
                    })
                });
                for idle in [Idle::Hold, Idle::Flush] {
                    group.bench_function(
                        format!("parked_{}_threads_{threads}", idle.name()),
                        |b| {
                            b.iter_custom(|iters| {
                                held(<$Load>::new(threads), threads, rounds, idle).1 * iters as f64
                            })
                        },
                    );
                }
            }
            group.finish();
        }};
    }
    every_workload!(each);
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_measurement(BYTES).without_plots().sample_size(10).measurement_time(Duration::from_millis(100)).warm_up_time(Duration::from_millis(1));
    targets = bench_memory
}
criterion_main!(benches);
