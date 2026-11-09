//! The time of single rounds of the runtime workloads (`tests/support/runtime.rs`), the 99th
//! and 99.9th percentiles over every worker's rounds of a run: where a round that frees what
//! others retired pays for it, the tail shows it. Each sample is one run; its percentile is the
//! reading (see `support/units.rs`).

#[path = "../tests/support/runtime.rs"]
mod runtime;
#[path = "support/units.rs"]
mod units;

use criterion::{Criterion, criterion_group, criterion_main};
use runtime::*;
use std::sync::Arc;
use std::time::Duration;
use units::{NANOS, Reading};

fn bench_latency(c: &mut Criterion<Reading>) {
    macro_rules! each {
        ($name:expr, $Load:ty, $size:expr) => {{
            let mut group = c.benchmark_group(format!("{}_{}", $name, $size));
            for threads in THREADS {
                for (label, q) in [("p99", 0.99), ("p999", 0.999)] {
                    group.bench_function(format!("{label}_threads_{threads}"), |b| {
                        b.iter_custom(|iters| {
                            let load = Arc::new(<$Load>::new(threads));
                            let run = run_workers::<_, Rounds>(
                                &load,
                                threads,
                                rounds_for($size) / 2,
                                Idle::Hold,
                                || (),
                            );
                            run.rounds.quantile(q) as f64 * iters as f64
                        })
                    });
                }
            }
            group.finish();
        }};
    }
    every_workload!(each);
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_measurement(NANOS).without_plots().sample_size(10).measurement_time(Duration::from_millis(100)).warm_up_time(Duration::from_millis(1));
    targets = bench_latency
}
criterion_main!(benches);
