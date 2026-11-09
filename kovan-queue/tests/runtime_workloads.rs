//! The workloads of `tests/support/runtime.rs`, the shapes an async runtime gives these
//! structures, on 2 and 8 workers with values from 64 bytes to 64 KiB. Each must deliver
//! everything intact; what each holds is reported on a `RUNTIME` line: its peak while it runs,
//! what its workers hold while they are parked (after a flush or without one) over what is left
//! once all have flushed, and its time per round. One test, so no other test allocates beside
//! it.

#[path = "../../kovan/tests/support/retained.rs"]
mod retained;
#[path = "support/runtime.rs"]
mod runtime;

use std::sync::Arc;

use runtime::*;

/// What a workload held: its peak over the quiet point before it, the bytes live while its
/// workers were parked and those left once they flushed (both over that point), and its time.
struct Held {
    peak: isize,
    parked: isize,
    left: isize,
    nanos: u128,
}

fn measure<L: Workload>(load: L, threads: usize, rounds: usize, idle: Idle) -> Held {
    let load = Arc::new(load);
    let base = retained::begin();
    let mut parked = 0;
    let run = run_workers::<L, ()>(&load, threads, rounds, idle, || {
        parked = retained::live() - base
    });
    let peak = retained::peak_over(base);
    let left = retained::quiesce() - base;
    drop(load);
    Held {
        peak,
        parked,
        left,
        nanos: run.took.as_nanos(),
    }
}

macro_rules! each {
    ($name:expr, $Load:ty, $size:expr) => {
        for threads in THREADS {
            for idle in [Idle::Hold, Idle::Flush] {
                let rounds = rounds_for($size);
                let held = measure(<$Load>::new(threads), threads, rounds, idle);
                println!(
                    "RUNTIME {:<12} size {:>6} threads {} idle {:<5} peak {:>11} parked {:>10} (workers hold {:>10}) left {:>9} ns/round {:>8.1}",
                    $name, $size, threads, idle.name(), held.peak, held.parked, held.parked - held.left, held.left,
                    held.nanos as f64 / rounds as f64
                );
            }
        }
    };
}

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn runtime_workloads_deliver_and_report_what_they_hold() {
    every_workload!(each);
}
