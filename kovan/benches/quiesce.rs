//! The cost of `quiesce()`: its fast path (a thread already at rest), its body after a pin
//! (what ending a reservation costs), and `flush()` after the same pin for comparison.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kovan::{flush, pin, quiesce};

fn bench_quiesce(c: &mut Criterion) {
    let mut group = c.benchmark_group("quiesce");

    // Pinned once, so the thread has a slot; every call after the first is at rest.
    drop(pin());
    quiesce();
    group.bench_function("fast_path", |b| b.iter(quiesce));

    group.bench_function("pin_then_quiesce", |b| {
        b.iter(|| {
            drop(black_box(pin()));
            quiesce();
        });
    });
    group.bench_function("pin_then_flush", |b| {
        b.iter(|| {
            drop(black_box(pin()));
            flush();
        });
    });
    group.bench_function("pin_alone", |b| b.iter(|| drop(black_box(pin()))));

    group.finish();
}

criterion_group!(benches, bench_quiesce);
criterion_main!(benches);
