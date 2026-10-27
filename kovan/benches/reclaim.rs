//! The reclamation primitives themselves: the node helpers, every retire
//! (boxed, reclaimable, node), on nodes from 16 bytes to 64 KiB, from one
//! thread and from several, `flush` and `flush_pending`, nested pins, the
//! `Atomic` and `Shared` operations the existing benches leave out,
//! `CachePadded`, `NodeBox` and the `Reclaimable` methods.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use kovan::{
    Atomic, CachePadded, NodeBox, Reclaimable, RetiredNode, Shared, alloc_node, alloc_node_uninit,
    drop_node, flush, flush_pending, free_node, pin, retire, retire_node, retire_reclaimable,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// A node of `S` bytes of payload, with its `RetiredNode` first.
#[repr(C)]
struct Node<const S: usize> {
    retired: RetiredNode,
    payload: [u8; S],
}

impl<const S: usize> Node<S> {
    fn value(byte: u8) -> Self {
        Self {
            retired: RetiredNode::new(),
            payload: [byte; S],
        }
    }
}

// SAFETY: `RetiredNode` is the first field of a `#[repr(C)]` struct, and the
// default `dealloc` frees the `Box` the node came from.
unsafe impl<const S: usize> Reclaimable for Node<S> {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }
    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }
}

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
                flush();
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

type Group<'a> = criterion::BenchmarkGroup<'a, criterion::measurement::WallTime>;

fn retires<const S: usize>(g: &mut Group<'_>) {
    g.throughput(Throughput::Bytes(S as u64));
    g.bench_function(BenchmarkId::new("box", S), |b| {
        b.iter(|| unsafe { retire(Box::into_raw(Box::new(Node::<S>::value(1)))) })
    });
    g.bench_function(BenchmarkId::new("reclaimable", S), |b| {
        b.iter(|| unsafe { retire_reclaimable(Box::into_raw(Box::new(Node::<S>::value(1)))) })
    });
    g.bench_function(BenchmarkId::new("node", S), |b| {
        b.iter(|| unsafe { retire_node(alloc_node(Node::<S>::value(1))) })
    });
    for threads in [2usize, 4, 8] {
        g.bench_function(
            BenchmarkId::new(format!("node_threads_{threads}"), S),
            |b| {
                b.iter_custom(|iters| {
                    on_threads(threads, iters, |_, i| unsafe {
                        retire_node(alloc_node(Node::<S>::value(i as u8)))
                    })
                })
            },
        );
    }
    // A slot that readers load while a writer swaps and retires.
    for readers in [1usize, 3, 7] {
        let slot = Arc::new(Atomic::new(alloc_node(Node::<S>::value(0))));
        g.bench_function(
            BenchmarkId::new(format!("swap_retire_readers_{readers}"), S),
            |b| {
                b.iter_custom(|iters| {
                    let slot = Arc::clone(&slot);
                    on_threads(readers + 1, iters, move |t, i| {
                        let guard = pin();
                        if t == 0 {
                            let new =
                                unsafe { Shared::from_raw(alloc_node(Node::<S>::value(i as u8))) };
                            let old = slot.swap(new, Ordering::AcqRel, &guard);
                            unsafe { retire_node(old.as_raw()) };
                        } else {
                            let cur = slot.load(Ordering::Acquire, &guard);
                            black_box(unsafe { cur.deref() }.payload[0]);
                        }
                    })
                })
            },
        );
    }
}

fn bench_reclaim(c: &mut Criterion) {
    let mut group = c.benchmark_group("retire_size");
    retires::<16>(&mut group);
    retires::<256>(&mut group);
    retires::<4096>(&mut group);
    retires::<65536>(&mut group);
    group.finish();

    let mut group = c.benchmark_group("node");
    group.bench_function("alloc_drop", |b| {
        b.iter(|| unsafe { drop_node(alloc_node(black_box(Node::<64>::value(1)))) })
    });
    group.bench_function("alloc_uninit_free", |b| {
        b.iter(|| unsafe { free_node(alloc_node_uninit::<Node<64>>()) })
    });
    group.bench_function("nodebox_new_into_raw_from_raw", |b| {
        b.iter(|| {
            let raw = NodeBox::into_raw(NodeBox::new(Node::<64>::value(2)));
            black_box(unsafe { NodeBox::from_raw(raw) });
        })
    });
    group.finish();

    let mut group = c.benchmark_group("flush");
    group.bench_function("idle", |b| b.iter(flush));
    group.bench_function("after_retire", |b| {
        b.iter(|| {
            unsafe { retire_node(alloc_node(Node::<64>::value(3))) };
            flush();
        })
    });
    group.bench_function("pending_idle", |b| b.iter(flush_pending));
    group.bench_function("pending_after_retire", |b| {
        b.iter(|| {
            unsafe { retire_node(alloc_node(Node::<64>::value(4))) };
            flush_pending();
        })
    });
    group.finish();

    let mut group = c.benchmark_group("guard");
    group.bench_function("pin_nested", |b| {
        let _outer = pin();
        b.iter(|| black_box(pin()))
    });
    group.finish();

    let mut group = c.benchmark_group("atomic");
    let slot = Atomic::new(alloc_node(Node::<16>::value(0)));
    group.bench_function("load_unprotected", |b| {
        let guard = pin();
        b.iter(|| black_box(unsafe { slot.load_unprotected(Ordering::Acquire, &guard) }.as_raw()))
    });
    group.bench_function("compare_exchange_weak", |b| {
        let guard = pin();
        b.iter(|| {
            let cur = slot.load(Ordering::Acquire, &guard);
            black_box(
                slot.compare_exchange_weak(cur, cur, Ordering::AcqRel, Ordering::Acquire, &guard)
                    .is_ok(),
            )
        })
    });
    group.bench_function("shared_deref", |b| {
        let guard = pin();
        let cur = slot.load(Ordering::Acquire, &guard);
        b.iter(|| black_box(unsafe { cur.deref() }.payload[0]))
    });
    group.bench_function("shared_as_mut", |b| {
        let guard = pin();
        let mut cur = slot.load(Ordering::Acquire, &guard);
        // SAFETY: no other thread reaches the node.
        b.iter(|| black_box(unsafe { cur.as_mut() }.map(|n| n.payload[0])))
    });
    group.finish();
    unsafe { drop_node(slot.load(Ordering::Acquire, &pin()).as_raw()) };

    let mut group = c.benchmark_group("misc");
    group.bench_function("cache_padded_new_into_inner", |b| {
        b.iter(|| black_box(CachePadded::new(black_box(7u64)).into_inner()))
    });
    let mut node = Node::<16>::value(5);
    group.bench_function("reclaimable_retired_node", |b| {
        b.iter(|| black_box(core::ptr::from_ref(node.retired_node())))
    });
    group.bench_function("reclaimable_retired_node_mut", |b| {
        b.iter(|| black_box(core::ptr::from_mut(node.retired_node_mut())))
    });
    group.bench_function("reclaimable_dealloc", |b| {
        b.iter(|| unsafe {
            <Node<16> as Reclaimable>::dealloc(Box::into_raw(Box::new(Node::<16>::value(6))))
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_reclaim
}
criterion_main!(benches);
