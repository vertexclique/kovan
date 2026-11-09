//! The disruptor: its sequences, both sequencers, every wait strategy, the
//! barrier, the ring buffer's accessors, the builder, and end-to-end
//! publishing to one and to several handlers.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kovan_queue::disruptor::{
    BatchEventProcessor, BlockingWaitStrategy, BusySpinWaitStrategy, Disruptor, DisruptorBuilder,
    EventHandler, MultiProducerSequencer, ProcessingSequenceBarrier, Producer, RingBuffer,
    Sequence, Sequencer, SingleProducerSequencer, WaitStrategy, YieldingWaitStrategy,
};
use kovan_queue::utils::CacheAligned;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Counts the events it sees.
struct Count(Arc<AtomicU64>);

impl EventHandler<u64> for Count {
    fn on_event(&self, event: &u64, _sequence: u64, _end_of_batch: bool) {
        black_box(*event);
        self.0.fetch_add(1, Ordering::Release);
    }
}

fn spin() -> Arc<dyn WaitStrategy> {
    Arc::new(BusySpinWaitStrategy)
}

fn bench_disruptor(c: &mut Criterion) {
    let mut group = c.benchmark_group("disruptor_sequence");
    let seq = Sequence::new(0);
    group.bench_function("new", |b| b.iter(|| black_box(Sequence::new(black_box(1)))));
    group.bench_function("get", |b| b.iter(|| black_box(seq.get())));
    group.bench_function("set", |b| b.iter(|| seq.set(black_box(2))));
    group.bench_function("compare_and_set", |b| {
        b.iter(|| {
            let cur = seq.get();
            black_box(seq.compare_and_set(cur, cur + 1))
        })
    });
    let handler = Count(Arc::new(AtomicU64::new(0)));
    group.bench_function("handler_on_event", |b| {
        b.iter(|| handler.on_event(black_box(&5), 0, true))
    });
    group.bench_function("cache_aligned_new", |b| {
        b.iter(|| black_box(CacheAligned::new(black_box(3u64))))
    });
    group.finish();

    let mut group = c.benchmark_group("disruptor_sequencer");
    for (name, make) in [
        (
            "single",
            (|| Arc::new(SingleProducerSequencer::new(1024, spin())) as Arc<dyn Sequencer>)
                as fn() -> Arc<dyn Sequencer>,
        ),
        ("multi", || {
            Arc::new(MultiProducerSequencer::new(1024, spin())) as Arc<dyn Sequencer>
        }),
    ] {
        group.bench_function(BenchmarkId::new("new", name), |b| {
            b.iter(|| black_box(make()))
        });
        let s = make();
        // A consumer that keeps up: the gating sequence follows the cursor.
        let gate = Arc::new(Sequence::new(-1));
        s.add_gating_sequences(vec![Arc::clone(&gate)]);
        group.bench_function(BenchmarkId::new("next_publish", name), |b| {
            b.iter(|| {
                let n = s.next();
                s.publish(n);
                gate.set(n);
            })
        });
        group.bench_function(BenchmarkId::new("get_cursor", name), |b| {
            b.iter(|| black_box(s.get_cursor()))
        });
        group.bench_function(
            BenchmarkId::new("get_highest_published_sequence", name),
            |b| {
                let cur = s.get_cursor();
                b.iter(|| black_box(s.get_highest_published_sequence(cur, cur)))
            },
        );
        group.bench_function(BenchmarkId::new("add_gating_sequences", name), |b| {
            let s = make();
            b.iter(|| s.add_gating_sequences(vec![Arc::new(Sequence::new(-1))]))
        });
    }
    group.finish();

    let mut group = c.benchmark_group("disruptor_wait");
    let seq: Arc<dyn Sequencer> = Arc::new(SingleProducerSequencer::new(1024, spin()));
    let n = seq.next();
    seq.publish(n);
    for (name, strategy) in [
        (
            "busy_spin",
            Arc::new(BusySpinWaitStrategy) as Arc<dyn WaitStrategy>,
        ),
        (
            "yielding",
            Arc::new(YieldingWaitStrategy) as Arc<dyn WaitStrategy>,
        ),
        (
            "blocking",
            Arc::new(BlockingWaitStrategy::new()) as Arc<dyn WaitStrategy>,
        ),
    ] {
        let barrier = ProcessingSequenceBarrier::new(
            Arc::clone(&strategy),
            Arc::clone(&seq),
            Arc::clone(&seq),
        );
        group.bench_function(BenchmarkId::new("wait_for_available", name), |b| {
            b.iter(|| black_box(strategy.wait_for(n, &seq, &seq, &barrier).ok()))
        });
        group.bench_function(BenchmarkId::new("signal_all_when_blocking", name), |b| {
            b.iter(|| strategy.signal_all_when_blocking())
        });
    }
    let barrier = ProcessingSequenceBarrier::new(spin(), Arc::clone(&seq), Arc::clone(&seq));
    group.bench_function("barrier_new", |b| {
        b.iter(|| {
            black_box(ProcessingSequenceBarrier::new(
                spin(),
                Arc::clone(&seq),
                Arc::clone(&seq),
            ))
        })
    });
    group.bench_function("barrier_wait_for", |b| {
        b.iter(|| black_box(barrier.wait_for(n).ok()))
    });
    group.bench_function("barrier_alert_clear", |b| {
        b.iter(|| {
            barrier.alert();
            black_box(barrier.is_alerted());
            barrier.clear_alert();
        })
    });
    group.finish();

    let mut group = c.benchmark_group("disruptor_ring");
    let sequencer: Arc<dyn Sequencer> = Arc::new(SingleProducerSequencer::new(1024, spin()));
    group.bench_function("new_1024", |b| {
        b.iter(|| {
            black_box(RingBuffer::new(
                || 0u64,
                1024,
                Arc::new(SingleProducerSequencer::new(1024, spin())),
            ))
        })
    });
    let mut ring = RingBuffer::new(|| 0u64, 1024, Arc::clone(&sequencer));
    let gate = Arc::new(Sequence::new(-1));
    ring.add_gating_sequences(vec![Arc::clone(&gate)]);
    group.bench_function("next_publish", |b| {
        b.iter(|| {
            let s = ring.next();
            ring.publish(s);
            gate.set(s);
        })
    });
    group.bench_function("get_mut", |b| b.iter(|| *ring.get_mut(black_box(5)) += 1));
    // SAFETY: no other thread touches the ring; every slot was made by the factory.
    group.bench_function("get", |b| {
        b.iter(|| black_box(unsafe { *ring.get(black_box(5)) }))
    });
    group.bench_function("get_unchecked", |b| {
        b.iter(|| black_box(unsafe { *ring.get_unchecked(black_box(5)) }))
    });
    group.bench_function("get_unchecked_mut", |b| {
        b.iter(|| unsafe { *ring.get_unchecked_mut(black_box(5)) += 1 })
    });
    group.bench_function("processor_new_get_sequence", |b| {
        let ring = Arc::new(RingBuffer::new(|| 0u64, 64, Arc::clone(&sequencer)));
        let barrier = Arc::new(ProcessingSequenceBarrier::new(
            spin(),
            Arc::clone(&sequencer),
            Arc::clone(&sequencer),
        ));
        b.iter(|| {
            let p = BatchEventProcessor::new(
                Arc::clone(&ring),
                Arc::clone(&barrier),
                Arc::new(Count(Arc::new(AtomicU64::new(0)))),
            );
            black_box(p.get_sequence())
        })
    });
    group.bench_function("processor_run_until_alert", |b| {
        b.iter_custom(|iters| {
            // A processor drains `iters` published events, then the alert ends its run.
            let seqr: Arc<dyn Sequencer> = Arc::new(SingleProducerSequencer::new(1 << 16, spin()));
            let ring = Arc::new(RingBuffer::new(|| 0u64, 1 << 16, Arc::clone(&seqr)));
            let barrier = Arc::new(ProcessingSequenceBarrier::new(
                spin(),
                Arc::clone(&seqr),
                Arc::clone(&seqr),
            ));
            let seen = Arc::new(AtomicU64::new(0));
            let p = Arc::new(BatchEventProcessor::new(
                Arc::clone(&ring),
                Arc::clone(&barrier),
                Arc::new(Count(Arc::clone(&seen))),
            ));
            ring.add_gating_sequences(vec![p.get_sequence()]);
            let runner = {
                let p = Arc::clone(&p);
                std::thread::spawn(move || p.run())
            };
            let began = Instant::now();
            for _ in 0..iters {
                let s = ring.next();
                ring.publish(s);
            }
            while seen.load(Ordering::Acquire) < iters {
                std::hint::spin_loop();
            }
            let took = began.elapsed();
            barrier.alert();
            runner.join().unwrap();
            took
        })
    });
    group.finish();

    let mut group = c.benchmark_group("disruptor_end_to_end");
    group.bench_function("builder_build", |b| {
        b.iter(|| {
            black_box(
                DisruptorBuilder::new(|| 0u64)
                    .buffer_size(1024)
                    .wait_strategy(BusySpinWaitStrategy)
                    .single_producer()
                    .build(),
            )
        })
    });
    group.bench_function("builder_multi_producer", |b| {
        b.iter(|| {
            black_box(
                Disruptor::builder(|| 0u64)
                    .buffer_size(1024)
                    .multi_producer()
                    .build(),
            )
        })
    });
    for handlers in [1usize, 3] {
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::new("publish", handlers), |b| {
            b.iter_custom(|iters| {
                let counts: Vec<_> = (0..handlers).map(|_| Arc::new(AtomicU64::new(0))).collect();
                let mut d = Disruptor::builder(|| 0u64)
                    .buffer_size(1 << 14)
                    .wait_strategy(BusySpinWaitStrategy)
                    .build();
                for c in &counts {
                    d.handle_events_with(Count(Arc::clone(c)));
                }
                let mut producer: Producer<u64> = d.start();
                let began = Instant::now();
                for i in 0..iters {
                    producer.publish(|e| *e = i);
                }
                for c in &counts {
                    while c.load(Ordering::Acquire) < iters {
                        std::hint::spin_loop();
                    }
                }
                let took = began.elapsed();
                drop(producer);
                took
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_disruptor
}
criterion_main!(benches);
