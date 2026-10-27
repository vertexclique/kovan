//! Every channel operation: both flavors' sends and receives on values from
//! 16 bytes to 64 KiB, the try, deadline and async calls, the signal hooks,
//! a rendezvous hand-off, `after`, `tick`, `never`, `set_timer`, `select!`,
//! `Signal` and `AsyncSignal` with the `Notifier` methods, and producers and
//! consumers on several threads.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::executor::block_on;
use kovan_channel::flavors::{bounded, unbounded};
use kovan_channel::signal::{AsyncSignal, Notifier, Signal};
use kovan_channel::{after, never, select, set_timer, tick};
use std::hint::black_box;
use std::sync::Arc;
use std::task::Waker;
use std::time::{Duration, Instant};

type Group<'a> = criterion::BenchmarkGroup<'a, criterion::measurement::WallTime>;

/// `producers` threads send `iters` values each; `consumers` threads receive
/// them all.
fn pipe(
    producers: usize,
    consumers: usize,
    iters: u64,
    send: impl Fn(u64) + Send + Sync + 'static,
    recv: impl Fn() + Send + Sync + 'static,
) -> Duration {
    let (send, recv) = (Arc::new(send), Arc::new(recv));
    let start = Arc::new(std::sync::Barrier::new(producers + consumers + 1));
    let total = iters * producers as u64;
    let share = total / consumers as u64;
    let mut handles = Vec::new();
    for _ in 0..producers {
        let (send, start) = (Arc::clone(&send), Arc::clone(&start));
        handles.push(std::thread::spawn(move || {
            start.wait();
            for i in 0..iters {
                send(i);
            }
            kovan::flush();
        }));
    }
    for c in 0..consumers {
        let (recv, start) = (Arc::clone(&recv), Arc::clone(&start));
        let n = if c == consumers - 1 {
            total - share * (consumers as u64 - 1)
        } else {
            share
        };
        handles.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..n {
                recv();
            }
            kovan::flush();
        }));
    }
    start.wait();
    let began = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    began.elapsed()
}

fn sized<const S: usize>(g: &mut Group<'_>) {
    g.throughput(Throughput::Bytes(S as u64));
    let (tx, rx) = unbounded::channel::<[u8; S]>();
    g.bench_function(BenchmarkId::new("unbounded_send_recv", S), |b| {
        b.iter(|| {
            tx.send(black_box([1u8; S]));
            black_box(rx.recv().map(|v| v[0]))
        })
    });
    let (btx, brx) = bounded::channel::<[u8; S]>(64);
    g.bench_function(BenchmarkId::new("bounded_send_recv", S), |b| {
        b.iter(|| {
            btx.send(black_box([1u8; S]));
            black_box(brx.recv().map(|v| v[0]))
        })
    });
    for (p, c) in [(1usize, 1usize), (2, 2), (4, 4)] {
        g.bench_function(
            BenchmarkId::new(format!("unbounded_pipe_{p}x{c}"), S),
            |b| {
                b.iter_custom(|iters| {
                    let (tx, rx) = unbounded::channel::<[u8; S]>();
                    let (tx, rx) = (Arc::new(tx), Arc::new(rx));
                    pipe(
                        p,
                        c,
                        iters,
                        move |i| tx.send([i as u8; S]),
                        move || {
                            black_box(rx.recv());
                        },
                    )
                })
            },
        );
        g.bench_function(BenchmarkId::new(format!("bounded_pipe_{p}x{c}"), S), |b| {
            b.iter_custom(|iters| {
                let (tx, rx) = bounded::channel::<[u8; S]>(256);
                let (tx, rx) = (Arc::new(tx), Arc::new(rx));
                pipe(
                    p,
                    c,
                    iters,
                    move |i| tx.send([i as u8; S]),
                    move || {
                        black_box(rx.recv());
                    },
                )
            })
        });
    }
}

fn noop_timer(_due: Instant, waker: Waker) {
    waker.wake();
}

fn bench_channels(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_size");
    sized::<16>(&mut group);
    sized::<256>(&mut group);
    sized::<4096>(&mut group);
    sized::<65536>(&mut group);
    group.finish();

    let mut group = c.benchmark_group("channel_api");
    group.bench_function("unbounded_new", |b| {
        b.iter(|| black_box(unbounded::channel::<u64>()))
    });
    group.bench_function("bounded_new_64", |b| {
        b.iter(|| black_box(bounded::channel::<u64>(64)))
    });
    let (tx, rx): (unbounded::Sender<u64>, unbounded::Receiver<u64>) = unbounded::channel();
    group.bench_function("unbounded_try_recv_empty", |b| {
        b.iter(|| black_box(rx.try_recv()))
    });
    group.bench_function("unbounded_recv_deadline_ready", |b| {
        b.iter(|| {
            tx.send(1);
            black_box(rx.recv_deadline(Instant::now() + Duration::from_secs(1)))
        })
    });
    group.bench_function("unbounded_is_empty", |b| {
        b.iter(|| black_box(rx.is_empty()))
    });
    group.bench_function("unbounded_is_disconnected", |b| {
        b.iter(|| black_box((rx.is_disconnected(), tx.is_disconnected())))
    });
    group.bench_function("unbounded_next_due", |b| {
        b.iter(|| black_box(rx.next_due()))
    });
    group.bench_function("unbounded_async_send_recv", |b| {
        b.iter(|| {
            tx.send(2);
            black_box(block_on(rx.recv_async()))
        })
    });
    let signal: Arc<dyn Notifier> = Arc::new(Signal::new());
    group.bench_function("unbounded_register_unregister_signal", |b| {
        b.iter(|| {
            rx.register_signal(Arc::clone(&signal));
            rx.unregister_signal(&*signal);
        })
    });
    group.bench_function("unbounded_wake_one_waiter", |b| {
        b.iter(|| rx.wake_one_waiter())
    });

    let (btx, brx): (bounded::Sender<u64>, bounded::Receiver<u64>) = bounded::channel(64);
    group.bench_function("bounded_is_full", |b| b.iter(|| black_box(btx.is_full())));
    group.bench_function("bounded_try_recv_empty", |b| {
        b.iter(|| black_box(brx.try_recv()))
    });
    group.bench_function("bounded_async_send_recv", |b| {
        b.iter(|| {
            block_on(btx.send_async(3));
            black_box(block_on(brx.recv_async()))
        })
    });
    group.bench_function("bounded_sender_register_unregister_signal", |b| {
        b.iter(|| {
            btx.register_signal(Arc::clone(&signal));
            btx.unregister_signal(&*signal);
        })
    });
    group.bench_function("bounded_sender_wake_one_waiter", |b| {
        b.iter(|| btx.wake_one_waiter())
    });
    group.bench_function("bounded_is_disconnected", |b| {
        b.iter(|| black_box((brx.is_disconnected(), btx.is_disconnected())))
    });
    group.bench_function("rendezvous_hand_off", |b| {
        b.iter_custom(|iters| {
            let (tx, rx) = bounded::channel::<u64>(0);
            let taker = std::thread::spawn(move || {
                for _ in 0..iters {
                    black_box(rx.recv());
                }
            });
            let began = Instant::now();
            for i in 0..iters {
                tx.send(i);
            }
            taker.join().unwrap();
            began.elapsed()
        })
    });
    group.bench_function("select_two_ready", |b| {
        let (t1, r1) = unbounded::channel::<u64>();
        let (_t2, r2) = unbounded::channel::<u64>();
        b.iter(|| {
            t1.send(4);
            select! {
                v = r1 => { black_box(v); },
                v = r2 => { black_box(v); },
            }
        })
    });
    group.bench_function("after_zero_recv", |b| {
        b.iter(|| black_box(after(Duration::ZERO).recv()))
    });
    let ticks = tick(Duration::from_nanos(1));
    group.bench_function("tick_recv", |b| b.iter(|| black_box(ticks.recv())));
    group.bench_function("never_try_recv", |b| {
        let r = never::<u64>();
        b.iter(|| black_box(r.try_recv()))
    });
    group.bench_function("set_timer", |b| b.iter(|| black_box(set_timer(noop_timer))));
    group.finish();

    let mut group = c.benchmark_group("signal");
    group.bench_function("signal_new", |b| b.iter(|| black_box(Signal::new())));
    let s = Signal::new();
    group.bench_function("signal_notify_wait", |b| {
        b.iter(|| {
            s.notify();
            s.wait();
        })
    });
    group.bench_function("signal_notify_wait_deadline", |b| {
        b.iter(|| {
            s.notify();
            black_box(s.wait_deadline(Instant::now() + Duration::from_secs(1)))
        })
    });
    group.bench_function("signal_is_notified_cancel", |b| {
        b.iter(|| {
            black_box(s.is_notified());
            black_box(s.cancel());
        })
    });
    let dynamic: &dyn Notifier = &s;
    group.bench_function("notifier_notify_cancel", |b| {
        b.iter(|| {
            dynamic.notify();
            black_box(dynamic.is_notified());
            black_box(dynamic.cancel());
        })
    });
    group.bench_function("notifier_try_notify_cancel", |b| {
        b.iter(|| {
            black_box(dynamic.try_notify());
            black_box(dynamic.cancel());
        })
    });
    group.bench_function("async_signal_new", |b| {
        b.iter(|| black_box(AsyncSignal::new()))
    });
    let a = AsyncSignal::new();
    let waker = Waker::noop();
    group.bench_function("async_signal_register_is_notified", |b| {
        b.iter(|| {
            a.register(waker);
            black_box(a.is_notified())
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(1)).warm_up_time(Duration::from_millis(300));
    targets = bench_channels
}
criterion_main!(benches);
