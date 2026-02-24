use kovan_queue::disruptor::{BusySpinWaitStrategy, Disruptor, EventHandler, Sequence};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

struct MyEvent {
    data: u64,
}

struct MyHandler {
    sum: Arc<AtomicU64>,
}

impl EventHandler<MyEvent> for MyHandler {
    fn on_event(&self, event: &MyEvent, _sequence: u64, _end_of_batch: bool) {
        self.sum.fetch_add(event.data, Ordering::Relaxed);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_disruptor_simple() {
    let sum = Arc::new(AtomicU64::new(0));
    let handler = MyHandler { sum: sum.clone() };

    let mut disruptor = Disruptor::builder(|| MyEvent { data: 0 })
        .buffer_size(128)
        .wait_strategy(BusySpinWaitStrategy)
        .build();

    disruptor.handle_events_with(handler);

    let mut producer = disruptor.start();

    for i in 0..100 {
        producer.publish(|event| {
            event.data = i;
        });
    }

    // Give some time for processing (in a real test we'd wait for a condition)
    let start = std::time::Instant::now();
    while sum.load(Ordering::Relaxed) < 4950 {
        if start.elapsed() > Duration::from_secs(2) {
            break;
        }
        thread::yield_now();
    }

    assert_eq!(sum.load(Ordering::Relaxed), 4950); // Sum of 0..100 is 4950
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_disruptor_multiple_handlers() {
    let sum1 = Arc::new(AtomicU64::new(0));
    let sum2 = Arc::new(AtomicU64::new(0));

    let handler1 = MyHandler { sum: sum1.clone() };
    let handler2 = MyHandler { sum: sum2.clone() };

    let mut disruptor = Disruptor::builder(|| MyEvent { data: 0 })
        .buffer_size(128)
        .wait_strategy(BusySpinWaitStrategy)
        .build();

    disruptor
        .handle_events_with(handler1)
        .handle_events_with(handler2);

    let mut producer = disruptor.start();

    for i in 0..100 {
        producer.publish(|event| {
            event.data = i;
        });
    }

    let start = std::time::Instant::now();
    while sum1.load(Ordering::Relaxed) < 4950 || sum2.load(Ordering::Relaxed) < 4950 {
        if start.elapsed() > Duration::from_secs(2) {
            break;
        }
        thread::yield_now();
    }

    assert_eq!(sum1.load(Ordering::Relaxed), 4950);
    assert_eq!(sum2.load(Ordering::Relaxed), 4950);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_disruptor_multi_producer() {
    struct TestEvent {
        data: u64,
    }

    struct TestHandler {
        processed: Arc<AtomicU64>,
    }

    impl EventHandler<TestEvent> for TestHandler {
        fn on_event(&self, event: &TestEvent, _sequence: u64, _end_of_batch: bool) {
            self.processed.fetch_add(event.data, Ordering::Relaxed);
        }
    }

    let mut disruptor = Disruptor::builder(|| TestEvent { data: 0 })
        .buffer_size(1024)
        .multi_producer()
        .build();

    let processed = Arc::new(AtomicU64::new(0));
    disruptor.handle_events_with(TestHandler {
        processed: processed.clone(),
    });

    let producer = disruptor.start();
    let producer = Arc::new(std::sync::Mutex::new(producer));

    let mut handles = vec![];

    // 10 producers, each publishing 100 events
    for _ in 0..10 {
        let producer = producer.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let mut p = producer.lock().unwrap();
                p.publish(|e| {
                    e.data = 1; // Just count events for simplicity, or use unique values
                });
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Wait for processing
    let start = std::time::Instant::now();
    while processed.load(Ordering::Relaxed) < 1000 {
        if start.elapsed() > Duration::from_secs(5) {
            break;
        }
        thread::yield_now();
    }

    assert_eq!(processed.load(Ordering::Relaxed), 1000);
}

/// Concurrent calls to `add_gating_sequences` must not race or lose entries.
#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_add_gating_sequences() {
    use kovan_queue::disruptor::{MultiProducerSequencer, Sequencer, YieldingWaitStrategy};
    use std::sync::Arc;

    // Build a bare MultiProducerSequencer (same type used inside Disruptor).
    let wait = Arc::new(YieldingWaitStrategy);
    let seq: Arc<dyn Sequencer> = Arc::new(MultiProducerSequencer::new(64, wait));

    // Spawn N threads, each calling add_gating_sequences with one Sequence.
    let n = 8usize;
    let mut handles = vec![];
    for _ in 0..n {
        let seq = Arc::clone(&seq);
        handles.push(thread::spawn(move || {
            seq.add_gating_sequences(vec![Arc::new(Sequence::new(0))]);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // The test passing without data-race / panic is the assertion.
}

/// `RingBuffer<T>` requires `T: Send + Sync` to be `Sync`, because multiple
/// consumers hold `&T` concurrently. This test verifies that a `Send + Sync`
/// type works correctly with multiple consumers.
#[test]
#[cfg_attr(miri, ignore)]
fn test_disruptor_sync_bound_with_multiple_consumers() {
    use std::sync::atomic::AtomicU64;

    /// Event whose inner value is `Send + Sync`.
    struct SyncEvent {
        value: AtomicU64,
    }

    struct SumHandler {
        total: Arc<AtomicU64>,
    }

    impl EventHandler<SyncEvent> for SumHandler {
        fn on_event(&self, event: &SyncEvent, _sequence: u64, _end_of_batch: bool) {
            self.total
                .fetch_add(event.value.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    let total1 = Arc::new(AtomicU64::new(0));
    let total2 = Arc::new(AtomicU64::new(0));

    let mut disruptor = Disruptor::builder(|| SyncEvent {
        value: AtomicU64::new(0),
    })
    .buffer_size(128)
    .wait_strategy(BusySpinWaitStrategy)
    .build();

    disruptor
        .handle_events_with(SumHandler {
            total: total1.clone(),
        })
        .handle_events_with(SumHandler {
            total: total2.clone(),
        });

    let mut producer = disruptor.start();

    for i in 0..50u64 {
        producer.publish(|e| {
            e.value.store(i, Ordering::Relaxed);
        });
    }

    let expected: u64 = (0..50).sum(); // 1225
    let start = std::time::Instant::now();
    while total1.load(Ordering::Relaxed) < expected || total2.load(Ordering::Relaxed) < expected {
        if start.elapsed() > Duration::from_secs(2) {
            break;
        }
        thread::yield_now();
    }

    assert_eq!(total1.load(Ordering::Relaxed), expected);
    assert_eq!(total2.load(Ordering::Relaxed), expected);
}

/// When the Producer is dropped, consumer threads must terminate cleanly.
#[test]
#[cfg_attr(miri, ignore)]
fn test_producer_drop_shuts_down_consumers() {
    use std::sync::atomic::AtomicBool;

    struct ShutdownEvent {
        data: u64,
    }

    // Track whether the handler was actually invoked.
    let handler_ran = Arc::new(AtomicBool::new(false));

    struct ShutdownHandler {
        ran: Arc<AtomicBool>,
    }

    impl EventHandler<ShutdownEvent> for ShutdownHandler {
        fn on_event(&self, _event: &ShutdownEvent, _sequence: u64, _end_of_batch: bool) {
            self.ran.store(true, Ordering::Relaxed);
        }
    }

    let mut disruptor = Disruptor::builder(|| ShutdownEvent { data: 0 })
        .buffer_size(64)
        .wait_strategy(BusySpinWaitStrategy)
        .build();

    disruptor.handle_events_with(ShutdownHandler {
        ran: handler_ran.clone(),
    });

    let mut producer = disruptor.start();

    // Publish a few events so the consumer is actively processing.
    for i in 0..10 {
        producer.publish(|e| {
            e.data = i;
        });
    }

    // Wait until the handler has processed at least one event.
    let start = std::time::Instant::now();
    while !handler_ran.load(Ordering::Relaxed) {
        if start.elapsed() > Duration::from_secs(2) {
            panic!("handler never ran");
        }
        thread::yield_now();
    }

    // Drop the producer. This should alert barriers and join consumer threads.
    // If the shutdown mechanism is broken, this will hang forever.
    let drop_start = std::time::Instant::now();
    drop(producer);
    let elapsed = drop_start.elapsed();

    // Consumer threads should terminate almost instantly (well under 5s).
    assert!(
        elapsed < Duration::from_secs(5),
        "Producer::drop took too long ({elapsed:?}), consumers may not have shut down"
    );
}
