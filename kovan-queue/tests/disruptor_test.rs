use kovan_queue::disruptor::{BusySpinWaitStrategy, Disruptor, EventHandler};
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
