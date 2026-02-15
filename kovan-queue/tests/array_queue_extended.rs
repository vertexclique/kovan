use kovan_queue::array_queue::ArrayQueue;
use std::sync::Arc;
use std::thread;

#[test]
fn test_capacity_small() {
    // ArrayQueue rounds capacity up to next power of 2
    let q = ArrayQueue::new(2);
    assert!(q.push(42).is_ok());
    assert!(q.push(99).is_ok());
    assert!(q.is_full());
    assert!(q.push(100).is_err());
    assert_eq!(q.pop(), Some(42));
    assert_eq!(q.pop(), Some(99));
    assert!(q.is_empty());
    assert_eq!(q.pop(), None);
}

#[test]
fn test_fifo_ordering() {
    let q = ArrayQueue::new(16);
    for i in 0..16 {
        q.push(i).unwrap();
    }
    for i in 0..16 {
        assert_eq!(q.pop(), Some(i));
    }
}

#[test]
fn test_wrap_around() {
    let q = ArrayQueue::new(4);

    // Fill and drain multiple times to exercise wrap-around
    for round in 0..10 {
        for i in 0..4 {
            q.push(round * 4 + i).unwrap();
        }
        for i in 0..4 {
            assert_eq!(q.pop(), Some(round * 4 + i));
        }
    }
}

#[test]
fn test_interleaved_push_pop() {
    let q = ArrayQueue::new(4);
    q.push(1).unwrap();
    q.push(2).unwrap();
    assert_eq!(q.pop(), Some(1));
    q.push(3).unwrap();
    assert_eq!(q.pop(), Some(2));
    assert_eq!(q.pop(), Some(3));
    assert_eq!(q.pop(), None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_spsc() {
    let q = Arc::new(ArrayQueue::new(64));
    let q2 = q.clone();

    let producer = thread::spawn(move || {
        for i in 0..10_000 {
            while q2.push(i).is_err() {
                thread::yield_now();
            }
        }
    });

    let mut received = Vec::new();
    let mut remaining = 10_000;
    while remaining > 0 {
        if let Some(v) = q.pop() {
            received.push(v);
            remaining -= 1;
        } else {
            thread::yield_now();
        }
    }

    producer.join().unwrap();

    // FIFO ordering preserved in SPSC
    for (i, val) in received.iter().enumerate() {
        assert_eq!(*val, i);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_mpmc_completeness() {
    let q = Arc::new(ArrayQueue::new(256));
    let total_items = 4000;
    let producers = 4;
    let consumers = 4;
    let items_per_producer = total_items / producers;
    let items_per_consumer = total_items / consumers;

    let mut handles = vec![];

    for _ in 0..producers {
        let q = q.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..items_per_producer {
                while q.push(1u64).is_err() {
                    thread::yield_now();
                }
            }
        }));
    }

    let sum = Arc::new(std::sync::atomic::AtomicU64::new(0));
    for _ in 0..consumers {
        let q = q.clone();
        let sum = sum.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..items_per_consumer {
                loop {
                    if let Some(v) = q.pop() {
                        sum.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        sum.load(std::sync::atomic::Ordering::SeqCst),
        total_items as u64
    );
    assert!(q.is_empty());
}

#[test]
fn test_push_returns_value_on_full() {
    let q = ArrayQueue::new(2);
    q.push(1).unwrap();
    q.push(2).unwrap();
    let err = q.push(3);
    assert_eq!(err, Err(3));
}
