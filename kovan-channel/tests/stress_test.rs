use kovan_channel::bounded;
use kovan_channel::unbounded;
use std::sync::Arc;
use std::thread;

#[test]
fn test_unbounded_high_throughput() {
    let (tx, rx) = unbounded::channel();
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);

    let mut handles = vec![];
    let producers = 4;
    let items_per = 5000;

    for p in 0..producers {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..items_per {
                tx.send(p * items_per + i);
            }
        }));
    }

    let total = producers * items_per;
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for _ in 0..4 {
        let rx = rx.clone();
        let count = count.clone();
        handles.push(thread::spawn(move || {
            while count.load(std::sync::atomic::Ordering::Relaxed) < total {
                if rx.try_recv().is_some() {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), total);
}

#[test]
fn test_bounded_back_pressure() {
    let (tx, rx) = bounded::channel(4);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);

    let tx2 = tx.clone();
    let producer = thread::spawn(move || {
        for i in 0..100 {
            tx2.send(i);
        }
    });

    let rx2 = rx.clone();
    let consumer = thread::spawn(move || {
        let mut received = vec![];
        for _ in 0..100 {
            received.push(rx2.recv().unwrap());
        }
        received
    });

    producer.join().unwrap();
    let received = consumer.join().unwrap();
    assert_eq!(received.len(), 100);

    // Verify FIFO ordering
    for (i, val) in received.iter().enumerate() {
        assert_eq!(*val, i);
    }
}

#[test]
fn test_unbounded_empty_try_recv() {
    let (_tx, rx) = unbounded::channel::<i32>();
    assert!(rx.try_recv().is_none());
    assert!(rx.is_empty());
}

#[test]
fn test_bounded_multiple_senders() {
    let (tx, rx) = bounded::channel(16);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);

    let mut handles = vec![];
    for t in 0..8 {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                tx.send(t * 100 + i);
            }
        }));
    }

    // Drain concurrently so senders don't block on the bounded channel
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rx2 = rx.clone();
    let count2 = count.clone();
    handles.push(thread::spawn(move || {
        while count2.load(std::sync::atomic::Ordering::Relaxed) < 800 {
            if rx2.try_recv().is_some() {
                count2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                thread::yield_now();
            }
        }
    }));

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 800);
}

#[test]
fn test_unbounded_multiple_receivers() {
    let (tx, rx) = unbounded::channel();
    let rx = Arc::new(rx);

    for i in 0..1000 {
        tx.send(i);
    }

    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..4 {
        let rx = rx.clone();
        let count = count.clone();
        handles.push(thread::spawn(move || {
            while count.load(std::sync::atomic::Ordering::Relaxed) < 1000 {
                if rx.try_recv().is_some() {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    break;
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1000);
}
