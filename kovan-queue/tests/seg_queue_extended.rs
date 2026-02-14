use kovan_queue::seg_queue::SegQueue;
use std::sync::Arc;
use std::thread;

#[test]
fn test_empty_pop() {
    let q: SegQueue<i32> = SegQueue::new();
    assert_eq!(q.pop(), None);
    assert_eq!(q.pop(), None);
}

#[test]
fn test_fifo_ordering() {
    let q = SegQueue::new();
    for i in 0..100 {
        q.push(i);
    }
    for i in 0..100 {
        assert_eq!(q.pop(), Some(i));
    }
    assert_eq!(q.pop(), None);
}

#[test]
fn test_many_items() {
    let q = SegQueue::new();
    let n = 50_000;
    for i in 0..n {
        q.push(i);
    }
    for i in 0..n {
        assert_eq!(q.pop(), Some(i));
    }
    assert_eq!(q.pop(), None);
}

#[test]
fn test_push_pop_interleaved() {
    let q = SegQueue::new();
    for round in 0..100 {
        for i in 0..10 {
            q.push(round * 10 + i);
        }
        for i in 0..10 {
            assert_eq!(q.pop(), Some(round * 10 + i));
        }
    }
}

#[test]
fn test_concurrent_mpmc_sum() {
    let q = Arc::new(SegQueue::new());
    let total = 8000;
    let producers = 4;
    let consumers = 4;

    let mut handles = vec![];
    for p in 0..producers {
        let q = q.clone();
        handles.push(thread::spawn(move || {
            for i in 0..(total / producers) {
                q.push(p * (total / producers) + i);
            }
        }));
    }

    let sum = Arc::new(std::sync::atomic::AtomicU64::new(0));
    for _ in 0..consumers {
        let q = q.clone();
        let sum = sum.clone();
        handles.push(thread::spawn(move || {
            let mut local = 0u64;
            for _ in 0..(total / consumers) {
                loop {
                    if let Some(v) = q.pop() {
                        local += v as u64;
                        break;
                    }
                    thread::yield_now();
                }
            }
            sum.fetch_add(local, std::sync::atomic::Ordering::Relaxed);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let expected: u64 = (0..total as u64).sum();
    assert_eq!(sum.load(std::sync::atomic::Ordering::SeqCst), expected);
}

#[test]
fn test_single_item() {
    let q = SegQueue::new();
    q.push(42);
    assert_eq!(q.pop(), Some(42));
    assert_eq!(q.pop(), None);
}

#[test]
fn test_string_values() {
    let q = SegQueue::new();
    q.push("hello".to_string());
    q.push("world".to_string());
    assert_eq!(q.pop(), Some("hello".to_string()));
    assert_eq!(q.pop(), Some("world".to_string()));
}
