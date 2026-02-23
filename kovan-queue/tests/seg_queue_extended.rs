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
#[cfg_attr(miri, ignore)]
fn test_concurrent_mpmc_sum() {
    let q = Arc::new(SegQueue::new());
    let total = 4000;
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

/// Verify that SegQueue initializes slots explicitly (not via zeroed MaybeUninit)
/// by exercising push/pop with types that have non-trivial constructors.
#[test]
fn test_explicit_slot_init() {
    let q: SegQueue<String> = SegQueue::new();
    // If initialization is wrong, String values would be corrupted on push/pop
    for i in 0..100 {
        q.push(format!("item-{}", i));
    }
    for i in 0..100 {
        assert_eq!(q.pop(), Some(format!("item-{}", i)));
    }
    assert_eq!(q.pop(), None);
}

/// Verify SegQueue Drop doesn't double-free by pushing items,
/// popping some (triggering retire), and then dropping the queue.
#[test]
fn test_drop_after_partial_pop() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let drop_count = Arc::new(AtomicUsize::new(0));

    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n = 100;
    {
        let q: SegQueue<Counted> = SegQueue::new();
        for _ in 0..n {
            q.push(Counted(Arc::clone(&drop_count)));
        }
        // Pop half — these trigger retire() on consumed segments
        for _ in 0..n / 2 {
            q.pop();
        }
        // q dropped here — remaining n/2 values must also be dropped
        // Not exactly a test that tests this correctly.
    }

    assert_eq!(
        drop_count.load(Ordering::Relaxed),
        n,
        "all values must be dropped exactly once"
    );
}
