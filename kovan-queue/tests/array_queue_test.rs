use kovan_queue::array_queue::ArrayQueue;
use std::sync::Arc;
use std::thread;

#[test]
fn test_simple_push_pop() {
    let q = ArrayQueue::new(2);
    assert!(q.is_empty());
    assert!(!q.is_full());

    assert!(q.push(1).is_ok());
    assert!(!q.is_empty());
    assert!(!q.is_full());

    assert!(q.push(2).is_ok());
    assert!(!q.is_empty());
    assert!(q.is_full());

    assert!(q.push(3).is_err());

    assert_eq!(q.pop(), Some(1));
    assert!(!q.is_full());

    assert_eq!(q.pop(), Some(2));
    assert!(q.is_empty());

    assert_eq!(q.pop(), None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_push_pop() {
    let q = Arc::new(ArrayQueue::new(100));
    let mut handles = vec![];

    // Producers
    for i in 0..4 {
        let q = q.clone();
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                while q.push(i * 100 + j).is_err() {
                    thread::yield_now();
                }
            }
        }));
    }

    // Consumers
    for _ in 0..4 {
        let q = q.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                while q.pop().is_none() {
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(q.is_empty());
}
