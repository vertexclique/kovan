use kovan_queue::seg_queue::SegQueue;
use std::sync::Arc;
use std::thread;

#[test]
fn test_seg_queue_simple() {
    let q = SegQueue::new();
    q.push(1);
    q.push(2);
    assert_eq!(q.pop(), Some(1));
    assert_eq!(q.pop(), Some(2));
    assert_eq!(q.pop(), None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_seg_queue_concurrent() {
    let q = Arc::new(SegQueue::new());
    let mut handles = vec![];

    // Producers
    for i in 0..4 {
        let q = q.clone();
        handles.push(thread::spawn(move || {
            for j in 0..1000 {
                q.push(i * 1000 + j);
            }
        }));
    }

    // Consumers
    for _ in 0..4 {
        let q = q.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                while q.pop().is_none() {
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(q.pop().is_none());
}
