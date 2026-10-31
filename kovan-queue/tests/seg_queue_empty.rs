//! `SegQueue`'s empty answers around a segment boundary: `is_empty`, `try_pop` and `pop` on a
//! head segment whose cells were all popped while a later segment still holds values.

use kovan_queue::seg_queue::{SegQueue, TryPopError};
use std::sync::Arc;
use std::thread;

/// Cells per segment in `seg_queue.rs` (private there).
const SEGMENT_SIZE: u64 = 32;

#[test]
fn is_empty_before_every_pop_across_segments() {
    for segments in 1..=4 {
        let q = SegQueue::new();
        let n = segments * SEGMENT_SIZE + 1;
        for i in 0..n {
            q.push(i);
        }
        for i in 0..n {
            assert!(!q.is_empty(), "{i} of {n} popped");
            assert_eq!(q.len() as u64, n - i);
            assert_eq!(q.pop(), Some(i));
        }
        assert!(q.is_empty());
        assert_eq!(q.pop(), None);
        assert_eq!(q.try_pop(), Err(TryPopError::Empty));
    }
}

#[test]
fn a_drained_head_segment_answers_again_and_again() {
    let q = SegQueue::new();
    for i in 0..=SEGMENT_SIZE {
        q.push(i);
    }
    for i in 0..SEGMENT_SIZE {
        assert_eq!(q.pop(), Some(i));
    }
    assert!(!q.is_empty());
    assert!(!q.is_empty());
    assert_eq!(q.len(), 1);
    q.push(SEGMENT_SIZE + 1);
    assert_eq!(q.try_pop(), Ok(SEGMENT_SIZE));
    assert_eq!(q.pop(), Some(SEGMENT_SIZE + 1));
    assert!(q.is_empty());
}

#[test]
fn try_pop_and_pop_take_the_next_segment_after_a_drained_one() {
    for by_try in [false, true] {
        let q = SegQueue::new();
        for i in 0..=2 * SEGMENT_SIZE {
            q.push(i);
        }
        for i in 0..=2 * SEGMENT_SIZE {
            let got = if by_try { q.try_pop().ok() } else { q.pop() };
            assert_eq!(got, Some(i));
        }
        assert_eq!(q.try_pop(), Err(TryPopError::Empty));
        assert_eq!(q.pop(), None);
        assert!(q.is_empty());
    }
}

#[test]
fn a_single_consumer_that_sees_a_value_gets_it() {
    const N: u64 = 40 * SEGMENT_SIZE + 5;
    let q = Arc::new(SegQueue::new());
    let producer = {
        let q = Arc::clone(&q);
        thread::spawn(move || {
            for i in 0..N {
                q.push(i);
            }
        })
    };
    let mut next = 0;
    while next < N {
        if q.is_empty() {
            std::hint::spin_loop();
            continue;
        }
        loop {
            match q.try_pop() {
                Ok(v) => {
                    assert_eq!(v, next);
                    next += 1;
                    break;
                }
                Err(TryPopError::Busy) => std::hint::spin_loop(),
                Err(TryPopError::Empty) => panic!("is_empty saw value {next}, try_pop found none"),
            }
        }
    }
    producer.join().unwrap();
    assert!(q.is_empty());
}
