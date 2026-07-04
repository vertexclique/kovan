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

#[test]
fn test_capacity_one_rejects_push_past_capacity() {
    // Regression (cap=1 livelock): the old stamp arithmetic collided at
    // capacity 1 ("occupied" tail+1 == next lap's "free" tail+capacity), so
    // the second push saw the full slot as free, overwrote the unpopped
    // value, and desynced the stamps - after which pop() and even Drop's
    // drain loop spun forever.
    let q = ArrayQueue::new(1);
    assert_eq!(q.capacity(), 1);
    assert!(q.is_empty());
    assert!(!q.is_full());

    assert!(q.push(10).is_ok());
    assert!(q.is_full());
    assert_eq!(q.push(20), Err(20));

    assert_eq!(q.pop(), Some(10));
    assert!(q.is_empty());
    assert_eq!(q.pop(), None);
}

#[test]
fn test_capacity_zero_rounds_to_one_and_works() {
    let q = ArrayQueue::new(0);
    assert_eq!(q.capacity(), 1);
    assert!(q.push(1).is_ok());
    assert_eq!(q.push(2), Err(2));
    assert_eq!(q.pop(), Some(1));
    assert_eq!(q.pop(), None);
}

#[test]
fn test_capacity_one_multi_lap_cycles() {
    // Every push/pop pair on a 1-slot ring crosses the lap boundary,
    // exercising the lap-jump arithmetic on both head and tail repeatedly.
    let q = ArrayQueue::new(1);
    for i in 0..1000u64 {
        assert!(q.push(i).is_ok());
        assert_eq!(q.push(u64::MAX - i), Err(u64::MAX - i));
        assert!(q.is_full());
        assert_eq!(q.pop(), Some(i));
        assert_eq!(q.pop(), None);
    }
}

#[test]
fn test_capacity_one_drop_with_resident_value_terminates() {
    // Regression: the corrupted-stamp state made ArrayQueue::drop()'s
    // `while pop().is_some()` drain spin forever. A full cap-1 queue must
    // drop cleanly and run the resident value's destructor exactly once
    // (the rejected value drops caller-side when the Err binding dies).
    use std::sync::atomic::{AtomicUsize, Ordering};
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct D;
    impl Drop for D {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    {
        let q = ArrayQueue::new(1);
        assert!(q.push(D).is_ok());
        assert!(q.push(D).is_err()); // Err(D) dropped here: +1
    } // queue drop drains the resident D: +1
    assert_eq!(DROPS.load(Ordering::Relaxed), 2);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_capacity_one_concurrent_terminates_with_exact_accounting() {
    // The end-to-end livelock shape: producers retrying on full against a
    // cap-1 ring while consumers drain. Must terminate with every pushed
    // value popped exactly once (pre-fix this hung forever).
    const PRODUCERS: u64 = 4;
    const PER_PRODUCER: u64 = 1000;
    let q = Arc::new(ArrayQueue::new(1));
    let mut handles = vec![];

    for p in 0..PRODUCERS {
        let q = q.clone();
        handles.push(thread::spawn(move || {
            for j in 0..PER_PRODUCER {
                let mut v = p * PER_PRODUCER + j;
                loop {
                    match q.push(v) {
                        Ok(()) => break,
                        Err(rejected) => {
                            v = rejected;
                            thread::yield_now();
                        }
                    }
                }
            }
        }));
    }

    let popped_sum = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let popped_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    for _ in 0..2 {
        let q = q.clone();
        let sum = popped_sum.clone();
        let count = popped_count.clone();
        handles.push(thread::spawn(move || {
            use std::sync::atomic::Ordering;
            loop {
                if count.load(Ordering::Acquire) >= PRODUCERS * PER_PRODUCER {
                    break;
                }
                if let Some(v) = q.pop() {
                    sum.fetch_add(v, Ordering::Relaxed);
                    count.fetch_add(1, Ordering::AcqRel);
                } else {
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    let n = PRODUCERS * PER_PRODUCER;
    assert_eq!(popped_count.load(std::sync::atomic::Ordering::Acquire), n);
    // Values are the distinct integers 0..n, so the sum pins exact identity.
    assert_eq!(
        popped_sum.load(std::sync::atomic::Ordering::Relaxed),
        n * (n - 1) / 2
    );
    assert!(q.is_empty());
}
