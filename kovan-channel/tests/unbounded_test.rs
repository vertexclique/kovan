use kovan_channel::unbounded;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_simple_send_recv() {
    let (s, r) = unbounded();
    s.send(1);
    s.send(2);
    s.send(3);

    assert_eq!(r.try_recv(), Some(1));
    assert_eq!(r.try_recv(), Some(2));
    assert_eq!(r.try_recv(), Some(3));
    assert_eq!(r.try_recv(), None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_threads() {
    let (s, r) = unbounded();
    let s1 = s.clone();
    let s2 = s.clone();

    let t1 = thread::spawn(move || {
        for i in 0..100 {
            s1.send(i);
        }
    });

    let t2 = thread::spawn(move || {
        for i in 100..200 {
            s2.send(i);
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let mut received = Vec::new();
    while let Some(i) = r.try_recv() {
        received.push(i);
    }

    received.sort();
    assert_eq!(received.len(), 200);
    for (i, &item) in received.iter().enumerate() {
        assert_eq!(item, i);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_recv_blocking() {
    let (s, r) = unbounded();
    let r = Arc::new(r);
    let r_clone = r.clone();

    let t = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        s.send(42);
    });

    // We haven't implemented blocking recv yet, but `recv` spins.
    assert_eq!(r_clone.recv(), Some(42));
    t.join().unwrap();
}

#[test]
fn test_receiver_clone() {
    let (s, r) = unbounded();
    let r2 = r.clone();

    s.send(1);
    s.send(2);

    assert_eq!(r.recv(), Some(1));
    assert_eq!(r2.recv(), Some(2));
}

/// Concurrent send/recv must not violate aliasing rules.
///
/// Before the fix, `try_recv()` created `&mut (*next.as_raw()).data` — a mutable
/// reference to the data field — while other threads could hold shared references
/// to the same node via an epoch guard.
/// The fix uses `addr_of_mut!` to obtain a raw pointer without asserting exclusivity.
#[test]
#[cfg_attr(miri, ignore)]
fn test_no_aliasing_in_dequeue() {
    let (tx, rx) = unbounded::<i32>();
    let rx = Arc::new(rx);

    let n = 1000usize;
    let sender = thread::spawn(move || {
        for i in 0..n as i32 {
            tx.send(i);
        }
    });

    let rx_clone = Arc::clone(&rx);
    let receiver = thread::spawn(move || {
        let mut received = vec![];
        while received.len() < n {
            if let Some(v) = rx_clone.try_recv() {
                received.push(v);
            }
        }
        received
    });

    sender.join().unwrap();
    let mut received = receiver.join().unwrap();
    received.sort();

    assert_eq!(received.len(), n);
    let expected: Vec<i32> = (0..n as i32).collect();
    assert_eq!(received, expected);
}
