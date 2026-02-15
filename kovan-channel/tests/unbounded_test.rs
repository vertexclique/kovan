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
