use kovan_channel::bounded;
use std::thread;
use std::time::Duration;

#[test]
fn test_bounded_simple() {
    let (s, r) = bounded(2);
    s.send(1);
    s.send(2);

    assert_eq!(r.try_recv(), Some(1));
    assert_eq!(r.try_recv(), Some(2));
    assert_eq!(r.try_recv(), None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_bounded_capacity() {
    let (s, r) = bounded(1);
    s.send(1);

    let s_clone = s.clone();
    let t = thread::spawn(move || {
        s_clone.send(2);
    });

    // Give thread time to block/spin
    thread::sleep(Duration::from_millis(50));

    assert_eq!(r.recv(), Some(1));

    t.join().unwrap();
    assert_eq!(r.recv(), Some(2));
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_bounded_threads() {
    let (s, r) = bounded(10);
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

    let mut received = Vec::new();
    for _ in 0..200 {
        received.push(r.recv().unwrap());
    }

    t1.join().unwrap();
    t2.join().unwrap();

    received.sort();
    assert_eq!(received.len(), 200);
    for (i, &item) in received.iter().enumerate() {
        assert_eq!(item, i);
    }
}
