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

/// Regression test for double-free in `try_recv`.
///
/// `try_recv` uses `ptr::read` (now `ptr::replace`) to extract data from the
/// successor node. Without replacing the source with `None`, the node's
/// `Option<T>` discriminant still says `Some` when kovan's reclamation later
/// frees the node, causing `Node::drop` to double-free the inner value.
///
/// This test uses `String` (a heap-allocated, Drop type) to detect double-free.
/// With Copy types like `i32`, the double-free silently corrupts the heap but
/// rarely crashes; with String it reliably triggers "double free or corruption".
#[test]
#[cfg_attr(miri, ignore)]
fn test_no_double_free_on_recv() {
    // Run on a dedicated thread so Handle cleanup (which calls try_retire and
    // free_batch_list) happens deterministically at thread exit.
    let handle = thread::spawn(|| {
        for _ in 0..200 {
            let (tx, rx) = unbounded::<String>();
            tx.send("hello".to_string());
            tx.send("world".to_string());
            let a = rx.try_recv().unwrap();
            let b = rx.try_recv().unwrap();
            assert_eq!(a, "hello");
            assert_eq!(b, "world");
            // Channel drop retires remaining nodes (sentinel).
            // Consumed nodes are retired on next try_recv or drop.
            // If ptr::read left stale Some(value), kovan's destructor
            // would double-free the strings here.
        }
    });
    handle.join().unwrap();
}

/// Test that dropping a channel with unconsumed heap-allocated messages
/// doesn't double-free. Channel::drop retires all remaining nodes; consumed
/// nodes must have their data cleared to None.
#[test]
#[cfg_attr(miri, ignore)]
fn test_no_double_free_on_drop_with_pending() {
    let handle = thread::spawn(|| {
        for _ in 0..200 {
            let (tx, rx) = unbounded::<String>();
            for i in 0..5 {
                tx.send(format!("msg-{i}"));
            }
            // Consume only some messages — the rest are pending at drop time.
            let _ = rx.try_recv();
            let _ = rx.try_recv();
            // Drop channel with 3 pending messages + 2 consumed sentinel nodes.
        }
    });
    handle.join().unwrap();
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
