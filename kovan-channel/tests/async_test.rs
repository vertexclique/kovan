use futures::executor::block_on;
use kovan_channel::{bounded, unbounded};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Deadline for regression tests that guard against a permanent hang: if
/// `recv_async` never wakes on disconnect, the awaiting task never sends on
/// `done_tx` and this bounds the wait instead of hanging the test suite.
const HANG_GUARD: Duration = Duration::from_secs(5);

#[test]
#[cfg_attr(miri, ignore)]
fn test_unbounded_async() {
    block_on(async {
        let (s, r) = unbounded();

        s.send(1);
        s.send(2);

        assert_eq!(r.recv_async().await, Some(1));
        assert_eq!(r.recv_async().await, Some(2));

        let r_clone = r.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            s.send(3);
        });

        assert_eq!(r_clone.recv_async().await, Some(3));
    });
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_bounded_async() {
    block_on(async {
        let (s, r) = bounded(1);

        s.send_async(1).await;

        let s_clone = s.clone();
        thread::spawn(move || {
            block_on(async move {
                s_clone.send_async(2).await;
            });
        });

        assert_eq!(r.recv_async().await, Some(1));
        assert_eq!(r.recv_async().await, Some(2));
    });
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_mixed_async_blocking() {
    block_on(async {
        let (s, r) = unbounded();

        s.send(1);
        assert_eq!(r.recv_async().await, Some(1));

        let r_clone = r.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            s.send(2);
        });

        assert_eq!(r_clone.recv_async().await, Some(2));
    });
}

#[test]
#[cfg_attr(miri, ignore)]
fn unbounded_recv_async_disconnect_resolves_none() {
    let (s, r) = unbounded::<i32>();
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = block_on(r.recv_async());
        let _ = done_tx.send(result);
    });

    // Give the receiver task time to park on the empty channel before it
    // disconnects, exercising the wake-on-disconnect path rather than the
    // already-disconnected fast path.
    thread::sleep(Duration::from_millis(50));
    drop(s);

    let result = done_rx
        .recv_timeout(HANG_GUARD)
        .expect("recv_async did not wake up after disconnect (hang regression)");
    assert_eq!(result, None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn bounded_recv_async_disconnect_resolves_none() {
    let (s, r) = bounded::<i32>(4);
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = block_on(r.recv_async());
        let _ = done_tx.send(result);
    });

    // Give the receiver task time to park on the empty channel before it
    // disconnects, exercising the wake-on-disconnect path rather than the
    // already-disconnected fast path.
    thread::sleep(Duration::from_millis(50));
    drop(s);

    let result = done_rx
        .recv_timeout(HANG_GUARD)
        .expect("recv_async did not wake up after disconnect (hang regression)");
    assert_eq!(result, None);
}

#[test]
#[cfg_attr(miri, ignore)]
fn bounded_recv_async_drains_buffered_after_disconnect() {
    block_on(async {
        let (s, r) = bounded::<i32>(4);

        s.send(1);
        s.send(2);
        s.send(3);
        drop(s);

        assert_eq!(r.recv_async().await, Some(1));
        assert_eq!(r.recv_async().await, Some(2));
        assert_eq!(r.recv_async().await, Some(3));
        assert_eq!(r.recv_async().await, None);
    });
}

#[test]
#[cfg_attr(miri, ignore)]
fn bounded_recv_async_roundtrip_cross_thread() {
    const N: i32 = 200;

    block_on(async {
        let (s, r) = bounded::<i32>(1);

        let t = thread::spawn(move || {
            block_on(async move {
                for i in 0..N {
                    s.send_async(i).await;
                }
            });
        });

        for i in 0..N {
            assert_eq!(r.recv_async().await, Some(i));
        }

        t.join().unwrap();
    });
}
