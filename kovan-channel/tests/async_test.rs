use futures::executor::block_on;
use kovan_channel::{bounded, unbounded};
use std::thread;
use std::time::Duration;

#[test]
#[cfg_attr(miri, ignore)]
fn test_unbounded_async() {
    block_on(async {
        let (s, r) = unbounded();

        s.send(1);
        s.send(2);

        assert_eq!(r.recv_async().await, 1);
        assert_eq!(r.recv_async().await, 2);

        let r_clone = r.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            s.send(3);
        });

        assert_eq!(r_clone.recv_async().await, 3);
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

        assert_eq!(r.recv_async().await, 1);
        assert_eq!(r.recv_async().await, 2);
    });
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_mixed_async_blocking() {
    block_on(async {
        let (s, r) = unbounded();

        s.send(1);
        assert_eq!(r.recv_async().await, 1);

        let r_clone = r.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            s.send(2);
        });

        assert_eq!(r_clone.recv_async().await, 2);
    });
}
