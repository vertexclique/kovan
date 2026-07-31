//! `Receiver::recv_deadline` (both flavors): a bounded blocking receive
//! built on `Signal::wait_deadline`. Three scenarios per flavor, matching
//! the resilience plan's P0 API requirement -- times out when empty,
//! receives a message that is already there, and wakes early (before the
//! deadline) when a message arrives mid-wait.

use kovan_channel::{RecvDeadline, bounded, unbounded};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn unbounded_recv_deadline_times_out_when_empty() {
    let (_s, r) = unbounded::<i32>();
    let deadline = Instant::now() + Duration::from_millis(50);
    assert_eq!(r.recv_deadline(deadline), RecvDeadline::Timeout);
}

#[test]
fn unbounded_recv_deadline_receives_already_sent() {
    let (s, r) = unbounded::<i32>();
    s.send(7);
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(r.recv_deadline(deadline), RecvDeadline::Msg(7));
}

#[test]
#[cfg_attr(miri, ignore)]
fn unbounded_recv_deadline_wakes_early_on_send() {
    let (s, r) = unbounded::<i32>();
    let sender = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        s.send(9);
    });

    let started = Instant::now();
    let result = r.recv_deadline(started + Duration::from_secs(10));
    let elapsed = started.elapsed();

    sender.join().unwrap();
    assert_eq!(result, RecvDeadline::Msg(9));
    assert!(
        elapsed < Duration::from_secs(5),
        "recv_deadline did not wake early on send: waited {elapsed:?} against a 10s deadline for \
         a message sent after ~50ms"
    );
}

#[test]
fn unbounded_recv_deadline_disconnected_empty() {
    let (s, r) = unbounded::<i32>();
    drop(s);
    let deadline = Instant::now() + Duration::from_millis(50);
    assert_eq!(r.recv_deadline(deadline), RecvDeadline::Disconnected);
}

#[test]
fn bounded_recv_deadline_times_out_when_empty() {
    let (_s, r) = bounded::<i32>(4);
    let deadline = Instant::now() + Duration::from_millis(50);
    assert_eq!(r.recv_deadline(deadline), RecvDeadline::Timeout);
}

#[test]
fn bounded_recv_deadline_receives_already_sent() {
    let (s, r) = bounded::<i32>(4);
    s.send(7);
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(r.recv_deadline(deadline), RecvDeadline::Msg(7));
}

#[test]
#[cfg_attr(miri, ignore)]
fn bounded_recv_deadline_wakes_early_on_send() {
    let (s, r) = bounded::<i32>(4);
    let sender = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        s.send(9);
    });

    let started = Instant::now();
    let result = r.recv_deadline(started + Duration::from_secs(10));
    let elapsed = started.elapsed();

    sender.join().unwrap();
    assert_eq!(result, RecvDeadline::Msg(9));
    assert!(
        elapsed < Duration::from_secs(5),
        "recv_deadline did not wake early on send: waited {elapsed:?} against a 10s deadline for \
         a message sent after ~50ms"
    );
}

#[test]
fn bounded_recv_deadline_disconnected_empty() {
    let (s, r) = bounded::<i32>(4);
    drop(s);
    let deadline = Instant::now() + Duration::from_millis(50);
    assert_eq!(r.recv_deadline(deadline), RecvDeadline::Disconnected);
}
