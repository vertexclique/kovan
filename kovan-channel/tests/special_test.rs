use futures::executor::block_on;
use kovan_channel::{RecvDeadline, after, never, select, tick, unbounded};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[test]
#[cfg_attr(miri, ignore)]
fn test_after() {
    let start = Instant::now();
    let r = after(Duration::from_millis(100));
    let msg = r.recv().unwrap();
    let elapsed = start.elapsed();

    assert!(elapsed >= Duration::from_millis(100));
    assert!(msg >= start + Duration::from_millis(100));
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_tick() {
    let start = Instant::now();
    let r = tick(Duration::from_millis(50));

    let _msg1 = r.recv().unwrap();
    let elapsed1 = start.elapsed();
    assert!(elapsed1 >= Duration::from_millis(50));

    let _msg2 = r.recv().unwrap();
    let elapsed2 = start.elapsed();
    assert!(elapsed2 >= Duration::from_millis(100));
}

#[test]
fn test_never() {
    let r = never::<i32>();
    assert_eq!(r.try_recv(), None);

    // We can't test blocking recv easily without timeout, but we know it should block.
    // We can spawn a thread and try recv, and assert it doesn't return quickly?
    // Or just trust the implementation.
}

/// An `after` delivers once, the instant it was due, and nothing after.
#[test]
#[cfg_attr(miri, ignore)] // clocks
fn an_after_delivers_once() {
    let start = Instant::now();
    let r = after(Duration::from_millis(20));
    let due = r.next_due().expect("an after not yet due has no due time");
    assert!(due >= start + Duration::from_millis(20));
    assert!(!r.is_disconnected(), "an after not yet due is disconnected");
    let at = r.recv().expect("an after delivered nothing");
    assert_eq!(at, due);
    assert!(Instant::now() >= due);
    // Delivered: disconnected, as when a thread sent it and dropped its
    // sender, so every receive answers at once.
    assert_eq!(r.try_recv(), None);
    assert_eq!(r.next_due(), None);
    assert!(r.is_empty());
    assert!(r.is_disconnected());
    assert_eq!(r.recv(), None);
    assert_eq!(
        r.recv_deadline(Instant::now() + Duration::from_secs(60)),
        RecvDeadline::Disconnected
    );
    assert_eq!(block_on(r.recv_async()), None);
}

/// An `after` past its due time is disconnected and still holds its
/// message: every receive takes it first.
#[test]
#[cfg_attr(miri, ignore)] // clocks
fn an_after_past_due_is_disconnected_and_delivers_first() {
    for take in 0..3 {
        let r = after(Duration::ZERO);
        assert!(r.is_disconnected());
        let got = match take {
            0 => r.recv(),
            1 => match r.recv_deadline(Instant::now()) {
                RecvDeadline::Msg(at) => Some(at),
                _ => None,
            },
            _ => block_on(r.recv_async()),
        };
        assert!(got.is_some(), "a due after answered nothing ({take})");
        assert_eq!(r.try_recv(), None);
    }
}

/// A task awaiting a clock that never comes due is not woken again and
/// again: it waits, as a blocking receive does.
#[test]
fn awaiting_a_clock_that_never_comes_due_does_not_spin() {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll, Wake, Waker};
    struct Flag(AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    for r in [after(Duration::MAX), tick(Duration::MAX)] {
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);
        let mut fut = pin!(r.recv_async());
        assert!(fut.as_mut().poll(&mut cx) == Poll::Pending);
        assert!(
            !flag.0.load(Ordering::SeqCst),
            "a never-due clock woke its task"
        );
    }
}

/// A tick delivers each interval: the instants it was due, in order, at
/// least an interval apart.
#[test]
#[cfg_attr(miri, ignore)] // clocks
fn a_tick_delivers_every_interval() {
    let start = Instant::now();
    let period = Duration::from_millis(10);
    let r = tick(period);
    let mut last = start;
    for _ in 0..5 {
        let at = r.recv().expect("a tick delivered nothing");
        assert!(at >= last + period);
        assert!(Instant::now() >= at);
        last = at;
    }
    assert!(start.elapsed() >= period * 5);
}

/// Ticks nobody received while they were due are skipped, not queued: one
/// message is due, and the next an interval after it is taken.
#[test]
#[cfg_attr(miri, ignore)] // clocks
fn a_tick_skips_the_ticks_nobody_received() {
    let period = Duration::from_millis(10);
    let r = tick(period);
    thread::sleep(period * 6);
    assert!(!r.is_empty());
    let taken = Instant::now();
    assert!(r.try_recv().is_some());
    let next = r.next_due().expect("a tick has a next due time");
    assert!(next >= taken + period);
    if Instant::now() < next {
        assert_eq!(r.try_recv(), None);
    }
}

/// A receive with a deadline before the message is due times out at the
/// deadline; one with a deadline after it gets the message. Neither answers
/// early: a thread the system wakes late, past the due time too, takes the
/// message then, which is due.
#[test]
#[cfg_attr(miri, ignore)] // clocks
fn a_clock_receive_waits_for_the_earlier_of_its_deadline_and_the_message() {
    let start = Instant::now();
    let r = after(Duration::from_millis(60));
    let deadline = start + Duration::from_millis(10);
    match r.recv_deadline(deadline) {
        RecvDeadline::Timeout => assert!(Instant::now() >= deadline),
        RecvDeadline::Msg(at) => {
            assert!(at >= start + Duration::from_millis(60));
            assert!(Instant::now() >= at, "a message taken before its due time");
            return;
        }
        other => panic!("an after before its due time gave {other:?}"),
    }
    match r.recv_deadline(Instant::now() + Duration::from_secs(60)) {
        RecvDeadline::Msg(at) => assert!(at >= start + Duration::from_millis(60)),
        other => panic!("an after past its due time gave {other:?}"),
    }
}

/// Receivers racing for an `after` take its one message once; racing for a
/// tick, each message once.
#[test]
#[cfg_attr(miri, ignore)] // threads and clocks
fn racing_receivers_take_each_message_once() {
    let r = after(Duration::from_millis(5));
    let taken = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (r, taken) = (r.clone(), Arc::clone(&taken));
            thread::spawn(move || {
                let end = Instant::now() + Duration::from_millis(50);
                while Instant::now() < end {
                    if r.try_recv().is_some() {
                        taken.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(taken.load(Ordering::SeqCst), 1);

    let r = tick(Duration::from_millis(1));
    let seen = Arc::new(Mutex::new(BTreeSet::new()));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (r, seen) = (r.clone(), Arc::clone(&seen));
            thread::spawn(move || {
                let end = Instant::now() + Duration::from_millis(50);
                while Instant::now() < end {
                    if let Some(at) = r.try_recv() {
                        assert!(seen.lock().unwrap().insert(at), "a tick taken twice");
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(!seen.lock().unwrap().is_empty());
}

/// A task awaits an `after` and a tick to their due times.
#[test]
#[cfg_attr(miri, ignore)] // clocks
fn a_task_awaits_a_clock_channel() {
    let start = Instant::now();
    let at = block_on(after(Duration::from_millis(15)).recv_async());
    assert!(at.is_some_and(|at| at >= start + Duration::from_millis(15)));
    let r = tick(Duration::from_millis(5));
    let first = block_on(r.recv_async()).unwrap();
    let second = block_on(r.recv_async()).unwrap();
    assert!(second >= first + Duration::from_millis(5));
}

/// `select!` wakes for a clock's message when no channel wakes it, and for
/// a channel's message when no clock is due.
#[test]
#[cfg_attr(miri, ignore)] // threads and clocks
#[allow(clippy::diverging_sub_expression)]
fn select_waits_for_the_earliest_clock_and_for_channels() {
    let (_quiet_tx, quiet) = unbounded::<u32>();
    let start = Instant::now();
    let ticks = tick(Duration::from_millis(10));
    for _ in 0..3 {
        select! {
            _v = quiet => panic!("a channel with nothing sent delivered"),
            at = ticks => assert!(at >= start + Duration::from_millis(10)),
        }
    }
    assert!(start.elapsed() >= Duration::from_millis(30));

    let (tx, rx) = unbounded::<u32>();
    let never_due = after(Duration::MAX);
    assert_eq!(never_due.next_due(), None);
    let sender = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        tx.send(7);
    });
    select! {
        _at = never_due => panic!("an after of the longest duration delivered"),
        v = rx => assert_eq!(v, 7),
    }
    sender.join().unwrap();
}

/// The longest durations never come due and never overflow the clock; a
/// tick of no duration is always due.
#[test]
#[cfg_attr(miri, ignore)] // clocks
fn the_longest_durations_never_come_due() {
    for r in [after(Duration::MAX), tick(Duration::MAX)] {
        assert_eq!(r.try_recv(), None);
        assert_eq!(r.next_due(), None);
        assert_eq!(
            r.recv_deadline(Instant::now() + Duration::from_millis(5)),
            RecvDeadline::Timeout
        );
    }
    let zero = tick(Duration::ZERO);
    for _ in 0..3 {
        assert!(zero.try_recv().is_some());
    }
}
