//! With a runtime's timer installed (`set_timer`), a task awaiting a channel
//! of `after` or `tick` sleeps until its message is due: the timer gets the due
//! instant and the task's waker, the task is not woken before, and a poll
//! before then schedules nothing more. One test, in a file of its own: the
//! timer is the process's.

use kovan_channel::{after, set_timer, tick};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

static SCHEDULED: Mutex<Vec<Instant>> = Mutex::new(Vec::new());

/// The test's timer: records the due instant, and wakes the waker from a
/// thread of the test's own once it has passed.
fn test_timer(at: Instant, waker: Waker) {
    SCHEDULED.lock().unwrap().push(at);
    thread::spawn(move || {
        let now = Instant::now();
        if at > now {
            thread::sleep(at - now);
        }
        waker.wake();
    });
}

struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[cfg_attr(miri, ignore)] // threads and clocks
fn a_task_sleeps_on_the_runtimes_timer_until_due() {
    assert!(set_timer(test_timer));
    assert!(!set_timer(test_timer), "a second timer replaced the first");
    let mut attempts = 100;
    let mut kinds = [after as fn(Duration) -> _, tick].into_iter().peekable();
    while let Some(&make) = kinds.peek() {
        let r = make(Duration::from_millis(30));
        SCHEDULED.lock().unwrap().clear();
        let due = r.next_due().expect("a clock channel with no due time");
        let woken = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        let mut fut = pin!(r.recv_async());
        let first = fut.as_mut().poll(&mut cx);
        // Polled again before it is due: nothing more scheduled, no wake.
        let second = fut.as_mut().poll(&mut cx);
        if Instant::now() >= due {
            // A loaded machine reached the polls only once the message was
            // due: the case under test never arose, so take a fresh channel.
            assert!(attempts > 1, "every channel came due before its polls");
            attempts -= 1;
            continue;
        }
        assert!(first.is_pending());
        assert!(second.is_pending());
        assert_eq!(*SCHEDULED.lock().unwrap(), vec![due]);
        assert_eq!(
            woken.0.load(Ordering::SeqCst),
            0,
            "woken before its due time"
        );
        let end = Instant::now() + Duration::from_secs(60);
        while woken.0.load(Ordering::SeqCst) == 0 && Instant::now() < end {
            thread::yield_now();
        }
        assert_eq!(
            woken.0.load(Ordering::SeqCst),
            1,
            "the timer never woke the task"
        );
        assert!(Instant::now() >= due);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Some(at)) => assert_eq!(at, due),
            other => panic!("a due clock answered {other:?}"),
        }
        kinds.next();
    }
}
