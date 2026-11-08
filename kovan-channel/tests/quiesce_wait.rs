//! A thread blocked in a channel operation holds no reservation.
//!
//! The receive (or send, or select) pins to check the queue, and a thread's
//! reservation stays published after its guard drops, so a thread that parks
//! in `recv` held every batch retired at or after its epoch for as long as it
//! stayed parked. The wait now leaves the reclamation protocol before it parks
//! (`kovan::quiesce`). Each case below pins a thread, blocks it in one
//! operation, retires values from another thread and flushes: the values must
//! be freed while the blocked thread stays blocked.
//!
//! The batch must be eligible for the blocked thread's slot, or the test would
//! pass without the fix: the values are made before the blocked thread pins
//! (births at or below its epoch), and the blocked thread pins on purpose
//! before it blocks, so its slot is active whatever the channel code does.

use kovan::{RetiredNode, flush, retire};
use kovan_channel::{RecvDeadline, after, bounded, select, unbounded};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// More than one batch (64 values each), so batches are submitted by the
/// retires themselves and the rest by the flush.
const VALUES: usize = 130;
const DEADLINE: Duration = Duration::from_secs(30);

static LOCK: Mutex<()> = Mutex::new(());

/// The cases run one at a time, each taking this before it makes its channel
/// (which pins): another case's thread, blocked or waiting here, would hold
/// the batches this one waits to see freed.
fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[repr(C)]
struct Counted {
    retired: RetiredNode,
    live: Arc<AtomicUsize>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

fn make(live: &Arc<AtomicUsize>) -> Vec<usize> {
    (0..VALUES)
        .map(|_| {
            live.fetch_add(1, Ordering::SeqCst);
            Box::into_raw(Box::new(Counted {
                retired: RetiredNode::new(),
                live: Arc::clone(live),
            })) as usize
        })
        .collect()
}

/// A thread blocked in one channel operation, and the way to end it.
struct Blocked {
    thread: JoinHandle<()>,
    /// Set when the operation returned.
    returned: Arc<AtomicBool>,
    wake: Box<dyn FnOnce()>,
}

/// Starts `blocking` on a thread that pins first and tells the test just
/// before it calls in; `wake` makes the call return.
fn block(blocking: impl FnOnce() + Send + 'static, wake: impl FnOnce() + 'static) -> Blocked {
    let (pinned, returned) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let (p, r) = (Arc::clone(&pinned), Arc::clone(&returned));
    let thread = thread::spawn(move || {
        drop(kovan::pin());
        p.store(true, Ordering::SeqCst);
        blocking();
        r.store(true, Ordering::SeqCst);
    });
    while !pinned.load(Ordering::SeqCst) {
        thread::yield_now();
    }
    Blocked {
        thread,
        returned,
        wake: Box::new(wake),
    }
}

/// Retires `VALUES` values made before `start` ran its thread from a thread
/// of its own, flushes until they are freed, and checks the blocked thread
/// stayed blocked meanwhile.
fn freed_while_blocked(start: impl FnOnce() -> Blocked) {
    let live = Arc::new(AtomicUsize::new(0));
    let values = make(&live);
    let blocked = start();
    // Creating the channel pinned this thread, which now blocks in a join.
    kovan::quiesce();
    let l = Arc::clone(&live);
    let returned = Arc::clone(&blocked.returned);
    let freed = thread::spawn(move || {
        for v in values {
            // SAFETY: made above by `Box::into_raw` of a `repr(C)` value whose
            // first field is the `RetiredNode`, retired once.
            unsafe { retire(v as *mut Counted) };
        }
        let end = Instant::now() + DEADLINE;
        while l.load(Ordering::SeqCst) != 0 && Instant::now() < end {
            flush();
            thread::yield_now();
        }
        (l.load(Ordering::SeqCst), returned.load(Ordering::SeqCst))
    })
    .join()
    .unwrap();
    let Blocked { thread, wake, .. } = blocked;
    wake();
    thread.join().unwrap();
    assert!(
        !freed.1,
        "the blocked operation returned before it was woken"
    );
    assert_eq!(
        freed.0, 0,
        "values retired while a thread was blocked in a channel operation stayed alive"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_receiver_blocked_in_recv_holds_nothing() {
    let _l = lock();
    let (s, r) = unbounded::<u32>();
    freed_while_blocked(|| block(move || assert_eq!(r.recv(), Some(1)), move || s.send(1)));
}

#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_receiver_blocked_in_recv_deadline_holds_nothing() {
    let _l = lock();
    let (s, r) = unbounded::<u32>();
    freed_while_blocked(|| {
        block(
            move || {
                let end = Instant::now() + Duration::from_secs(120);
                assert_eq!(r.recv_deadline(end), RecvDeadline::Msg(1));
            },
            move || s.send(1),
        )
    });
}

#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_receiver_blocked_on_a_bounded_channel_holds_nothing() {
    let _l = lock();
    let (s, r) = bounded::<u32>(2);
    freed_while_blocked(|| block(move || assert_eq!(r.recv(), Some(1)), move || s.send(1)));
}

#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_sender_blocked_on_a_full_channel_holds_nothing() {
    let _l = lock();
    let (s, r) = bounded::<u32>(1);
    s.send(0);
    freed_while_blocked(|| {
        block(
            move || s.send(1),
            move || {
                assert_eq!(r.recv(), Some(0));
                assert_eq!(r.recv(), Some(1));
            },
        )
    });
}

#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_select_blocked_on_two_channels_holds_nothing() {
    let _l = lock();
    let (s1, r1) = unbounded::<u32>();
    let (_s2, r2) = unbounded::<u32>();
    freed_while_blocked(|| {
        block(
            move || {
                select! {
                    v = r1 => assert_eq!(v, 1),
                    v = r2 => assert_eq!(v, 2),
                }
            },
            move || s1.send(1),
        )
    });
}

#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_receiver_sleeping_on_a_timer_holds_nothing() {
    let _l = lock();
    let timer = after(Duration::from_secs(3));
    freed_while_blocked(|| {
        block(
            move || {
                timer.recv();
            },
            || {},
        )
    });
}

/// One million messages between two threads, each blocking in `recv` for the
/// answer: every message arrives once, in order, in both directions.
#[test]
#[cfg_attr(miri, ignore)] // a million messages
fn a_ping_pong_of_a_million_messages_delivers_each_once_in_order() {
    const MESSAGES: u64 = 1_000_000;
    let _l = lock();
    let (ping_s, ping_r) = unbounded::<u64>();
    let (pong_s, pong_r) = unbounded::<u64>();
    let echo = thread::spawn(move || {
        let mut expected = 0;
        while let Some(v) = ping_r.recv() {
            assert_eq!(v, expected, "a ping arrived out of order or twice");
            expected += 1;
            pong_s.send(v);
        }
        expected
    });
    for i in 0..MESSAGES / 2 {
        ping_s.send(i);
        assert_eq!(
            pong_r.recv(),
            Some(i),
            "a pong arrived out of order or twice"
        );
    }
    drop(ping_s);
    assert_eq!(echo.join().unwrap(), MESSAGES / 2);
    assert_eq!(pong_r.recv(), None);
}

/// Several senders, one blocking receiver: each sender's messages arrive
/// exactly once and in the order it sent them.
#[test]
#[cfg_attr(miri, ignore)] // threads, many messages
fn blocking_receives_keep_each_senders_order() {
    const SENDERS: usize = 4;
    const EACH: u64 = 100_000;
    let _l = lock();
    let (s, r) = unbounded::<(usize, u64)>();
    let senders: Vec<_> = (0..SENDERS)
        .map(|id| {
            let s = s.clone();
            thread::spawn(move || {
                for i in 0..EACH {
                    s.send((id, i));
                    // Let the receiver drain and park now and then.
                    if i % 1_000 == 0 {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();
    drop(s);
    let mut next = [0u64; SENDERS];
    while let Some((id, i)) = r.recv() {
        assert_eq!(
            i, next[id],
            "sender {id}: a message arrived out of order or twice"
        );
        next[id] += 1;
    }
    for t in senders {
        t.join().unwrap();
    }
    assert!(
        next.iter().all(|&n| n == EACH),
        "a message was lost: {next:?}"
    );
}
