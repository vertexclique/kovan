//! The channels' public operations from a thread-local destructor that runs
//! after the thread's exit (see kovan's `tests/support/thread_exit.rs`),
//! with a peer thread on the other end: every value arrives once and alive,
//! nothing panics, and every value is freed in the end, a channel dropped
//! there included (its drop retires its nodes and flushes).

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_channel::{RecvDeadline, bounded, unbounded};
use std::thread;
use std::time::{Duration, Instant};
use thread_exit::{DEADLINE, Val, after_exit, alive, freed_down_to, live, lock};

const MESSAGES: usize = 500;

macro_rules! channel_after_the_exit {
    ($both_ways:ident, $dropped:ident, $make:expr) => {
        /// Sends and receives after the exit, a peer thread echoing back:
        /// every value arrives alive, none is lost, every one is freed.
        #[test]
        #[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
        fn $both_ways() {
            let _l = lock();
            let base = live();
            let (to_peer, from_late) = $make;
            let (to_late, from_peer) = $make;
            let peer = thread::spawn(move || {
                let mut echoed = 0;
                while let Some(v) = from_late.recv() {
                    assert!(alive(v.id), "the peer received a dropped value");
                    to_late.send(v);
                    echoed += 1;
                }
                echoed
            });
            let (received, all_alive) = after_exit(move || {
                let mut received = 0;
                let mut all_alive = true;
                for i in 0..MESSAGES {
                    to_peer.send(Val::new());
                    let back = if i % 3 == 0 {
                        match from_peer.recv_deadline(Instant::now() + DEADLINE) {
                            RecvDeadline::Msg(v) => Some(v),
                            RecvDeadline::Timeout | RecvDeadline::Disconnected => None,
                        }
                    } else if i % 3 == 1 {
                        let end = Instant::now() + DEADLINE;
                        loop {
                            if let Some(v) = from_peer.try_recv() {
                                break Some(v);
                            }
                            if Instant::now() > end {
                                break None;
                            }
                            thread::yield_now();
                        }
                    } else {
                        from_peer.recv()
                    };
                    if let Some(v) = back {
                        all_alive &= alive(v.id);
                        received += 1;
                    }
                }
                drop(to_peer);
                (received, all_alive)
            });
            let echoed = peer.join().unwrap();
            assert_eq!(
                echoed, MESSAGES,
                "the peer lost values sent after the thread's exit"
            );
            assert_eq!(
                received, MESSAGES,
                "values sent back were lost after the thread's exit"
            );
            assert!(
                all_alive,
                "a value received after the thread's exit was dropped"
            );
            assert!(
                freed_down_to(base),
                "values outlived the test: {}",
                live() - base
            );
        }

        /// A channel whose last handles drop after the exit, holding values,
        /// frees them.
        #[test]
        #[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
        fn $dropped() {
            let _l = lock();
            let base = live();
            let (tx, rx) = $make;
            for _ in 0..50 {
                tx.send(Val::new());
            }
            after_exit(move || {
                drop(rx.recv());
                drop(tx);
                drop(rx);
            });
            assert!(
                freed_down_to(base),
                "values outlived the test: {}",
                live() - base
            );
        }
    };
}

channel_after_the_exit!(
    unbounded_after_the_exit,
    an_unbounded_channel_dropped_after_the_exit_is_freed,
    unbounded::<Val>()
);
channel_after_the_exit!(
    bounded_after_the_exit,
    a_bounded_channel_dropped_after_the_exit_is_freed,
    bounded::<Val>(64)
);

/// The receive deadline after the exit still ends at its deadline.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_receive_deadline_after_the_exit_ends() {
    let _l = lock();
    let (tx, rx) = unbounded::<Val>();
    let waited = after_exit(move || {
        let start = Instant::now();
        let got = rx.recv_deadline(start + Duration::from_millis(20));
        drop(tx);
        (matches!(got, RecvDeadline::Timeout), start.elapsed())
    });
    assert!(waited.0, "a receive with nothing sent did not time out");
    assert!(
        waited.1 >= Duration::from_millis(20),
        "the deadline ended early"
    );
}
