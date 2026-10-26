//! The queues' public operations from a thread-local destructor that runs
//! after the thread's exit (see kovan's `tests/support/thread_exit.rs`),
//! while other threads push and pop the same queue: every value comes out
//! once and alive, nothing panics, and every value is freed in the end.

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan::flush;
use kovan_queue::disruptor::{BusySpinWaitStrategy, Disruptor, EventHandler};
use kovan_queue::seg_queue::SegQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use thread_exit::{Val, after_exit, alive, eventually, freed_down_to, live, lock};

/// Pushes and pops after the exit while another thread pushes and pops the
/// same queue: what comes out is alive, and every value is freed in the end.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn seg_queue_operations_after_the_exit() {
    let _l = lock();
    let base = live();
    let queue = Arc::new(SegQueue::new());
    for _ in 0..64 {
        queue.push(Val::new());
    }
    let stop = Arc::new(AtomicBool::new(false));
    let churner = {
        let (queue, stop) = (Arc::clone(&queue), Arc::clone(&stop));
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..32 {
                    queue.push(Val::new());
                }
                for _ in 0..32 {
                    drop(queue.pop());
                }
                flush();
            }
        })
    };
    let q = Arc::clone(&queue);
    let checks = after_exit(move || {
        let mut ok = Vec::new();
        for _ in 0..200 {
            q.push(Val::new());
            q.push(Val::new());
            if let Some(v) = q.pop() {
                ok.push(alive(v.id));
            }
            if let Ok(v) = q.try_pop() {
                ok.push(alive(v.id));
            }
            let _ = q.len();
            let _ = q.is_empty();
        }
        flush();
        ok
    });
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();
    assert!(
        checks.iter().all(|&ok| ok),
        "a pop after the thread's exit handed out a dropped value"
    );
    while queue.pop().is_some() {}
    drop(queue);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// A queue dropped after the exit frees the values it still holds.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_seg_queue_dropped_after_the_exit_is_freed() {
    let _l = lock();
    let base = live();
    let queue = SegQueue::new();
    for _ in 0..100 {
        queue.push(Val::new());
    }
    after_exit(move || {
        drop(queue.pop());
        drop(queue);
    });
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

struct Event {
    data: u64,
}

struct Sum(Arc<AtomicU64>);

impl EventHandler<Event> for Sum {
    fn on_event(&self, event: &Event, _sequence: u64, _end_of_batch: bool) {
        self.0.fetch_add(event.data, Ordering::Relaxed);
    }
}

/// A disruptor's producer publishing after the exit (its sequencer reads the
/// gating sequences through an `Atom`): every event reaches the handler.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_disruptor_publishes_after_the_exit() {
    let _l = lock();
    let sum = Arc::new(AtomicU64::new(0));
    let mut disruptor = Disruptor::builder(|| Event { data: 0 })
        .buffer_size(64)
        .wait_strategy(BusySpinWaitStrategy)
        .build();
    disruptor.handle_events_with(Sum(Arc::clone(&sum)));
    let mut producer = disruptor.start();
    after_exit(move || {
        for i in 0..1000 {
            producer.publish(|event| event.data = i);
        }
    });
    let expected = (0..1000).sum::<u64>();
    assert!(
        eventually(|| sum.load(Ordering::Relaxed) == expected),
        "events published after the thread's exit were lost: {} of {expected}",
        sum.load(Ordering::Relaxed)
    );
}
