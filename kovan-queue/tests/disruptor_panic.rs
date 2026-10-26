//! A `publish` whose closure panics still publishes its slot, so the
//! consumers go on and the producer's drop returns. It left the claimed
//! sequence unpublished: a multi-producer consumer waited at it for good,
//! and the drop that joins the consumers never returned.

use kovan_queue::disruptor::{Disruptor, EventHandler};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

struct Event {
    value: u64,
}

struct Seen(Arc<AtomicU64>);

impl EventHandler<Event> for Seen {
    fn on_event(&self, _event: &Event, _sequence: u64, _end_of_batch: bool) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[cfg_attr(miri, ignore)] // threads and spin waits
fn a_panicking_publish_still_publishes_its_slot() {
    for multi in [false, true] {
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let builder = Disruptor::builder(|| Event { value: 0 }).buffer_size(8);
            let builder = if multi {
                builder.multi_producer()
            } else {
                builder.single_producer()
            };
            let mut disruptor = builder.build();
            let seen = Arc::new(AtomicU64::new(0));
            disruptor.handle_events_with(Seen(Arc::clone(&seen)));
            let mut producer = disruptor.start();
            producer.publish(|e| e.value = 1);
            let panicked = panic::catch_unwind(AssertUnwindSafe(|| {
                producer.publish(|_| panic!("the update failed"));
            }));
            assert!(panicked.is_err());
            // Past the ring's size: a slot left unpublished stops this too.
            for value in 0..20 {
                producer.publish(|e| e.value = value);
            }
            drop(producer);
            let _ = done_tx.send(seen.load(Ordering::SeqCst));
        });
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(60)).ok(),
            Some(22),
            "multi producer {multi}: the events the handler saw"
        );
    }
}
