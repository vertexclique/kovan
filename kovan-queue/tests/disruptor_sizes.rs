//! A disruptor of any buffer size hands every published event to its
//! handler, with either sequencer: the size is rounded up to a power of two,
//! at least one, alike for the ring and its sequencer. A multi-producer
//! sequencer sized 1000 while its ring held 1024 indexed its availability
//! record with the mask 999, so events 0 and 8 shared an entry, event 0
//! never read as published, and the consumer and then the producer stalled
//! for good; a size of 0 underflowed the mask.

use kovan_queue::disruptor::{Disruptor, EventHandler};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

struct Event {
    value: u64,
}

struct Count {
    sum: Arc<AtomicU64>,
    seen: Arc<AtomicU64>,
}

impl EventHandler<Event> for Count {
    fn on_event(&self, event: &Event, _sequence: u64, _end_of_batch: bool) {
        self.sum.fetch_add(event.value, Ordering::SeqCst);
        self.seen.fetch_add(1, Ordering::SeqCst);
    }
}

/// Publishes `events` events through a disruptor of `size` slots, drops its
/// producer (which joins the consumer), and answers how many the handler saw
/// and their sum, or `None` if that took longer than a minute.
fn run(size: usize, multi: bool, events: u64) -> Option<(u64, u64)> {
    let (done_tx, done_rx) = mpsc::channel();
    thread::spawn(move || {
        let builder = Disruptor::builder(|| Event { value: 0 }).buffer_size(size);
        let builder = if multi {
            builder.multi_producer()
        } else {
            builder.single_producer()
        };
        let mut disruptor = builder.build();
        let (sum, seen) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        disruptor.handle_events_with(Count {
            sum: Arc::clone(&sum),
            seen: Arc::clone(&seen),
        });
        let mut producer = disruptor.start();
        for value in 1..=events {
            producer.publish(|e| e.value = value);
        }
        drop(producer);
        let _ = done_tx.send((seen.load(Ordering::SeqCst), sum.load(Ordering::SeqCst)));
    });
    done_rx.recv_timeout(Duration::from_secs(60)).ok()
}

#[test]
#[cfg_attr(miri, ignore)] // threads and spin waits
fn every_size_hands_out_every_event_with_either_sequencer() {
    for multi in [false, true] {
        for size in [0, 1, 3, 1000, 1024] {
            let events = 2_500;
            let got = run(size, multi, events);
            assert_eq!(
                got,
                Some((events, events * (events + 1) / 2)),
                "size {size}, multi producer {multi}: the events the handler saw (count, sum)"
            );
        }
    }
}
