//! A consumer whose barrier is alerted (what the producer's drop does) hands
//! every event published before the alert to its handler before it stops,
//! with each wait strategy. The handler is held inside the first event while
//! the rest are published and the barrier is alerted, so a consumer that
//! stops at the alert with events left fails every time, not by chance.

use kovan_queue::disruptor::{
    BatchEventProcessor, BlockingWaitStrategy, BusySpinWaitStrategy, EventHandler,
    ProcessingSequenceBarrier, RingBuffer, Sequencer, SingleProducerSequencer, WaitStrategy,
    YieldingWaitStrategy,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const EVENTS: u64 = 500;
const DEADLINE: Duration = Duration::from_secs(60);

struct Event {
    value: u64,
}

/// Counts what it handles; held inside the first event until `go`.
struct Held {
    handled: Arc<AtomicU64>,
    arrived: Arc<AtomicBool>,
    go: Arc<AtomicBool>,
}

impl EventHandler<Event> for Held {
    fn on_event(&self, event: &Event, sequence: u64, _end_of_batch: bool) {
        if sequence == 0 {
            self.arrived.store(true, Ordering::SeqCst);
            let end = Instant::now() + DEADLINE;
            while !self.go.load(Ordering::SeqCst) && Instant::now() < end {
                thread::yield_now();
            }
        }
        assert_eq!(event.value, sequence, "an event read out of order");
        self.handled.fetch_add(1, Ordering::SeqCst);
    }
}

fn until(done: impl Fn() -> bool) -> bool {
    let end = Instant::now() + DEADLINE;
    while !done() {
        if Instant::now() > end {
            return false;
        }
        thread::yield_now();
    }
    true
}

/// Publishes the first event, holds the consumer inside it, publishes the
/// rest, alerts the barrier, lets the consumer go, and answers how many
/// events it handled before it stopped.
fn handled_after_an_alert(wait: Arc<dyn WaitStrategy>) -> u64 {
    let sequencer: Arc<dyn Sequencer> =
        Arc::new(SingleProducerSequencer::new(1024, Arc::clone(&wait)));
    let ring = Arc::new(RingBuffer::new(
        || Event { value: 0 },
        1024,
        Arc::clone(&sequencer),
    ));
    let barrier = Arc::new(ProcessingSequenceBarrier::new(
        wait,
        Arc::clone(&sequencer),
        Arc::clone(&sequencer),
    ));
    let (handled, arrived, go) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let processor = BatchEventProcessor::new(
        Arc::clone(&ring),
        Arc::clone(&barrier),
        Arc::new(Held {
            handled: Arc::clone(&handled),
            arrived: Arc::clone(&arrived),
            go: Arc::clone(&go),
        }),
    );
    ring.add_gating_sequences(vec![processor.get_sequence()]);
    let consumer = thread::spawn(move || processor.run());

    let publish = |value: u64| {
        let sequence = ring.next();
        // SAFETY: the sequence is claimed and not yet published, so no
        // consumer reads its slot.
        unsafe { ring.get_unchecked_mut(sequence) }.value = value;
        ring.publish(sequence);
    };
    publish(0);
    assert!(
        until(|| arrived.load(Ordering::SeqCst)),
        "the consumer never took the first event"
    );
    for value in 1..EVENTS {
        publish(value);
    }
    barrier.alert();
    go.store(true, Ordering::SeqCst);
    let end = Instant::now() + DEADLINE;
    while !consumer.is_finished() {
        assert!(Instant::now() < end, "the alerted consumer never stopped");
        thread::yield_now();
    }
    consumer.join().unwrap();
    handled.load(Ordering::SeqCst)
}

#[test]
#[cfg_attr(miri, ignore)] // threads and spin waits
fn an_alerted_busy_spin_consumer_hands_out_every_published_event() {
    assert_eq!(
        handled_after_an_alert(Arc::new(BusySpinWaitStrategy)),
        EVENTS
    );
}

#[test]
#[cfg_attr(miri, ignore)] // threads and spin waits
fn an_alerted_yielding_consumer_hands_out_every_published_event() {
    assert_eq!(
        handled_after_an_alert(Arc::new(YieldingWaitStrategy)),
        EVENTS
    );
}

#[test]
#[cfg_attr(miri, ignore)] // threads and a condition variable
fn an_alerted_blocking_consumer_hands_out_every_published_event() {
    assert_eq!(
        handled_after_an_alert(Arc::new(BlockingWaitStrategy::new())),
        EVENTS
    );
}
