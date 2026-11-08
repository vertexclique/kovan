//! A consumer blocked on the blocking wait strategy's condition variable holds no
//! reservation: values retired from another thread while it waits are freed.
//!
//! The consumer pins on purpose before it runs, so its slot is active whatever the
//! wait does, and the values are made before it pins (births at or below its epoch):
//! without `kovan::quiesce` in the wait, they stay alive for as long as it waits.

use kovan::{RetiredNode, flush, retire};
use kovan_queue::disruptor::{
    BatchEventProcessor, BlockingWaitStrategy, EventHandler, ProcessingSequenceBarrier, RingBuffer,
    Sequencer, SingleProducerSequencer, WaitStrategy,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const VALUES: usize = 130;
const DEADLINE: Duration = Duration::from_secs(30);

struct Event;

struct Nothing;

impl EventHandler<Event> for Nothing {
    fn on_event(&self, _event: &Event, _sequence: u64, _end_of_batch: bool) {}
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

#[test]
#[cfg_attr(miri, ignore)] // threads and a condition variable
fn a_consumer_blocked_on_the_condition_variable_holds_nothing() {
    let live = Arc::new(AtomicUsize::new(0));
    let values: Vec<usize> = (0..VALUES)
        .map(|_| {
            live.fetch_add(1, Ordering::SeqCst);
            Box::into_raw(Box::new(Counted {
                retired: RetiredNode::new(),
                live: Arc::clone(&live),
            })) as usize
        })
        .collect();

    let wait: Arc<dyn WaitStrategy> = Arc::new(BlockingWaitStrategy::new());
    let sequencer: Arc<dyn Sequencer> =
        Arc::new(SingleProducerSequencer::new(1024, Arc::clone(&wait)));
    let ring = Arc::new(RingBuffer::new(|| Event, 1024, Arc::clone(&sequencer)));
    let barrier = Arc::new(ProcessingSequenceBarrier::new(
        wait,
        Arc::clone(&sequencer),
        Arc::clone(&sequencer),
    ));
    let processor = BatchEventProcessor::new(ring, Arc::clone(&barrier), Arc::new(Nothing));
    let pinned = Arc::new(AtomicBool::new(false));
    let p = Arc::clone(&pinned);
    let consumer = thread::spawn(move || {
        drop(kovan::pin());
        p.store(true, Ordering::SeqCst);
        processor.run();
    });
    while !pinned.load(Ordering::SeqCst) {
        thread::yield_now();
    }
    // This thread's own reservation (the ring's setup pinned it) would hold the
    // batches too: it only waits for the retiring thread from here.
    kovan::quiesce();

    let l = Arc::clone(&live);
    let left = thread::spawn(move || {
        for v in values {
            // SAFETY: made above by `Box::into_raw` of a `repr(C)` value whose first
            // field is the `RetiredNode`, retired once.
            unsafe { retire(v as *mut Counted) };
        }
        let end = Instant::now() + DEADLINE;
        while l.load(Ordering::SeqCst) != 0 && Instant::now() < end {
            flush();
            thread::yield_now();
        }
        l.load(Ordering::SeqCst)
    })
    .join()
    .unwrap();

    let stopped = Instant::now() + DEADLINE;
    barrier.alert();
    while !consumer.is_finished() {
        assert!(
            Instant::now() < stopped,
            "the alerted consumer never stopped"
        );
        thread::yield_now();
    }
    consumer.join().unwrap();
    assert_eq!(
        left, 0,
        "values retired while a consumer waited on the condition variable stayed alive"
    );
}
