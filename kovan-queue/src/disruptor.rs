use crate::utils::CacheAligned;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::thread;

/// A sequence number in the Disruptor.
///
/// Padded to prevent false sharing.
#[derive(Debug)]
pub struct Sequence {
    value: CacheAligned<AtomicI64>,
}

impl Sequence {
    /// Creates a new sequence with the given initial value.
    pub fn new(initial: i64) -> Self {
        Sequence {
            value: CacheAligned::new(AtomicI64::new(initial)),
        }
    }

    /// Gets the current value of the sequence.
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Acquire)
    }

    /// Sets the value of the sequence.
    pub fn set(&self, value: i64) {
        self.value.store(value, Ordering::Release);
    }

    /// Atomically compares and sets the value of the sequence.
    ///
    /// Returns `true` if the swap occurred.
    pub fn compare_and_set(&self, current: i64, new: i64) -> bool {
        self.value
            .compare_exchange(current, new, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }
}

/// Sequencer trait for claiming and publishing slots.
pub trait Sequencer: Send + Sync {
    /// Claims the next sequence.
    fn next(&self) -> i64;

    /// Publishes the given sequence.
    fn publish(&self, sequence: i64);

    /// Gets the current cursor value (highest published sequence).
    fn get_cursor(&self) -> i64;

    /// Adds gating sequences.
    fn add_gating_sequences(&self, sequences: Vec<Arc<Sequence>>);

    /// Gets the highest published sequence that is safe to read.
    fn get_highest_published_sequence(&self, next_sequence: i64, available_sequence: i64) -> i64;
}

/// Single Producer Sequencer.
///
/// Optimized for single-threaded publishing.
pub struct SingleProducerSequencer {
    /// The cursor for the sequencer (highest published sequence).
    cursor: Arc<Sequence>,
    /// The next sequence to be claimed.
    next_sequence: Sequence,
    /// Sequences to gate on (prevent wrapping).
    gating_sequences: UnsafeCell<Vec<Arc<Sequence>>>,
    /// Size of the buffer.
    buffer_size: usize,
    /// Strategy for waiting.
    wait_strategy: Arc<dyn WaitStrategy>,
}

unsafe impl Send for SingleProducerSequencer {}
unsafe impl Sync for SingleProducerSequencer {}

impl SingleProducerSequencer {
    pub fn new(buffer_size: usize, wait_strategy: Arc<dyn WaitStrategy>) -> Self {
        Self {
            cursor: Arc::new(Sequence::new(-1)),
            next_sequence: Sequence::new(-1),
            gating_sequences: UnsafeCell::new(Vec::new()),
            buffer_size,
            wait_strategy,
        }
    }
}

impl Sequencer for SingleProducerSequencer {
    fn next(&self) -> i64 {
        let next = self.next_sequence.get() + 1;
        self.next_sequence.set(next);

        let wrap_point = next - self.buffer_size as i64;
        let gating_sequences = unsafe { &*self.gating_sequences.get() };

        // Cached gating sequence check could be added here for optimization
        // For now, simple check
        let mut min_gating_sequence = i64::MAX;
        for seq in gating_sequences {
            let s = seq.get();
            if s < min_gating_sequence {
                min_gating_sequence = s;
            }
        }

        if wrap_point > min_gating_sequence {
            while wrap_point > min_gating_sequence {
                thread::yield_now();
                min_gating_sequence = i64::MAX;
                for seq in gating_sequences {
                    let s = seq.get();
                    if s < min_gating_sequence {
                        min_gating_sequence = s;
                    }
                }
            }
        }

        next
    }

    fn publish(&self, sequence: i64) {
        self.cursor.set(sequence);
        self.wait_strategy.signal_all_when_blocking();
    }

    fn get_cursor(&self) -> i64 {
        self.cursor.get()
    }

    fn add_gating_sequences(&self, sequences: Vec<Arc<Sequence>>) {
        unsafe {
            (*self.gating_sequences.get()).extend(sequences);
        }
    }

    fn get_highest_published_sequence(&self, _next_sequence: i64, available_sequence: i64) -> i64 {
        available_sequence
    }
}

/// Multi Producer Sequencer.
///
/// Thread-safe for multiple producers.
pub struct MultiProducerSequencer {
    /// Sequences to gate on.
    gating_sequences: UnsafeCell<Vec<Arc<Sequence>>>,
    /// Size of the buffer.
    buffer_size: usize,
    /// Strategy for waiting.
    wait_strategy: Arc<dyn WaitStrategy>,
    /// The sequence used for claiming slots.
    claim_sequence: AtomicI64,
    /// Buffer tracking published slots for availability.
    available_buffer: Box<[AtomicI64]>,
    /// Mask for fast modulo operations.
    mask: usize,
}

unsafe impl Send for MultiProducerSequencer {}
unsafe impl Sync for MultiProducerSequencer {}

impl MultiProducerSequencer {
    pub fn new(buffer_size: usize, wait_strategy: Arc<dyn WaitStrategy>) -> Self {
        let mut available_buffer = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            available_buffer.push(AtomicI64::new(-1));
        }

        Self {
            gating_sequences: UnsafeCell::new(Vec::new()),
            buffer_size,
            wait_strategy,
            claim_sequence: AtomicI64::new(-1),
            available_buffer: available_buffer.into_boxed_slice(),
            mask: buffer_size - 1,
        }
    }
}

impl Sequencer for MultiProducerSequencer {
    fn next(&self) -> i64 {
        let current = self.claim_sequence.fetch_add(1, Ordering::SeqCst);
        let next = current + 1;

        let wrap_point = next - self.buffer_size as i64;
        let gating_sequences = unsafe { &*self.gating_sequences.get() };

        let mut min_gating_sequence = i64::MAX;
        for seq in gating_sequences {
            let s = seq.get();
            if s < min_gating_sequence {
                min_gating_sequence = s;
            }
        }

        if wrap_point > min_gating_sequence {
            while wrap_point > min_gating_sequence {
                thread::yield_now();
                min_gating_sequence = i64::MAX;
                for seq in gating_sequences {
                    let s = seq.get();
                    if s < min_gating_sequence {
                        min_gating_sequence = s;
                    }
                }
            }
        }

        next
    }

    fn publish(&self, sequence: i64) {
        let index = (sequence as usize) & self.mask;
        self.available_buffer[index].store(sequence, Ordering::Release);
        self.wait_strategy.signal_all_when_blocking();
    }

    fn get_cursor(&self) -> i64 {
        self.claim_sequence.load(Ordering::Relaxed)
    }

    fn add_gating_sequences(&self, sequences: Vec<Arc<Sequence>>) {
        unsafe {
            (*self.gating_sequences.get()).extend(sequences);
        }
    }

    fn get_highest_published_sequence(&self, next_sequence: i64, available_sequence: i64) -> i64 {
        // Check if all sequences up to the requested one are published.
        // In MP, we must ensure contiguous availability.

        let mut sequence = next_sequence;
        while sequence <= available_sequence {
            if !self.is_published(sequence) {
                return sequence - 1;
            }
            sequence += 1;
        }
        available_sequence
    }
}

impl MultiProducerSequencer {
    fn is_published(&self, sequence: i64) -> bool {
        let index = (sequence as usize) & self.mask;
        self.available_buffer[index].load(Ordering::Acquire) == sequence
    }
}

/// Strategy for waiting for a sequence to be available.
pub trait WaitStrategy: Send + Sync {
    /// Waits for the given sequence to be available.
    fn wait_for(
        &self,
        sequence: i64,
        cursor: &Arc<dyn Sequencer>,
        dependent: &Arc<dyn Sequencer>,
        barrier: &ProcessingSequenceBarrier,
    ) -> Result<i64, AlertException>;

    /// Signals all waiting threads that the cursor has advanced.
    fn signal_all_when_blocking(&self);
}

#[derive(Debug, Clone, Copy)]
pub struct AlertException;

/// Busy Spin Wait Strategy.
///
/// Low latency but high CPU usage.
pub struct BusySpinWaitStrategy;

impl WaitStrategy for BusySpinWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        _cursor: &Arc<dyn Sequencer>,
        dependent: &Arc<dyn Sequencer>,
        barrier: &ProcessingSequenceBarrier,
    ) -> Result<i64, AlertException> {
        let mut available_sequence;
        loop {
            if barrier.is_alerted() {
                return Err(AlertException);
            }
            available_sequence = dependent.get_cursor();
            if available_sequence >= sequence {
                return Ok(available_sequence);
            }
            std::hint::spin_loop();
        }
    }

    fn signal_all_when_blocking(&self) {}
}

/// Yielding Wait Strategy.
///
/// Compromise between latency and CPU usage.
pub struct YieldingWaitStrategy;

impl WaitStrategy for YieldingWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        _cursor: &Arc<dyn Sequencer>,
        dependent: &Arc<dyn Sequencer>,
        barrier: &ProcessingSequenceBarrier,
    ) -> Result<i64, AlertException> {
        let mut counter = 100;
        let mut available_sequence;
        loop {
            if barrier.is_alerted() {
                return Err(AlertException);
            }
            available_sequence = dependent.get_cursor();
            if available_sequence >= sequence {
                return Ok(available_sequence);
            }

            counter -= 1;
            if counter == 0 {
                thread::yield_now();
                counter = 100;
            } else {
                std::hint::spin_loop();
            }
        }
    }

    fn signal_all_when_blocking(&self) {}
}

/// Blocking Wait Strategy.
///
/// Uses a lock and condition variable. Lowest CPU usage.
pub struct BlockingWaitStrategy {
    mutex: std::sync::Mutex<()>,
    condvar: std::sync::Condvar,
}

impl Default for BlockingWaitStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingWaitStrategy {
    /// Creates a new blocking wait strategy.
    pub fn new() -> Self {
        Self {
            mutex: std::sync::Mutex::new(()),
            condvar: std::sync::Condvar::new(),
        }
    }
}

impl WaitStrategy for BlockingWaitStrategy {
    fn wait_for(
        &self,
        sequence: i64,
        _cursor: &Arc<dyn Sequencer>,
        dependent: &Arc<dyn Sequencer>,
        barrier: &ProcessingSequenceBarrier,
    ) -> Result<i64, AlertException> {
        let mut available_sequence = dependent.get_cursor();
        if available_sequence < sequence {
            let mut guard = self.mutex.lock().unwrap();
            while dependent.get_cursor() < sequence {
                if barrier.is_alerted() {
                    return Err(AlertException);
                }
                guard = self.condvar.wait(guard).unwrap();
                // Re-acquire guard is automatic
                // Loop continues to check condition
            }
            available_sequence = dependent.get_cursor();
        }

        while available_sequence < sequence {
            if barrier.is_alerted() {
                return Err(AlertException);
            }
            available_sequence = dependent.get_cursor();
            // Busy spin fallback or check again
            thread::yield_now();
        }

        Ok(available_sequence)
    }

    fn signal_all_when_blocking(&self) {
        let _guard = self.mutex.lock().unwrap();
        self.condvar.notify_all();
    }
}

/// Coordination barrier for tracking dependencies.
pub struct ProcessingSequenceBarrier {
    /// Strategy for waiting.
    wait_strategy: Arc<dyn WaitStrategy>,
    /// The sequencer to wait on (dependent).
    dependent_sequencer: Arc<dyn Sequencer>,
    /// The sequencer of the ring buffer (cursor).
    cursor_sequencer: Arc<dyn Sequencer>,
    /// Whether the barrier has been alerted.
    alerted: AtomicBool,
}

impl ProcessingSequenceBarrier {
    /// Creates a new processing sequence barrier.
    pub fn new(
        wait_strategy: Arc<dyn WaitStrategy>,
        dependent_sequencer: Arc<dyn Sequencer>,
        cursor_sequencer: Arc<dyn Sequencer>,
    ) -> Self {
        Self {
            wait_strategy,
            dependent_sequencer,
            cursor_sequencer,
            alerted: AtomicBool::new(false),
        }
    }

    /// Waits for the given sequence to be available.
    pub fn wait_for(&self, sequence: i64) -> Result<i64, AlertException> {
        let available = self.wait_strategy.wait_for(
            sequence,
            &self.cursor_sequencer,
            &self.dependent_sequencer,
            self,
        )?;

        // Ensure the sequence is fully published (crucial for MP).
        Ok(self
            .cursor_sequencer
            .get_highest_published_sequence(sequence, available))
    }

    /// Returns true if the barrier has been alerted.
    pub fn is_alerted(&self) -> bool {
        self.alerted.load(Ordering::Relaxed)
    }

    /// Alerts the barrier, causing waiters to wake up.
    pub fn alert(&self) {
        self.alerted.store(true, Ordering::Relaxed);
        self.wait_strategy.signal_all_when_blocking();
    }

    /// Clears the alert status.
    pub fn clear_alert(&self) {
        self.alerted.store(false, Ordering::Relaxed);
    }
}

/// Event Handler trait.
pub trait EventHandler<T>: Send + Sync {
    /// Called when an event is available for processing.
    fn on_event(&self, event: &T, sequence: u64, end_of_batch: bool);
}

/// Ring Buffer.
pub struct RingBuffer<T> {
    /// The buffer of events.
    buffer: Box<[UnsafeCell<T>]>,
    /// Mask for fast modulo operations.
    mask: usize,
    /// The sequencer managing this buffer.
    sequencer: Arc<dyn Sequencer>,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new ring buffer with the given factory, size, and sequencer.
    pub fn new<F>(factory: F, size: usize, sequencer: Arc<dyn Sequencer>) -> Self
    where
        F: Fn() -> T,
    {
        let capacity = size.next_power_of_two();
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(UnsafeCell::new(factory()));
        }

        Self {
            buffer: buffer.into_boxed_slice(),
            mask: capacity - 1,
            sequencer,
        }
    }

    /// Adds gating sequences to the sequencer.
    pub fn add_gating_sequences(&self, sequences: Vec<Arc<Sequence>>) {
        self.sequencer.add_gating_sequences(sequences);
    }

    /// Gets a reference to the event at the given sequence.
    pub fn get(&self, sequence: i64) -> &T {
        unsafe { &*self.buffer[(sequence as usize) & self.mask].get() }
    }

    /// Gets a mutable reference to the event at the given sequence.
    pub fn get_mut(&mut self, sequence: i64) -> &mut T {
        unsafe { &mut *self.buffer[(sequence as usize) & self.mask].get() }
    }

    /// Unsafe access for high performance (internal use).
    ///
    /// # Safety
    ///
    /// The caller must ensure that the sequence number is valid and that
    /// no other thread is concurrently modifying the same slot.
    pub unsafe fn get_unchecked(&self, sequence: i64) -> &T {
        unsafe {
            &*self
                .buffer
                .get_unchecked((sequence as usize) & self.mask)
                .get()
        }
    }

    /// Unsafe mutable access for high performance (internal use).
    ///
    /// # Safety
    ///
    /// The caller must ensure that the sequence number is valid and that
    /// they have exclusive access to the slot (e.g., via the Disruptor protocol).
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_unchecked_mut(&self, sequence: i64) -> &mut T {
        unsafe {
            &mut *self
                .buffer
                .get_unchecked((sequence as usize) & self.mask)
                .get()
        }
    }

    /// Claims the next sequence in the ring buffer.
    pub fn next(&self) -> i64 {
        self.sequencer.next()
    }

    /// Publishes the sequence, making it available to consumers.
    pub fn publish(&self, sequence: i64) {
        self.sequencer.publish(sequence);
    }
}

/// Producer handle.
pub struct Producer<T> {
    ring_buffer: Arc<RingBuffer<T>>,
}

impl<T> Producer<T> {
    /// Publishes an event to the ring buffer.
    pub fn publish<F>(&mut self, update: F)
    where
        F: FnOnce(&mut T),
    {
        let sequence = self.ring_buffer.next();
        // SAFETY: We have claimed the sequence, so we have exclusive access to this slot.
        let event = unsafe { self.ring_buffer.get_unchecked_mut(sequence) };
        update(event);
        self.ring_buffer.publish(sequence);
    }
}

/// Batch Event Processor.
pub struct BatchEventProcessor<T> {
    /// The ring buffer to read from.
    ring_buffer: Arc<RingBuffer<T>>,
    /// The sequence of this processor.
    sequence: Arc<Sequence>,
    /// The barrier to wait on.
    barrier: Arc<ProcessingSequenceBarrier>,
    /// The handler to process events.
    handler: Arc<dyn EventHandler<T>>,
}

impl<T> BatchEventProcessor<T> {
    /// Creates a new batch event processor.
    pub fn new(
        ring_buffer: Arc<RingBuffer<T>>,
        barrier: Arc<ProcessingSequenceBarrier>,
        handler: Arc<dyn EventHandler<T>>,
    ) -> Self {
        Self {
            ring_buffer,
            sequence: Arc::new(Sequence::new(-1)),
            barrier,
            handler,
        }
    }

    /// Gets the sequence of the processor.
    pub fn get_sequence(&self) -> Arc<Sequence> {
        self.sequence.clone()
    }

    /// Runs the processor loop.
    pub fn run(&self) {
        let mut next_sequence = self.sequence.get() + 1;
        loop {
            match self.barrier.wait_for(next_sequence) {
                Ok(available_sequence) => {
                    while next_sequence <= available_sequence {
                        // SAFETY: The barrier guarantees that the sequence is available for reading.
                        let event = unsafe { self.ring_buffer.get_unchecked(next_sequence) };
                        self.handler.on_event(
                            event,
                            next_sequence as u64,
                            next_sequence == available_sequence,
                        );
                        next_sequence += 1;
                    }
                    self.sequence.set(available_sequence);
                }
                Err(_) => {
                    if self.barrier.is_alerted() {
                        break;
                    }
                }
            }
        }
    }
}

/// Disruptor Facade.
pub struct Disruptor<T> {
    /// The ring buffer.
    ring_buffer: Arc<RingBuffer<T>>,
    /// The registered event processors.
    processors: Vec<Arc<BatchEventProcessor<T>>>,
    /// Whether the disruptor has been started.
    started: bool,
    /// The wait strategy used.
    wait_strategy: Arc<dyn WaitStrategy>,
}

pub enum ProducerType {
    Single,
    Multi,
}

pub struct DisruptorBuilder<T, F> {
    /// Factory for creating events.
    factory: F,
    /// Size of the ring buffer.
    buffer_size: usize,
    /// Wait strategy to use.
    wait_strategy: Arc<dyn WaitStrategy>,
    /// Type of producer (Single or Multi).
    producer_type: ProducerType,
    _marker: std::marker::PhantomData<T>,
}

impl<T, F> DisruptorBuilder<T, F>
where
    F: Fn() -> T,
{
    /// Creates a new builder with the given factory.
    pub fn new(factory: F) -> Self {
        Self {
            factory,
            buffer_size: 1024,
            wait_strategy: Arc::new(BusySpinWaitStrategy),
            producer_type: ProducerType::Single,
            _marker: std::marker::PhantomData,
        }
    }

    /// Sets the buffer size.
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Sets the wait strategy.
    pub fn wait_strategy<W: WaitStrategy + 'static>(mut self, strategy: W) -> Self {
        self.wait_strategy = Arc::new(strategy);
        self
    }

    /// Sets the producer type to Single.
    pub fn single_producer(mut self) -> Self {
        self.producer_type = ProducerType::Single;
        self
    }

    /// Sets the producer type to Multi.
    pub fn multi_producer(mut self) -> Self {
        self.producer_type = ProducerType::Multi;
        self
    }

    /// Builds the Disruptor.
    pub fn build(self) -> Disruptor<T> {
        let sequencer: Arc<dyn Sequencer> = match self.producer_type {
            ProducerType::Single => Arc::new(SingleProducerSequencer::new(
                self.buffer_size,
                self.wait_strategy.clone(),
            )),
            ProducerType::Multi => Arc::new(MultiProducerSequencer::new(
                self.buffer_size,
                self.wait_strategy.clone(),
            )),
        };

        let ring_buffer = Arc::new(RingBuffer::new(self.factory, self.buffer_size, sequencer));
        Disruptor {
            ring_buffer,
            processors: Vec::new(),
            started: false,
            wait_strategy: self.wait_strategy,
        }
    }
}

impl<T: Send + Sync + 'static> Disruptor<T> {
    /// Creates a new builder for the Disruptor.
    pub fn builder<F>(factory: F) -> DisruptorBuilder<T, F>
    where
        F: Fn() -> T,
    {
        DisruptorBuilder::new(factory)
    }

    /// Registers an event handler.
    pub fn handle_events_with<H: EventHandler<T> + 'static>(&mut self, handler: H) -> &mut Self {
        // Create a barrier that waits on the ring buffer's sequencer.
        // Initially, the barrier depends on the sequencer itself (producers).

        let barrier = Arc::new(ProcessingSequenceBarrier::new(
            self.wait_strategy.clone(),
            self.ring_buffer.sequencer.clone(), // Dependent on cursor (producer)
            self.ring_buffer.sequencer.clone(),
        ));

        let processor = Arc::new(BatchEventProcessor::new(
            self.ring_buffer.clone(),
            barrier,
            Arc::new(handler),
        ));

        self.processors.push(processor);
        self
    }

    /// Starts the Disruptor and returns a Producer.
    pub fn start(mut self) -> Producer<T> {
        let mut gating_sequences = Vec::new();
        for processor in &self.processors {
            gating_sequences.push(processor.get_sequence());
            let p = processor.clone();
            thread::spawn(move || {
                p.run();
            });
        }

        // SAFETY: We have exclusive ownership and no publishers yet.
        // Safe to mutate RingBuffer to add gating sequences.
        // RingBuffer::add_gating_sequences is now safe (interior mutability in Sequencer)
        self.ring_buffer.add_gating_sequences(gating_sequences);

        self.started = true;

        Producer {
            ring_buffer: self.ring_buffer,
        }
    }
}
