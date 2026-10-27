//! Workloads in the shape an async runtime gives them: worker threads, one per core, that hand
//! operations to each other's inboxes, send messages to a task on another worker, publish a
//! value many tasks read, keep a ring of the latest values for every receiver, register and
//! remove the waiter of every operation in flight, block on a channel, and open and close a
//! queue per connection; and that park once they run out of work.
//!
//! Shared by `tests/runtime_workloads.rs`, which checks that each workload delivers everything
//! intact and reports what it holds while it runs and while its workers are parked, and by the
//! `runtime_workloads` (time), `runtime_latency` (the time of single rounds) and
//! `runtime_memory` (bytes) benches.

#![allow(dead_code)]

use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use kovan::{Atom, Atomic, RetiredNode, Shared, alloc_node, drop_node, pin, retire_node};
use kovan_channel::flavors::RecvDeadline;
use kovan_queue::seg_queue::SegQueue;

/// What a worker does before it parks for want of work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idle {
    /// Parks as it is, holding what it retired and has not handed to reclamation.
    Hold,
    /// Calls `kovan::flush` first.
    Flush,
}

impl Idle {
    pub fn name(self) -> &'static str {
        match self {
            Idle::Hold => "hold",
            Idle::Flush => "flush",
        }
    }
}

/// What a worker records of its rounds: nothing (`()`), for time and memory, or the time of
/// each round (`Rounds`), for latency.
pub trait Recorder: Default + Send + 'static {
    fn round<T>(&mut self, f: impl FnOnce() -> T) -> T;
    fn merge(&mut self, other: Self);
}

impl Recorder for () {
    #[inline(always)]
    fn round<T>(&mut self, f: impl FnOnce() -> T) -> T {
        f()
    }

    fn merge(&mut self, _: Self) {}
}

/// The time of each round, in buckets of a power of two split in eight: a quantile it answers
/// is the top of its bucket, at most 12.5% above the true time.
pub struct Rounds {
    counts: Box<[u64; 512]>,
    n: u64,
}

impl Default for Rounds {
    fn default() -> Self {
        Self {
            counts: Box::new([0; 512]),
            n: 0,
        }
    }
}

impl Rounds {
    fn bucket(nanos: u64) -> usize {
        if nanos < 8 {
            return nanos as usize;
        }
        let exp = 63 - nanos.leading_zeros() as u64;
        ((exp - 2) * 8 + ((nanos >> (exp - 3)) & 7)) as usize
    }

    fn low(bucket: usize) -> u64 {
        if bucket < 8 {
            return bucket as u64;
        }
        (8 + bucket as u64 % 8)
            .checked_shl(bucket as u32 / 8 - 1)
            .unwrap_or(u64::MAX)
    }

    /// The time no more than a share `1 - q` of the rounds took longer than, in nanoseconds.
    pub fn quantile(&self, q: f64) -> u64 {
        let rank = ((q * self.n as f64).ceil() as u64).clamp(1, self.n.max(1));
        let mut seen = 0;
        for (bucket, &count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Self::low(bucket + 1).saturating_sub(1);
            }
        }
        0
    }

    pub fn len(&self) -> u64 {
        self.n
    }
}

impl Recorder for Rounds {
    #[inline(always)]
    fn round<T>(&mut self, f: impl FnOnce() -> T) -> T {
        let began = Instant::now();
        let out = f();
        let nanos = began.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.counts[Self::bucket(nanos)] += 1;
        self.n += 1;
        out
    }

    fn merge(&mut self, other: Self) {
        for (mine, theirs) in self.counts.iter_mut().zip(other.counts.iter()) {
            *mine += theirs;
        }
        self.n += other.n;
    }
}

/// A workload: what each worker does, and the check once all are done.
pub trait Workload: Send + Sync + 'static {
    /// The work of `worker` (of `threads`) for `rounds` rounds.
    fn work<R: Recorder>(&self, worker: usize, threads: usize, rounds: usize, rec: &mut R);
    /// Panics unless everything the workers sent arrived, intact and in order.
    fn check(&self, threads: usize, rounds: usize);
}

/// What `run_workers` measured: the time of the work and the merged records of the rounds.
pub struct Run<R> {
    pub took: Duration,
    pub rounds: R,
}

/// Runs `load` on `threads` workers at once for `rounds` rounds each. Then each worker parks as
/// an idle runtime worker does (after a flush under `Idle::Flush`), and `at_rest` runs while all
/// of them are parked; then they wake, flush and exit, and the workload is checked. The time is
/// from the moment every worker started to the moment the last one finished its rounds.
pub fn run_workers<L: Workload, R: Recorder>(
    load: &Arc<L>,
    threads: usize,
    rounds: usize,
    idle: Idle,
    at_rest: impl FnOnce(),
) -> Run<R> {
    let start = Arc::new(Barrier::new(threads + 1));
    let done = Arc::new(Barrier::new(threads + 1));
    let wake = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (load, start, done, wake) = (
                Arc::clone(load),
                Arc::clone(&start),
                Arc::clone(&done),
                Arc::clone(&wake),
            );
            std::thread::spawn(move || {
                let mut rec = R::default();
                start.wait();
                // A worker that fails still meets the others at the barriers, so the run ends
                // and the failure is raised by the join instead of hanging the others.
                let worked =
                    catch_unwind(AssertUnwindSafe(|| load.work(t, threads, rounds, &mut rec)));
                if idle == Idle::Flush {
                    kovan::flush();
                }
                done.wait();
                wake.wait();
                kovan::flush();
                if let Err(panic) = worked {
                    resume_unwind(panic);
                }
                rec
            })
        })
        .collect();
    start.wait();
    let began = Instant::now();
    done.wait();
    let took = began.elapsed();
    at_rest();
    wake.wait();
    let mut merged = R::default();
    for h in handles {
        merged.merge(h.join().expect("a worker failed"));
    }
    load.check(threads, rounds);
    Run {
        took,
        rounds: merged,
    }
}

/// Waits for `ready` to give a value, spinning, then yielding, and fails after a minute: a peer
/// that died leaves a loud failure, not a hang.
fn until<T>(what: &str, mut ready: impl FnMut() -> Option<T>) -> T {
    let mut spins = 0u32;
    let mut deadline = None;
    loop {
        if let Some(v) = ready() {
            return v;
        }
        if spins < 64 {
            spins += 1;
            std::hint::spin_loop();
            continue;
        }
        let at = *deadline.get_or_insert_with(|| Instant::now() + Duration::from_secs(60));
        assert!(Instant::now() < at, "{what}: nothing for a minute");
        std::thread::yield_now();
    }
}

/// Messages a sender may have sent that the receiver has not taken yet: the bound a bounded
/// channel keeps.
pub const IN_FLIGHT: usize = 64;

/// A message of `S` bytes (24 or more): the sequence number of its sender at the front and at the back
/// (so a torn message shows), the sender in the byte before the back.
fn message<const S: usize>(from: usize, seq: u64) -> [u8; S] {
    let mut m = [from as u8; S];
    m[..8].copy_from_slice(&seq.to_le_bytes());
    m[S - 8..].copy_from_slice(&seq.to_le_bytes());
    m
}

fn read_message<const S: usize>(m: &[u8; S]) -> (usize, u64) {
    let seq = u64::from_le_bytes(m[..8].try_into().unwrap());
    assert_eq!(seq.to_le_bytes(), m[S - 8..], "a torn message");
    (m[S - 9] as usize, seq)
}

/// Takes messages from workers `1..threads`, each sender's in the order it sent them.
struct InOrder(Vec<u64>);

impl InOrder {
    fn new(threads: usize) -> Self {
        Self(vec![0; threads])
    }

    fn take(&mut self, (from, seq): (usize, u64)) {
        assert!(from >= 1 && from < self.0.len(), "a message from no sender");
        assert_eq!(seq, self.0[from], "a message of worker {from} out of order");
        self.0[from] += 1;
    }
}

// The workloads. ------------------------------------------------------------------------------

/// An operation one worker hands another, the size of a timer or IO registration.
#[derive(Clone, Copy)]
struct Op {
    from: u64,
    seq: u64,
    sum: u64,
}

const OP_SUM: u64 = 0x9e37_79b9_7f4a_7c15;

/// Every worker has an inbox; each round a worker hands an operation to another worker's inbox
/// and runs what its own inbox holds.
pub struct Inbox {
    queues: Vec<SegQueue<Op>>,
    ran: AtomicUsize,
}

impl Inbox {
    pub fn new(threads: usize) -> Self {
        assert!(threads >= 2, "an inbox workload needs two workers");
        Self {
            queues: (0..threads).map(|_| SegQueue::new()).collect(),
            ran: AtomicUsize::new(0),
        }
    }

    fn drain(&self, worker: usize) {
        while let Some(op) = self.queues[worker].pop() {
            assert_eq!(op.sum, op.from ^ op.seq ^ OP_SUM, "a torn operation");
            self.ran.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Workload for Inbox {
    fn work<R: Recorder>(&self, worker: usize, threads: usize, rounds: usize, rec: &mut R) {
        for i in 0..rounds {
            rec.round(|| {
                let to = (worker + 1 + i % (threads - 1)) % threads;
                let (from, seq) = (worker as u64, i as u64);
                self.queues[to].push(Op {
                    from,
                    seq,
                    sum: from ^ seq ^ OP_SUM,
                });
                self.drain(worker);
            });
        }
    }

    fn check(&self, threads: usize, rounds: usize) {
        (0..threads).for_each(|t| self.drain(t));
        assert_eq!(
            self.ran.load(Ordering::Relaxed),
            threads * rounds,
            "operations lost"
        );
    }
}

/// Workers `1..` send messages of `S` bytes to a task on worker 0 through a queue, at most
/// `IN_FLIGHT` waiting.
pub struct Messages<const S: usize> {
    queue: SegQueue<[u8; S]>,
    waiting: AtomicUsize,
    taken: AtomicUsize,
}

impl<const S: usize> Messages<S> {
    pub fn new(threads: usize) -> Self {
        assert!((2..256).contains(&threads) && S >= 24);
        Self {
            queue: SegQueue::new(),
            waiting: AtomicUsize::new(0),
            taken: AtomicUsize::new(0),
        }
    }
}

impl<const S: usize> Workload for Messages<S> {
    fn work<R: Recorder>(&self, worker: usize, threads: usize, rounds: usize, rec: &mut R) {
        if worker == 0 {
            let mut order = InOrder::new(threads);
            for _ in 0..(threads - 1) * rounds {
                let m = rec.round(|| until("a message", || self.queue.pop()));
                self.waiting.fetch_sub(1, Ordering::AcqRel);
                order.take(read_message(&m));
                self.taken.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            for i in 0..rounds {
                until("room for a message", || {
                    (self.waiting.load(Ordering::Acquire) < IN_FLIGHT).then_some(())
                });
                self.waiting.fetch_add(1, Ordering::AcqRel);
                rec.round(|| self.queue.push(message::<S>(worker, i as u64)));
            }
        }
    }

    fn check(&self, threads: usize, rounds: usize) {
        assert_eq!(
            self.taken.load(Ordering::Relaxed),
            (threads - 1) * rounds,
            "messages lost"
        );
        assert!(self.queue.pop().is_none(), "a message nobody sent");
    }
}

/// Worker 0 publishes a value of `S` bytes each round, the others read the latest one: the
/// shape of a watched configuration.
pub struct Watch<const S: usize> {
    value: Atom<[u8; S]>,
}

impl<const S: usize> Watch<S> {
    pub fn new(_threads: usize) -> Self {
        assert!(S >= 24);
        Self {
            value: Atom::new(message::<S>(0, 0)),
        }
    }
}

impl<const S: usize> Workload for Watch<S> {
    fn work<R: Recorder>(&self, worker: usize, _threads: usize, rounds: usize, rec: &mut R) {
        if worker == 0 {
            for v in 1..=rounds as u64 {
                rec.round(|| self.value.store(message::<S>(0, v)));
            }
        } else {
            let mut last = 0;
            for _ in 0..rounds {
                let (_, v) = rec.round(|| read_message(&self.value.load()));
                assert!(v >= last, "a value older than one already read");
                last = v;
            }
        }
    }

    fn check(&self, _threads: usize, rounds: usize) {
        assert_eq!(
            read_message(&self.value.load()).1,
            rounds as u64,
            "the last value is not the latest"
        );
    }
}

/// The slots of the ring of `Broadcast`.
const RING: usize = 16;

/// A value in the ring: its node, its sequence number, and `S` bytes.
#[repr(C)]
struct Slot<const S: usize> {
    retired: RetiredNode,
    seq: u64,
    payload: [u8; S],
}

/// Worker 0 sends a value of `S` bytes each round into a ring of the latest `RING`, retiring
/// the one it replaces; the others read the ring: the shape of a broadcast channel.
pub struct Broadcast<const S: usize> {
    ring: Box<[Atomic<Slot<S>>]>,
}

impl<const S: usize> Broadcast<S> {
    pub fn new(_threads: usize) -> Self {
        let ring = (0..RING)
            .map(|_| {
                Atomic::new(alloc_node(Slot {
                    retired: RetiredNode::new(),
                    seq: 0,
                    payload: [0; S],
                }))
            })
            .collect();
        Self { ring }
    }
}

impl<const S: usize> Workload for Broadcast<S> {
    fn work<R: Recorder>(&self, worker: usize, _threads: usize, rounds: usize, rec: &mut R) {
        if worker == 0 {
            for seq in 1..=rounds as u64 {
                rec.round(|| {
                    let node = alloc_node(Slot {
                        retired: RetiredNode::new(),
                        seq,
                        payload: [seq as u8; S],
                    });
                    let guard = pin();
                    // SAFETY: `node` is a fresh node from `alloc_node`.
                    let old = self.ring[seq as usize % RING].swap(
                        unsafe { Shared::from_raw(node) },
                        Ordering::AcqRel,
                        &guard,
                    );
                    // SAFETY: the swap unlinked `old`, retired once, here.
                    unsafe { retire_node(old.as_raw()) };
                });
            }
        } else {
            for i in 0..rounds {
                rec.round(|| {
                    let guard = pin();
                    let cur = self.ring[i % RING].load(Ordering::Acquire, &guard);
                    // SAFETY: loaded under the guard, a node the ring holds or retired since.
                    let slot = unsafe { cur.deref() };
                    assert!(
                        slot.seq == 0 || slot.seq as usize % RING == i % RING,
                        "a value in the wrong slot"
                    );
                    assert!(
                        slot.payload[0] == slot.seq as u8 && slot.payload[S - 1] == slot.seq as u8,
                        "a torn value"
                    );
                });
            }
        }
    }

    fn check(&self, _threads: usize, rounds: usize) {
        let guard = pin();
        for (index, slot) in self.ring.iter().enumerate() {
            // SAFETY: under the guard; no worker runs any more.
            let seq = unsafe { slot.load(Ordering::Acquire, &guard).deref() }.seq;
            assert!(
                seq as usize % RING == index && seq + RING as u64 > rounds as u64
                    || seq == 0 && index > rounds,
                "slot {index} does not hold its latest value"
            );
        }
    }
}

impl<const S: usize> Drop for Broadcast<S> {
    fn drop(&mut self) {
        let guard = pin();
        for slot in self.ring.iter() {
            // SAFETY: nothing else reaches the ring; each node came from `alloc_node`.
            unsafe { drop_node(slot.load(Ordering::Acquire, &guard).as_raw()) };
        }
    }
}

/// The waiter of an operation in flight: the state its completion wakes, `S` bytes.
pub struct Waiter<const S: usize>([u8; S]);

/// Each round a worker registers the waiter of an operation, finds it as the completion does,
/// and removes it: the map drops its waiter when the entry is reclaimed.
pub struct Registry<const S: usize> {
    map: kovan_map::HashMap<u64, Arc<Waiter<S>>>,
}

impl<const S: usize> Registry<S> {
    pub fn new(_threads: usize) -> Self {
        assert!(S >= 24);
        Self {
            map: kovan_map::HashMap::new(),
        }
    }
}

impl<const S: usize> Workload for Registry<S> {
    fn work<R: Recorder>(&self, worker: usize, _threads: usize, rounds: usize, rec: &mut R) {
        for i in 0..rounds {
            rec.round(|| {
                let id = (worker as u64) << 32 | i as u64;
                self.map
                    .insert(id, Arc::new(Waiter(message::<S>(worker, id))));
                let found = self.map.get(&id).expect("a registered waiter");
                assert_eq!(read_message(&found.0).1, id, "another operation's waiter");
                let gone = self.map.remove(&id).expect("a registered waiter");
                assert_eq!(read_message(&gone.0).1, id, "another operation's waiter");
            });
        }
    }

    fn check(&self, _threads: usize, _rounds: usize) {
        assert!(self.map.is_empty(), "a waiter left registered");
    }
}

/// Workers `1..` send messages of `S` bytes over a channel to worker 0, which blocks on it
/// whenever it is empty, at most `IN_FLIGHT` waiting.
pub struct Channel<const S: usize> {
    tx: kovan_channel::unbounded::Sender<[u8; S]>,
    rx: kovan_channel::unbounded::Receiver<[u8; S]>,
    waiting: AtomicUsize,
    taken: AtomicUsize,
}

impl<const S: usize> Channel<S> {
    pub fn new(threads: usize) -> Self {
        assert!((2..256).contains(&threads) && S >= 24);
        let (tx, rx) = kovan_channel::unbounded();
        Self {
            tx,
            rx,
            waiting: AtomicUsize::new(0),
            taken: AtomicUsize::new(0),
        }
    }
}

impl<const S: usize> Workload for Channel<S> {
    fn work<R: Recorder>(&self, worker: usize, threads: usize, rounds: usize, rec: &mut R) {
        if worker == 0 {
            let mut order = InOrder::new(threads);
            for _ in 0..(threads - 1) * rounds {
                let m = rec.round(|| {
                    match self
                        .rx
                        .recv_deadline(Instant::now() + Duration::from_secs(60))
                    {
                        RecvDeadline::Msg(m) => m,
                        RecvDeadline::Timeout => panic!("a message: nothing for a minute"),
                        RecvDeadline::Disconnected => panic!("the channel closed"),
                    }
                });
                self.waiting.fetch_sub(1, Ordering::AcqRel);
                order.take(read_message(&m));
                self.taken.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            for i in 0..rounds {
                until("room for a message", || {
                    (self.waiting.load(Ordering::Acquire) < IN_FLIGHT).then_some(())
                });
                self.waiting.fetch_add(1, Ordering::AcqRel);
                rec.round(|| self.tx.send(message::<S>(worker, i as u64)));
            }
        }
    }

    fn check(&self, threads: usize, rounds: usize) {
        assert_eq!(
            self.taken.load(Ordering::Relaxed),
            (threads - 1) * rounds,
            "messages lost"
        );
        assert!(self.rx.try_recv().is_none(), "a message nobody sent");
    }
}

/// The read arrivals a connection's queue takes before it closes: more than a segment holds.
const ARRIVALS: u32 = 40;

/// Each round a worker opens a connection's queue of read arrivals, takes `ARRIVALS` through it,
/// and closes it.
pub struct Connections {
    closed: AtomicUsize,
}

impl Connections {
    pub fn new(_threads: usize) -> Self {
        Self {
            closed: AtomicUsize::new(0),
        }
    }
}

impl Workload for Connections {
    fn work<R: Recorder>(&self, _worker: usize, _threads: usize, rounds: usize, rec: &mut R) {
        for i in 0..rounds as u64 {
            rec.round(|| {
                let arrivals = SegQueue::<(u64, u32)>::new();
                (0..ARRIVALS).for_each(|a| arrivals.push((i, a)));
                for a in 0..ARRIVALS {
                    assert_eq!(arrivals.pop(), Some((i, a)), "an arrival out of order");
                }
            });
            self.closed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn check(&self, threads: usize, rounds: usize) {
        assert_eq!(
            self.closed.load(Ordering::Relaxed),
            threads * rounds,
            "connections lost"
        );
    }
}

/// Every workload at every size, for `$each!(name, Type, size)`: the list the test and the
/// benches share.
#[macro_export]
macro_rules! every_workload {
    ($each:ident) => {
        $each!("inbox", Inbox, 24);
        $each!("connections", Connections, 16);
        $each!("messages", Messages<64>, 64);
        $each!("messages", Messages<4096>, 4096);
        $each!("messages", Messages<65536>, 65536);
        $each!("watch", Watch<64>, 64);
        $each!("watch", Watch<4096>, 4096);
        $each!("watch", Watch<65536>, 65536);
        $each!("broadcast", Broadcast<64>, 64);
        $each!("broadcast", Broadcast<4096>, 4096);
        $each!("broadcast", Broadcast<65536>, 65536);
        $each!("registry", Registry<64>, 64);
        $each!("registry", Registry<4096>, 4096);
        $each!("registry", Registry<65536>, 65536);
        $each!("channel", Channel<64>, 64);
        $each!("channel", Channel<4096>, 4096);
        $each!("channel", Channel<65536>, 65536);
    };
}

/// The rounds each worker runs for a value of `size` bytes, long enough for every retirement to
/// cycle many times.
pub fn rounds_for(size: usize) -> usize {
    if size >= 1 << 16 { 2_000 } else { 20_000 }
}

/// The worker counts every workload runs on.
pub const THREADS: [usize; 2] = [2, 8];
