//! A value an `AtomOption` replaced must outlive every reader that loaded
//! it: each read a reader makes of it happens before its free.
//!
//! A retire places its batch in the slots its scan reads as active with an
//! era that covers the batch and skips every other slot; a batch placed in
//! no slot is freed at once. A reader whose slot the scan skipped is safe
//! only if its reads happen before the scan, or the scan happens before its
//! load. kovan 0.2.4 lacked three of those edges, and each one alone lets
//! a value be freed with no happens-before from a reader's reads of it:
//!
//! * Edge 1, the leave's tag. A guard drop stores the tag `ERA_INACTIVE`
//!   into its slot's era word, then exchanges its list word to INVPTR. A
//!   scan that reads the list word still active (0) and the era word tagged
//!   skips the slot. With a relaxed tag nothing orders the reader's reads
//!   before the free that follows; with a release tag the scan's acquire
//!   load of it does.
//! * Edge 2, the pin's barrier. A pin exchanges its list word to 0 with a
//!   SeqCst exchange and then loads the pointer with an acquire load; on
//!   x86-64 kovan counted on the locked exchange as a full barrier. In
//!   Rust's memory model a SeqCst exchange is not a fence: when the scan's
//!   fence comes first, the scan may read the slot as it was before the pin
//!   and skip it, while the acquire load still reads the pointer the retire
//!   replaced. A SeqCst fence after the exchange, or a SeqCst protected
//!   load, orders the load after the scan.
//! * Edge 3, the thread count. A scan visits the slots of the thread IDs
//!   below `next_tid`, which it read before its fence. A thread that just
//!   claimed a fresh ID and pinned can be past that bound: the scan never
//!   reads its slot. Read after the scan's fence, and with the claim a
//!   SeqCst write when the protected load is the SeqCst one, the bound
//!   covers every thread whose pin the scan must see.
//!
//! These are miri tests first: the races are in what the memory model
//! allows, which miri explores with its weak memory emulation. Natively
//! they run as stress tests at larger sizes.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use kovan::AtomOption;

static LOCK: Mutex<()> = Mutex::new(());

/// Serializes the tests: kovan's slots are process-wide, and each test
/// counts on which threads hold one when its writer retires.
fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// How long one thread waits for the other's step before the test fails.
const DEADLINE: Duration = Duration::from_secs(300);

/// Sets its flag when its thread unwinds, so the other side stops waiting.
struct OnUnwind<'a>(&'a AtomicBool);

impl Drop for OnUnwind<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.store(true, Ordering::Relaxed);
        }
    }
}

/// Yields until `ready()` holds. Panics once the other side unwound
/// (`failed`) or `DEADLINE` passed.
fn wait_until(failed: &AtomicBool, ready: impl Fn() -> bool) {
    let end = Instant::now() + DEADLINE;
    while !ready() {
        assert!(!failed.load(Ordering::Relaxed), "the other thread failed");
        assert!(Instant::now() < end, "the other thread made no progress");
        thread::yield_now();
    }
}

// ---------------------------------------------------------------------------
// An append-only log and a sorted index over it, replaced every few appends
// ---------------------------------------------------------------------------

/// Appends between two rebuilds of the index.
const REBUILD: usize = 16;

/// How far each entry reaches past its start: past every key a reader asks
/// for, so a lookup walks the whole index.
const REACH: u64 = 1 << 32;

/// An append-only log. Entry `p` holds its sequence `p + 1` once its append
/// is done and 0 before; it covers the keys from its sequence up to `REACH`
/// past it. The entries are read with no guard; the index over a prefix of
/// them lives in an `AtomOption`, replaced every `REBUILD` appends.
struct Log {
    entries: Box<[AtomicU64]>,
    claimed: AtomicUsize,
    index: AtomOption<Index>,
}

/// A sorted view of the first `covered` entries. Its arrays are heap
/// allocations of their own, so a walk of a freed index reads freed memory.
struct Index {
    covered: usize,
    /// The positions of those entries, by start.
    order: Box<[u32]>,
    /// `reach[j]`: the position, among `order[..=j]`, of the entry that
    /// reaches farthest.
    reach: Box<[u32]>,
}

impl Log {
    fn new(capacity: usize) -> Self {
        Self {
            entries: (0..capacity).map(|_| AtomicU64::new(0)).collect(),
            claimed: AtomicUsize::new(0),
            index: AtomOption::none(),
        }
    }

    /// Positions handed out so far.
    fn len(&self) -> usize {
        self.claimed.load(Ordering::Acquire)
    }

    /// The entry at `position`, or `None` while its append is not done.
    fn get(&self, position: usize) -> Option<u64> {
        match self.entries[position].load(Ordering::Acquire) {
            0 => None,
            seq => Some(seq),
        }
    }

    /// An entry the index covers, done by construction.
    fn indexed(&self, position: u32) -> u64 {
        self.get(position as usize)
            .expect("the index covers done entries only")
    }

    /// Whether the log holds `seq`, looking back from its end.
    fn holds(&self, seq: u64) -> bool {
        for position in (0..self.len()).rev() {
            let Some(held) = self.get(position) else {
                continue;
            };
            if held < seq {
                return false;
            }
            if held == seq {
                return true;
            }
        }
        false
    }

    fn push(&self, seq: u64) {
        if self.holds(seq) {
            return;
        }
        let position = self.claimed.fetch_add(1, Ordering::AcqRel);
        self.entries[position].store(seq, Ordering::Release);
        self.maybe_rebuild();
    }

    /// Replaces the index once `REBUILD` done entries gathered past it:
    /// reads the current one under its guard and merges the new entries
    /// into its order, then stores the larger one and flushes, so the
    /// replaced one goes to reclamation at once.
    fn maybe_rebuild(&self) {
        let covered = self.index.load().map_or(0, |index| index.covered);
        if self.len() - covered < REBUILD {
            return;
        }
        let (covered, old) = match self.index.load() {
            Some(index) => (index.covered, index.order.to_vec()),
            None => (0, Vec::new()),
        };
        let len = self.len();
        let published = (covered..len)
            .find(|&position| self.get(position).is_none())
            .unwrap_or(len);
        if published - covered < REBUILD {
            return;
        }
        let mut fresh: Vec<u32> = (covered as u32..published as u32).collect();
        fresh.sort_by_key(|&position| self.indexed(position));
        let mut order = Vec::with_capacity(published);
        let (mut old, mut fresh) = (old.into_iter().peekable(), fresh.into_iter().peekable());
        loop {
            let take_old = match (old.peek(), fresh.peek()) {
                (Some(&a), Some(&b)) => self.indexed(a) <= self.indexed(b),
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            order.extend(if take_old { old.next() } else { fresh.next() });
        }
        let mut reach: Vec<u32> = Vec::with_capacity(order.len());
        for &position in &order {
            let farthest = match reach.last() {
                Some(&prev) if self.indexed(prev) >= self.indexed(position) => prev,
                _ => position,
            };
            reach.push(farthest);
        }
        self.index.store_some(Index {
            covered: published,
            order: order.into_boxed_slice(),
            reach: reach.into_boxed_slice(),
        });
        kovan::flush();
    }

    /// The largest sequence of an entry covering `key`: a binary search and
    /// a walk down the index under the guard `load` returned, then a scan
    /// of the entries past it.
    fn covering_seq(&self, key: u64) -> u64 {
        let covers = |seq: u64| seq <= key && key < seq + REACH;
        let mut best = 0;
        let covered = match self.index.load() {
            Some(index) => {
                let mut j = index
                    .order
                    .partition_point(|&position| self.indexed(position) <= key);
                while j > 0 {
                    j -= 1;
                    if self.indexed(index.reach[j]) + REACH <= key {
                        break;
                    }
                    let seq = self.indexed(index.order[j]);
                    if covers(seq) {
                        best = best.max(seq);
                    }
                }
                index.covered
            }
            None => 0,
        };
        for seq in (covered..self.len()).filter_map(|position| self.get(position)) {
            if covers(seq) {
                best = best.max(seq);
            }
        }
        best
    }
}

/// Every edge, in the shape of the workload where miri first caught the
/// race. One writer appends 100 entries and, every 16, replaces the index
/// with a larger one and flushes, so the replaced index goes to reclamation
/// at once. Four readers loop: each reads the done entries with no guard,
/// then walks the whole index under the guard `load` returned.
///
/// The interleaving: a reader walks an index the writer has replaced while
/// the writer's flush, or another reader's guard drop (`Guard::drop` ->
/// `leave` -> `leave_cold` -> `drain_free_list` -> `free_batch_list`),
/// frees it. The scan that placed the index's batch skipped the walking
/// reader's slot: the reader had loaded the index and was leaving as the
/// scan read its slot (edge 1), it pinned after the scan's fence and its
/// load still read the replaced pointer (edge 2), or the scan's bound on
/// the thread IDs, read before its fence, left the reader out (edge 3,
/// right after the readers' first pins).
///
/// Without the fixes, miri reports a data race between a read of the
/// index's `order` in `covering_seq` and its deallocation, or a dangling
/// `Box` read through the replaced pointer.
#[test]
fn readers_of_a_rebuilt_index_while_a_writer_appends() {
    // Under miri: 100 appends make six rebuilds under four readers.
    const APPENDS: usize = if cfg!(miri) { 100 } else { 4_000 };
    const READERS: usize = 4;
    let _serial = lock();
    let log = Log::new(APPENDS);
    let (done, failed) = (AtomicBool::new(false), AtomicBool::new(false));
    thread::scope(|scope| {
        for _ in 0..READERS {
            scope.spawn(|| {
                let _unwind = OnUnwind(&failed);
                let mut last = 0usize;
                while !done.load(Ordering::Acquire) || last < APPENDS {
                    assert!(!failed.load(Ordering::Relaxed), "another thread failed");
                    let len = log.len();
                    assert!(len >= last, "the prefix shrank from {last} to {len}");
                    // One writer: the done entries are a prefix of the
                    // positions handed out.
                    let mut ready = last;
                    while ready < len {
                        let Some(seq) = log.get(ready) else {
                            break;
                        };
                        assert_eq!(seq, ready as u64 + 1, "entry {ready} out of order");
                        ready += 1;
                    }
                    if ready > 0 {
                        let seen = log.covering_seq(ready as u64);
                        assert!(seen >= ready as u64, "a done entry was missed");
                    }
                    last = ready;
                }
            });
        }
        let _unwind = OnUnwind(&failed);
        for seq in 1..=APPENDS as u64 {
            log.push(seq);
        }
        done.store(true, Ordering::Release);
    });
}

// ---------------------------------------------------------------------------
// One edge at a time: a writer and a reader taking turns
// ---------------------------------------------------------------------------

/// Words in a `Value`.
const WORDS: usize = if cfg!(miri) { 8 } else { 32 };

/// What the atom holds in the turn-taking tests: its round, and a heap
/// array the reader reads whole, so a read of a freed value reads freed
/// memory.
struct Value {
    round: usize,
    words: Box<[u64]>,
}

impl Value {
    fn new(round: usize) -> Self {
        Self {
            round,
            words: (0..WORDS).map(|i| (round * WORDS + i) as u64).collect(),
        }
    }

    /// Reads every word.
    fn check(&self) {
        for (i, &word) in self.words.iter().enumerate() {
            assert_eq!(word, (self.round * WORDS + i) as u64, "a torn value");
        }
    }
}

/// Edge 1: a scan that reads a slot's leave orders that reader's reads
/// before the free.
///
/// One round at a time. The writer publishes value `r` and lets the reader
/// go; the reader loads it, tells the writer so, then reads it whole and
/// drops its guard. Meanwhile the writer replaces value `r` and flushes at
/// once. The interleaving: the reader's drop stores the tag into its era
/// word and exchanges its list word to INVPTR around the writer's scan, and
/// the scan reads the list word still active (0) and the era word tagged.
/// It skips the slot, no other slot is active, and the flush frees value
/// `r` itself.
///
/// The reader loads value `r` after the writer published it and before the
/// writer replaces it (the turn's release and acquire), so the pin's
/// barrier (edge 2) plays no part: there is no newer pointer to miss.
///
/// Without the release on the tag, nothing orders the reader's reads
/// before the free: miri reports a data race between the reader's read of
/// the words and their deallocation in the writer's flush (`flush` ->
/// `place_all` -> `drain_free_list` -> `free_batch_list`). With it, the
/// scan's acquire load of the tag synchronizes with the reader's leave.
#[test]
fn a_scan_that_reads_a_leave_sees_the_reads_before_it() {
    const ROUNDS: usize = if cfg!(miri) { 40 } else { 20_000 };
    let _serial = lock();
    let atom = AtomOption::some(Value::new(0));
    let (round, loaded, failed) = (
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicBool::new(false),
    );
    thread::scope(|scope| {
        scope.spawn(|| {
            let _unwind = OnUnwind(&failed);
            for r in 0..ROUNDS {
                wait_until(&failed, || round.load(Ordering::Acquire) == r);
                let value = atom.load().expect("the atom always holds a value");
                assert_eq!(value.round, r, "the reader loaded another round's value");
                loaded.store(r + 1, Ordering::Release);
                value.check();
            }
        });
        let _unwind = OnUnwind(&failed);
        for r in 0..ROUNDS {
            round.store(r, Ordering::Release);
            wait_until(&failed, || loaded.load(Ordering::Acquire) == r + 1);
            atom.store_some(Value::new(r + 1));
            kovan::flush();
        }
    });
}

/// Edge 2: a pin that follows a scan never loads the value that scan
/// freed.
///
/// One round at a time. The writer replaces value `r - 1` with value `r`
/// and flushes. The reader's slot is left, so the scan skips it, and the
/// flush frees value `r - 1` at once. The writer then raises a relaxed
/// flag; the reader, which waits for it, pins and loads. The interleaving:
/// the reader's pin comes after the scan's fence, and the relaxed flag
/// carries no happens-before, so only kovan's pin orders the reader's load
/// after the replacement.
///
/// The reader tells the writer it is done, with a release the writer
/// acquires before its next replacement, so every read of a round happens
/// before the next scan: the leave's tag (edge 1) plays no part.
///
/// Without a fence after the pin's exchange (or a SeqCst protected load),
/// a SeqCst exchange is not a fence in Rust's model and the acquire load
/// that follows may still read the replaced pointer: miri reports it
/// dangling in `AtomOption::load`, the value it names freed by the flush.
#[test]
fn a_pin_after_the_scan_never_loads_the_value_it_freed() {
    const ROUNDS: usize = if cfg!(miri) { 64 } else { 20_000 };
    let _serial = lock();
    let atom = AtomOption::some(Value::new(0));
    let (scanned, done, failed) = (
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicBool::new(false),
    );
    thread::scope(|scope| {
        scope.spawn(|| {
            let _unwind = OnUnwind(&failed);
            for r in 1..=ROUNDS {
                // Relaxed on purpose: the reader learns that the writer's
                // round is over without synchronizing with it.
                wait_until(&failed, || scanned.load(Ordering::Relaxed) == r);
                let value = atom.load().expect("the atom always holds a value");
                assert_eq!(value.round, r, "the reader loaded a replaced value");
                value.check();
                drop(value);
                done.store(r, Ordering::Release);
            }
        });
        let _unwind = OnUnwind(&failed);
        for r in 1..=ROUNDS {
            wait_until(&failed, || done.load(Ordering::Acquire) == r - 1);
            atom.store_some(Value::new(r));
            kovan::flush();
            scanned.store(r, Ordering::Relaxed);
        }
    });
}

/// Edge 3: a scan visits the slot of a reader whose thread pinned for the
/// first time.
///
/// One round at a time, each with a reader on a thread of its own that
/// stays alive to the end, so every reader's first pin claims a fresh
/// thread ID. The writer pins and spawns the reader; the reader loads value
/// `r - 1`, raises a relaxed flag, reads the value whole and drops its
/// guard; the writer, which waits for the flag, replaces value `r - 1`,
/// drops its own guard and flushes. The writer's guard is held across the
/// round so its pins inside it are nested and publish nothing, and the
/// relaxed flag carries no happens-before: nothing the writer does before
/// its scan synchronizes with the reader. The interleaving: the scan reads
/// `next_tid` from before the reader's claim and stops below the reader's
/// slot. No other slot is active, and the flush frees value `r - 1`.
///
/// The reader loads value `r - 1` before the writer replaces it, so edge 2
/// plays no part, and once the scan reads the reader's slot, edge 1's tag
/// is what decides.
///
/// Without the bound read after the scan's fence (and, with the SeqCst
/// protected load, a SeqCst claim of the thread ID), miri reports a data
/// race between the reader's read of the words and their deallocation in
/// the writer's flush (`flush` -> `place_all` -> `drain_free_list` ->
/// `free_batch_list`).
#[test]
fn a_scan_sees_a_reader_whose_thread_pinned_for_the_first_time() {
    const ROUNDS: usize = if cfg!(miri) { 24 } else { 512 };
    let _serial = lock();
    let atom = AtomOption::some(Value::new(0));
    let (loaded, failed) = (AtomicUsize::new(0), AtomicBool::new(false));
    // Every reader waits here until the writer is done, so no thread exits
    // and releases its ID: each reader's first pin claims a fresh one. The
    // writer's hold ends with its last round, or as it unwinds.
    let gate = RwLock::new(());
    thread::scope(|scope| {
        let _unwind = OnUnwind(&failed);
        let closed = gate.write().unwrap_or_else(|e| e.into_inner());
        for r in 1..=ROUNDS {
            // Held across the round: the writer's pins inside it are nested
            // and publish nothing, so nothing the writer does before its
            // flush's scan synchronizes with the reader.
            let section = kovan::pin();
            let (atom, loaded, failed, gate) = (&atom, &loaded, &failed, &gate);
            scope.spawn(move || {
                {
                    let _unwind = OnUnwind(failed);
                    let value = atom.load().expect("the atom always holds a value");
                    assert_eq!(value.round, r - 1, "the reader loaded another value");
                    // Relaxed on purpose: the writer learns that this
                    // reader holds the value without synchronizing with it.
                    loaded.store(r, Ordering::Relaxed);
                    value.check();
                }
                drop(gate.read());
            });
            wait_until(failed, || loaded.load(Ordering::Relaxed) == r);
            atom.store_some(Value::new(r));
            drop(section);
            kovan::flush();
        }
        drop(closed);
    });
}
