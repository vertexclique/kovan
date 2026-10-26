//! Recycling retired nodes: `Reclaimable::dealloc` plus `retire_reclaimable`.
//!
//! A pool-backed node type overrides `dealloc` to drop its value in place and
//! push the node on a free list, and a writer takes nodes back from that list
//! (re-arming the embedded `RetiredNode`) instead of allocating. Three
//! properties are checked:
//!
//! - retired nodes come back to the pool and are reused: after warm-up the
//!   allocation count stays flat;
//! - under concurrent readers and writers no reader ever sees a node that was
//!   recycled while its guard was live. `dealloc` poisons the value before it
//!   pools the node, and every publish stamps a sequence number plus words
//!   derived from it, so a reader that catches a poisoned, torn or rewritten
//!   value fails;
//! - every node is accounted for at the end: each one allocated is back in the
//!   pool exactly once, and each incarnation's value was dropped exactly once.
//!
//! The tests share kovan's process-global reclamation state, so they run one
//! at a time: a reader pinned by one would otherwise hold back the batches
//! another waits on, and the allocation counts would depend on test order.

use kovan::{Atomic, Reclaimable, RetiredNode, Shared, flush, pin, retire_reclaimable};
use std::collections::HashSet;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

const POISON: u64 = u64::MAX;
const KEYS: [u64; 3] = [
    0x9E37_79B9_7F4A_7C15,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
];

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Gives back what a test leaked for `'static`, once every thread that used
/// it is joined and every node that points at it is freed: LeakSanitizer
/// checks the suite.
fn unleak<T>(leaked: &'static T) {
    // SAFETY: made by `Box::leak` in this test, and nothing uses it any more.
    drop(unsafe { Box::from_raw(leaked as *const T as *mut T) });
}

/// Counts its own drops, so a missed or doubled in-place drop shows up.
struct Tracked(&'static Pool);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct Ptr(*mut Slot);

// SAFETY: a pooled node is reachable only through the pool.
unsafe impl Send for Ptr {}

/// The per-structure free list, plus the counters the assertions read.
struct Pool {
    free: Mutex<Vec<Ptr>>,
    allocs: AtomicUsize,
    reuses: AtomicUsize,
    deallocs: AtomicUsize,
    drops: AtomicUsize,
}

impl Pool {
    fn leak() -> &'static Pool {
        Box::leak(Box::new(Pool {
            free: Mutex::new(Vec::new()),
            allocs: AtomicUsize::new(0),
            reuses: AtomicUsize::new(0),
            deallocs: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        }))
    }

    fn put(&self, node: *mut Slot) {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Ptr(node));
    }

    fn take(&self) -> Option<*mut Slot> {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .map(|p| p.0)
    }

    fn pooled(&self) -> usize {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    fn allocs(&self) -> usize {
        self.allocs.load(Ordering::Relaxed)
    }

    /// A node holding `seq`: recycled if the pool has one, allocated if not.
    fn acquire(&'static self, seq: u64) -> *mut Slot {
        let node = match self.take() {
            Some(node) => {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                // SAFETY: the node left the pool, so this thread owns it. The
                // header is re-armed (fresh birth epoch) before the node can
                // be published, as `Reclaimable` documents.
                unsafe {
                    ptr::write(&raw mut (*node).retired, RetiredNode::new());
                    (*node).tracked.write(Tracked(self));
                }
                node
            }
            None => {
                self.allocs.fetch_add(1, Ordering::Relaxed);
                Box::into_raw(Box::new(Slot {
                    retired: RetiredNode::new(),
                    seq: AtomicU64::new(0),
                    echo: [const { AtomicU64::new(0) }; 3],
                    tracked: MaybeUninit::new(Tracked(self)),
                    pool: self,
                }))
            }
        };
        // SAFETY: unpublished, so no other thread reads it yet.
        unsafe { (*node).fill(seq) };
        node
    }

    /// Frees every pooled node (their values are already dropped).
    fn free_all(&self) -> usize {
        let nodes = std::mem::take(&mut *self.free.lock().unwrap_or_else(PoisonError::into_inner));
        let n = nodes.len();
        for Ptr(node) in nodes {
            // SAFETY: allocated by `acquire` with `Box`, owned by the pool.
            // `tracked` is a `MaybeUninit`, so the drop below skips it.
            drop(unsafe { Box::from_raw(node) });
        }
        n
    }
}

#[repr(C)]
struct Slot {
    retired: RetiredNode,
    seq: AtomicU64,
    echo: [AtomicU64; 3],
    tracked: MaybeUninit<Tracked>,
    pool: &'static Pool,
}

impl Slot {
    fn fill(&self, seq: u64) {
        self.seq.store(seq, Ordering::Relaxed);
        for (e, k) in self.echo.iter().zip(KEYS) {
            e.store(seq ^ k, Ordering::Relaxed);
        }
    }

    /// The sequence number, after checking the value is whole and not poisoned.
    fn check(&self) -> u64 {
        let seq = self.seq.load(Ordering::Relaxed);
        assert_ne!(seq, POISON, "reader saw a node already passed to dealloc");
        for (e, k) in self.echo.iter().zip(KEYS) {
            assert_eq!(
                e.load(Ordering::Relaxed),
                seq ^ k,
                "reader saw a torn or recycled value for seq {seq}"
            );
        }
        seq
    }
}

// SAFETY: `RetiredNode` is the first field of a `#[repr(C)]` struct.
unsafe impl Reclaimable for Slot {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }

    unsafe fn dealloc(node: *mut Self) {
        // SAFETY: kovan never touches the node again and no guard reaches it,
        // so it is ours to poison, drop and hand to the pool.
        unsafe {
            let pool = (*node).pool;
            (*node).seq.store(POISON, Ordering::Relaxed);
            for e in &(*node).echo {
                e.store(POISON, Ordering::Relaxed);
            }
            ptr::drop_in_place((*node).tracked.as_mut_ptr());
            pool.deallocs.fetch_add(1, Ordering::Relaxed);
            pool.put(node);
        }
    }
}

/// Publish a fresh node holding `seq`, retire what it replaced.
fn publish(pool: &'static Pool, slot: &Atomic<Slot>, seq: u64) {
    let node = pool.acquire(seq);
    let guard = pin();
    // SAFETY: `node` is unpublished and valid; `old` is unlinked by the swap.
    let old = slot.swap(unsafe { Shared::from_raw(node) }, Ordering::AcqRel, &guard);
    if !old.is_null() {
        // SAFETY: retired once, never touched again by this thread.
        unsafe { retire_reclaimable(old.as_raw()) };
    }
}

/// Retire the node `slot` still holds and flush until every node is back in
/// the pool, then check the books balance: each node allocated is pooled once,
/// each incarnation's value was dropped once.
fn drain_and_audit(pool: &'static Pool, slot: &Atomic<Slot>) {
    {
        let guard = pin();
        let last = slot.swap(
            unsafe { Shared::from_raw(ptr::null_mut()) },
            Ordering::AcqRel,
            &guard,
        );
        if !last.is_null() {
            unsafe { retire_reclaimable(last.as_raw()) };
        }
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while pool.pooled() != pool.allocs() && Instant::now() < deadline {
        flush();
        thread::yield_now();
    }

    let allocs = pool.allocs();
    let reuses = pool.reuses.load(Ordering::Relaxed);
    assert_eq!(pool.pooled(), allocs, "a retired node never came back");
    assert_eq!(
        pool.deallocs.load(Ordering::Relaxed),
        allocs + reuses,
        "dealloc must run once per incarnation"
    );
    assert_eq!(
        pool.drops.load(Ordering::Relaxed),
        allocs + reuses,
        "every value must be dropped in place exactly once"
    );
    let distinct: HashSet<usize> = pool
        .free
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .map(|p| p.0 as usize)
        .collect();
    assert_eq!(distinct.len(), allocs, "a node was pooled twice");
    assert_eq!(pool.free_all(), allocs);
}

#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn retired_nodes_return_to_the_pool_and_are_reused() {
    const WARMUP: u64 = 4_096;
    const STEADY: u64 = 100_000;

    let _serial = serial();
    let pool = Pool::leak();
    let slot = Atomic::<Slot>::null();

    for seq in 0..WARMUP {
        publish(pool, &slot, seq);
        if seq % 64 == 63 {
            flush();
        }
    }
    flush();
    let warm = pool.allocs();
    assert!(warm > 0 && pool.pooled() > 0, "warm-up returned no node");

    for seq in WARMUP..WARMUP + STEADY {
        publish(pool, &slot, seq);
        if seq % 64 == 63 {
            flush();
        }
    }
    let grown = pool.allocs() - warm;
    assert_eq!(
        grown, 0,
        "allocations grew by {grown} over {STEADY} publishes after warm-up ({warm} at warm-up)"
    );
    let reuses = pool.reuses.load(Ordering::Relaxed);
    assert!(
        reuses as u64 >= STEADY,
        "only {reuses} of {STEADY} steady-state publishes reused a node"
    );

    drain_and_audit(pool, &slot);
    unleak(pool);
}

/// A type that keeps the default `dealloc` frees its `Box`, as `retire` does.
#[repr(C)]
struct Plain {
    retired: RetiredNode,
    drops: &'static AtomicUsize,
}

impl Drop for Plain {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

// SAFETY: `RetiredNode` is the first field of a `#[repr(C)]` struct.
unsafe impl Reclaimable for Plain {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }
}

#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn default_dealloc_frees_the_box() {
    const NODES: usize = 1_000;

    let _serial = serial();
    let drops: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
    for _ in 0..NODES {
        let node = Box::into_raw(Box::new(Plain {
            retired: RetiredNode::new(),
            drops,
        }));
        let _guard = pin();
        // SAFETY: never published, retired once.
        unsafe { retire_reclaimable(node) };
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while drops.load(Ordering::Relaxed) != NODES && Instant::now() < deadline {
        flush();
        thread::yield_now();
    }
    assert_eq!(drops.load(Ordering::Relaxed), NODES);
    unleak(drops);
}

#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn concurrent_recycling_never_exposes_a_recycled_value() {
    const READERS: usize = 4;
    const WRITERS: usize = 2;
    const PUBLISHES: u64 = 40_000;
    const MIN_CHECKS: usize = 100;

    let _serial = serial();
    let pool = Pool::leak();
    let slot: &'static Atomic<Slot> = Box::leak(Box::new(Atomic::null()));
    let next_seq: &'static AtomicU64 = Box::leak(Box::new(AtomicU64::new(1)));
    let stop: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
    let observed: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));

    publish(pool, slot, 0);

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            thread::spawn(move || {
                // Keep reading until told to stop and until it has checked
                // enough nodes, however the scheduler treated it meanwhile.
                let mut checks = 0;
                while !stop.load(Ordering::Relaxed) || checks < MIN_CHECKS {
                    let guard = pin();
                    let shared = slot.load(Ordering::Acquire, &guard);
                    // SAFETY: loaded under `guard`, which stays live below.
                    if let Some(node) = unsafe { shared.as_ref() } {
                        let seq = node.check();
                        // Hold the guard while writers retire and reuse nodes.
                        for _ in 0..32 {
                            std::hint::spin_loop();
                        }
                        thread::yield_now();
                        assert_eq!(node.check(), seq, "value changed under a live guard");
                        checks += 1;
                        observed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    let writers: Vec<_> = (0..WRITERS)
        .map(|_| {
            thread::spawn(move || {
                for _ in 0..PUBLISHES {
                    publish(pool, slot, next_seq.fetch_add(1, Ordering::Relaxed));
                }
                flush();
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    let published = WRITERS as u64 * PUBLISHES;
    let allocs = pool.allocs();
    let reuses = pool.reuses.load(Ordering::Relaxed);
    println!(
        "recycle stress: {published} publishes, {allocs} allocations, {reuses} reuses, \
         {} reader checks",
        observed.load(Ordering::Relaxed)
    );
    assert!(observed.load(Ordering::Relaxed) >= READERS * MIN_CHECKS);
    // Without recycling every publish allocates. With it, only the nodes in
    // flight (batches waiting on a reader's next transition) are extra, and
    // how many that is depends on scheduling: about 3% of the publishes on
    // an idle machine, up to about 10% with the cores oversubscribed. A
    // quarter leaves room for that and still fails a pool that is not used.
    assert!(
        (allocs as u64) < published / 4,
        "{allocs} allocations for {published} publishes: nodes are not being recycled"
    );
    assert!(reuses as u64 + allocs as u64 > published);

    drain_and_audit(pool, slot);
    unleak(slot);
    unleak(next_seq);
    unleak(stop);
    unleak(observed);
    unleak(pool);
}
