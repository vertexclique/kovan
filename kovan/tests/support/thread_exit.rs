//! Runs code in a thread-local destructor that runs after kovan's own: the
//! thread's exit is over and kovan's thread-local handle is gone, the case
//! `kovan::Guard`'s docs call "a pin after the thread's exit". Every kovan
//! crate's `tests/thread_exit.rs` includes this file, so each public
//! operation is checked from there by one harness, with one tracked value.
//!
//! Thread-local destructors run in the reverse order of their registration.
//! The destructor's thread-local is set before the thread first uses kovan,
//! so it is registered before kovan's handle and destroyed after it. A probe
//! set after kovan's first use checks that order on the platform at hand:
//! the late destructor reports a failure, and runs nothing, if the probe is
//! not destroyed yet, since the case would then not be reached.
//!
//! [`Val`] records whether it is alive by a unique id, in a set outside its
//! own memory, so a test reads liveness without following a pointer that
//! may dangle; a clone of a value already dropped (a read of freed memory)
//! is counted.

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// How long a test waits for another thread before it fails.
pub const DEADLINE: Duration = Duration::from_secs(60);

type Body = Box<dyn FnOnce() -> bool + Send>;

struct Late(Option<Body>);

impl Drop for Late {
    fn drop(&mut self) {
        if let Some(body) = self.0.take() {
            body();
        }
    }
}

struct Probe;

impl Drop for Probe {
    fn drop(&mut self) {
        PROBED.with(|p| p.set(true));
    }
}

thread_local! {
    static LATE: RefCell<Option<Late>> = const { RefCell::new(None) };
    static PROBE: Probe = const { Probe };
    // Const and without a destructor: it answers during every destructor.
    static PROBED: Cell<bool> = const { Cell::new(false) };
}

/// Runs `body` on a fresh thread in a thread-local destructor that runs
/// after kovan's own, and returns what it returns. Panics if the platform
/// ran the destructors in another order (the case would not be reached).
/// A panic in `body` aborts the process: a panic in a thread-local
/// destructor is fatal, which a test run reports as a failure.
pub fn after_exit<R: Send + 'static>(body: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel::<Result<R, ()>>();
    let t = thread::spawn(move || {
        LATE.with(|late| {
            *late.borrow_mut() = Some(Late(Some(Box::new(move || {
                let result = if PROBED.with(Cell::get) {
                    Ok(body())
                } else {
                    Err(())
                };
                tx.send(result).is_ok()
            }))))
        });
        // kovan's handle (its destructor is the thread's exit) is registered
        // here, after `LATE` and before `PROBE`.
        drop(kovan::pin());
        PROBE.with(|_| {});
    });
    t.join()
        .expect("the thread whose destructor runs after its exit panicked");
    match rx.recv_timeout(DEADLINE) {
        Ok(Ok(value)) => value,
        Ok(Err(())) => panic!(
            "thread-local destructors ran out of registration order here: the case after the \
             thread's exit was not reached"
        ),
        Err(_) => panic!("the destructor that runs after the thread's exit reported nothing"),
    }
}

/// Runs `step` until it returns true or `DEADLINE` passes; whether it did.
pub fn eventually(mut step: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + DEADLINE;
    while !step() {
        if Instant::now() > end {
            return false;
        }
        thread::yield_now();
    }
    true
}

/// The tests of one binary count live values process-wide: one at a time.
static LOCK: Mutex<()> = Mutex::new(());

/// Serializes the tests of one binary.
pub fn lock() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

static ALIVE: Mutex<BTreeSet<u64>> = Mutex::new(BTreeSet::new());
static NEXT: AtomicU64 = AtomicU64::new(0);
static DEAD_READS: AtomicUsize = AtomicUsize::new(0);

/// Whether the value of `id` is alive.
pub fn alive(id: u64) -> bool {
    ALIVE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&id)
}

/// How many values are alive.
pub fn live() -> usize {
    ALIVE.lock().unwrap_or_else(|e| e.into_inner()).len()
}

/// Clones made of a value already dropped since the last `take_dead_reads`.
pub fn take_dead_reads() -> usize {
    DEAD_READS.swap(0, Ordering::SeqCst)
}

/// A value that records whether it is alive, by a unique id, outside its own
/// memory.
pub struct Val {
    pub id: u64,
}

impl Val {
    pub fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        ALIVE.lock().unwrap_or_else(|e| e.into_inner()).insert(id);
        Val { id }
    }
}

impl Default for Val {
    fn default() -> Self {
        Val::new()
    }
}

/// A clone stopped, once, inside the structure that runs it.
struct Gate {
    id: u64,
    arrived: mpsc::Sender<()>,
    go: mpsc::Receiver<()>,
}

static GATE: Mutex<Option<Gate>> = Mutex::new(None);

/// A fresh value; a clone of a value already dropped is counted. The clone
/// of the value a gate is armed for (`held_across_retire`) reports its
/// arrival and waits for the go before it looks.
impl Clone for Val {
    fn clone(&self) -> Self {
        let gate = {
            let mut armed = GATE.lock().unwrap_or_else(|e| e.into_inner());
            if armed.as_ref().is_some_and(|g| g.id == self.id) {
                armed.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            let _ = gate.arrived.send(());
            let _ = gate.go.recv_timeout(DEADLINE);
        }
        // `self.id` is read from the value the structure holds, after the
        // other thread retired it: the liveness set says whether it was
        // freed, and AddressSanitizer reports the read if it was.
        if !alive(self.id) {
            DEAD_READS.fetch_add(1, Ordering::SeqCst);
        }
        Val::new()
    }
}

/// Runs `read` after a thread's exit, stopping it inside its clone of the
/// value of `id` (the structure under test holds that value then, under its
/// guard); meanwhile `retire`, on this thread, replaces or removes the value,
/// retiring it, and this thread flushes again and again. Returns whether the
/// clone found the value still alive when it went on: a structure whose
/// guard protects nothing after the exit fails this every time.
pub fn held_across_retire(
    id: u64,
    read: impl FnOnce() + Send + 'static,
    retire: impl FnOnce(),
) -> bool {
    take_dead_reads();
    let (arrived_tx, arrived_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    *GATE.lock().unwrap_or_else(|e| e.into_inner()) = Some(Gate {
        id,
        arrived: arrived_tx,
        go: go_rx,
    });
    let late = thread::spawn(move || after_exit(read));
    arrived_rx
        .recv_timeout(DEADLINE)
        .expect("the read after the thread's exit never cloned the value");
    retire();
    for _ in 0..64 {
        kovan::flush();
    }
    let _ = go_tx.send(());
    late.join()
        .expect("the thread that read after its exit panicked");
    take_dead_reads() == 0
}

impl Drop for Val {
    fn drop(&mut self) {
        ALIVE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

/// Flushes until `expected` values are alive; whether it got there.
pub fn freed_down_to(expected: usize) -> bool {
    eventually(|| {
        kovan::flush();
        live() == expected
    })
}
