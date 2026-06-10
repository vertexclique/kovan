//! Regression tests for the reclamation core.
//!
//! Covers: batch accumulation when try_retire cannot place a batch (high
//! thread counts), orphan adoption from exiting threads, protection across
//! flush() under a live guard, re-entrant destructor chains, epoch-churn
//! stress with payload validation, and liveness of pin() under a flush storm.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

static DROPS: AtomicUsize = AtomicUsize::new(0);

struct Counted(#[allow(dead_code)] u64);

impl Drop for Counted {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

/// Serialize ALL tests in this file: kovan's reclamation state (epoch,
/// slots, orphan list) is process-global, so concurrent tests would
/// perturb each other's exact drop counts. Poison-resilient: a failed
/// test must not cascade into the others.
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// One full batch (64) retired while `n_pinned` threads hold guards, then a
/// complete unwind. Every value must eventually be freed regardless of how
/// many slots were eligible during try_retire.
fn run_pinned_scenario(n_pinned: usize) -> (usize, usize) {
    DROPS.store(0, Ordering::SeqCst);

    let atom = Arc::new(kovan::Atom::new(Counted(0)));
    let start = Arc::new(Barrier::new(n_pinned + 1));
    let release = Arc::new(Barrier::new(n_pinned + 1));

    let handles: Vec<_> = (0..n_pinned)
        .map(|_| {
            let atom = Arc::clone(&atom);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            thread::spawn(move || {
                let g = atom.load();
                start.wait();
                release.wait();
                drop(g);
                kovan::flush();
            })
        })
        .collect();

    start.wait();
    // Exactly one full batch -> one try_retire while everyone is pinned.
    for i in 1..=64u64 {
        atom.store(Counted(i));
    }
    release.wait();
    for h in handles {
        h.join().unwrap();
    }

    kovan::flush();
    drop(atom);
    kovan::flush();

    (DROPS.load(Ordering::SeqCst), 65)
}

/// Regression for the whole-batch leak: with >= 64 eligible slots the scan
/// phase cannot place the batch. The batch must be kept (accumulated) and
/// submitted later — never dropped.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn no_batch_leak_with_many_pinned_threads() {
    let _l = test_lock();
    let (drops, expected) = run_pinned_scenario(80);
    assert_eq!(
        drops, expected,
        "batch leaked: try_retire failure must keep the batch, not drop it"
    );
}

/// Control: the same scenario well under the slot threshold.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn no_batch_leak_with_few_pinned_threads() {
    let _l = test_lock();
    let (drops, expected) = run_pinned_scenario(8);
    assert_eq!(drops, expected);
}

/// A thread that exits while its partial batch cannot be placed (many
/// eligible slots) must park the batch on the orphan list; a later retiring
/// thread adopts and frees it. Nothing leaks across thread exit.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn orphaned_partial_batch_is_adopted() {
    let _l = test_lock();
    DROPS.store(0, Ordering::SeqCst);

    const PINNED: usize = 80;
    const ORPHAN_STORES: u64 = 10;

    let atom = Arc::new(kovan::Atom::new(Counted(0)));
    let start = Arc::new(Barrier::new(PINNED + 1));
    let release = Arc::new(Barrier::new(PINNED + 1));

    let pinned: Vec<_> = (0..PINNED)
        .map(|_| {
            let atom = Arc::clone(&atom);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            thread::spawn(move || {
                let g = atom.load();
                start.wait();
                release.wait();
                drop(g);
                kovan::flush();
            })
        })
        .collect();

    start.wait();

    // Short-lived thread: a few retires, then exit. Its cleanup try_retire
    // fails (9 assignable nodes < ~81 eligible slots) -> orphan.
    {
        let atom = Arc::clone(&atom);
        thread::spawn(move || {
            for i in 1..=ORPHAN_STORES {
                atom.store(Counted(1000 + i));
            }
        })
        .join()
        .unwrap();
    }

    release.wait();
    for h in pinned {
        h.join().unwrap();
    }

    // Adopter: retire enough on this thread to trigger the RETIRE_FREQ
    // block, which adopts the orphan and submits the merged batch.
    for i in 1..=64u64 {
        atom.store(Counted(2000 + i));
    }
    kovan::flush();
    drop(atom);
    kovan::flush();

    // initial + ORPHAN_STORES + 64 stores = 1 + 10 + 64 = 75 values total.
    let expected = 1 + ORPHAN_STORES as usize + 64;
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        expected,
        "orphaned batch from exited thread must be adopted and freed"
    );
}

/// flush() while holding a guard must not release protection of pointers
/// loaded in the current critical section (the slot list drain is deferred
/// to the next pin boundary).
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn flush_under_live_guard_keeps_protection() {
    let _l = test_lock();
    static TARGET_DROPPED: AtomicBool = AtomicBool::new(false);

    struct Target {
        canary: u64,
    }
    impl Drop for Target {
        fn drop(&mut self) {
            TARGET_DROPPED.store(true, Ordering::SeqCst);
            self.canary = 0;
        }
    }
    struct Filler;

    let atom = Arc::new(kovan::Atom::new(Target { canary: 0xC0FFEE }));
    let filler = Arc::new(kovan::Atom::new(Filler));

    // Reader pins and holds a reference to the initial Target.
    let guard = atom.load();
    assert_eq!(guard.canary, 0xC0FFEE);

    // Writer replaces the Target (retiring it) and then drives a full batch
    // through try_retire so the Target's batch gets distributed to slots.
    {
        let atom = Arc::clone(&atom);
        let filler = Arc::clone(&filler);
        thread::spawn(move || {
            atom.store(Target { canary: 0xDEAD });
            for _ in 0..63 {
                filler.store(Filler);
            }
            kovan::flush();
        })
        .join()
        .unwrap();
    }

    // flush() on the reader thread with the guard still live: must not drain
    // the slot list that protects `guard`.
    kovan::flush();
    assert!(
        !TARGET_DROPPED.load(Ordering::SeqCst),
        "flush under a live guard released protection of a loaded value"
    );
    assert_eq!(guard.canary, 0xC0FFEE);

    drop(guard);
    kovan::pin(); // boundary: slot transitions on next pin after epoch change
    kovan::flush();
}

/// Values that contain Atoms themselves: dropping them runs destructors
/// inside the reclamation path (free_batch_list -> drop -> Atom::drop ->
/// flush), exercising the re-entrancy protections. Every value must drop
/// exactly once (a double-retire would crash or over-count).
#[test]
fn reentrant_destructor_chains_drop_exactly_once() {
    let _l = test_lock();
    DROPS.store(0, Ordering::SeqCst);

    struct Nested {
        _inner: kovan::Atom<Counted>,
    }

    let atom = kovan::Atom::new(Nested {
        _inner: kovan::Atom::new(Counted(0)),
    });

    // Each store retires a Nested whose destructor drops an Atom<Counted>,
    // which calls flush() inside the reclamation path. 200 stores crosses
    // several RETIRE_FREQ boundaries.
    for i in 1..=200u64 {
        atom.store(Nested {
            _inner: kovan::Atom::new(Counted(i)),
        });
    }

    kovan::flush();
    drop(atom);
    kovan::flush();

    // 201 Counted values (initial + 200), each exactly once.
    assert_eq!(DROPS.load(Ordering::SeqCst), 201);
}

/// Epoch-churn stress: writers retire continuously (advancing the epoch)
/// while readers validate payload integrity through guards. Exercises the
/// bounded convergence loop and the unconditional-reservation escalation
/// under maximum epoch movement.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn loads_remain_valid_under_epoch_churn() {
    let _l = test_lock();
    const READERS: usize = 4;
    const WRITERS: usize = 4;
    const DURATION: Duration = Duration::from_secs(3);

    struct Payload {
        a: u64,
        b: u64, // invariant: b == !a
    }

    let atom = Arc::new(kovan::Atom::new(Payload { a: 0, b: !0 }));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    for _ in 0..WRITERS {
        let atom = Arc::clone(&atom);
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                atom.store(Payload { a: i, b: !i });
                // flush() advances the epoch every call — maximum churn for
                // the readers' convergence loops.
                if i.is_multiple_of(16) {
                    kovan::flush();
                }
                i += 1;
            }
            kovan::flush();
        }));
    }

    for _ in 0..READERS {
        let atom = Arc::clone(&atom);
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let g = atom.load();
                assert_eq!(g.b, !g.a, "torn or freed payload observed");
                drop(g);
            }
            kovan::flush();
        }));
    }

    thread::sleep(DURATION);
    stop.store(true, Ordering::SeqCst);
    for h in handles {
        h.join().unwrap();
    }
    kovan::flush();
}

/// Liveness: pin() must keep completing while another thread storms
/// flush()/epoch advances. Regression for the unhelped-advance starvation
/// (flush used to advance max_threads+2 times without helping).
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn pin_completes_under_flush_storm() {
    let _l = test_lock();
    let stop = Arc::new(AtomicBool::new(false));

    let stormer = {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let a = kovan::Atom::new(7u64);
                drop(a); // Atom::drop -> flush -> epoch advance
            }
        })
    };

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut pins = 0u64;
    while pins < 200_000 {
        let g = kovan::pin();
        drop(g);
        pins += 1;
        assert!(
            Instant::now() < deadline,
            "pin() starved under flush storm: {pins} pins completed"
        );
    }

    stop.store(true, Ordering::SeqCst);
    stormer.join().unwrap();
    kovan::flush();
}

/// Thread churn: many short-lived threads retire and exit concurrently with
/// pinned readers. Exercises free_tid's exchange-based deactivation and tid
/// recycling; everything must be freed after the unwind.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn thread_churn_frees_everything() {
    let _l = test_lock();
    DROPS.store(0, Ordering::SeqCst);

    const CHURN_THREADS: usize = 32;
    const STORES_PER_THREAD: u64 = 100;

    let atom = Arc::new(kovan::Atom::new(Counted(0)));

    // A few stable readers create load on the slots.
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let atom = Arc::clone(&atom);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let g = atom.load();
                    drop(g);
                }
                kovan::flush();
            })
        })
        .collect();

    let churn: Vec<_> = (0..CHURN_THREADS)
        .map(|t| {
            let atom = Arc::clone(&atom);
            thread::spawn(move || {
                for i in 0..STORES_PER_THREAD {
                    atom.store(Counted(t as u64 * 10_000 + i));
                }
                // exit without flush: cleanup() must hand off correctly
            })
        })
        .collect();

    for h in churn {
        h.join().unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    for h in readers {
        h.join().unwrap();
    }

    // Drain: adopt any orphans and force full reclamation on this thread.
    for i in 0..128u64 {
        atom.store(Counted(900_000 + i));
    }
    kovan::flush();
    drop(atom);
    kovan::flush();

    let expected = 1 + CHURN_THREADS * STORES_PER_THREAD as usize + 128;
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        expected,
        "thread churn leaked or double-freed values"
    );
}
