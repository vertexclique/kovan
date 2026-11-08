//! `quiesce()`: `flush` without its epoch advance. A thread that pinned and
//! then blocks holds every batch retired at or after its epoch until its next
//! pin, flush or exit; `quiesce` ends that reservation, costs nothing when
//! there is nothing to end, and otherwise does what `flush` does.
//!
//! Each test runs on fresh threads and the tests are serialized, as the
//! others of this binary: the slots of every thread decide which batches wait.

use super::{
    Counted, DEADLINE, Hold, QUIESCE_BODIES, eventually, flush_until_none_parked, join_within,
    lock, own_tid, published_epoch, spend_first_retire, with_handle,
};
use crate::atom::Atom;
use crate::guard::{pin, quiesce};
use crate::slot::{self, RETIRE_FREQ};
use crate::stall::Step;
use std::boxed::Box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

/// How many times this thread's `quiesce` ran past its fast path.
fn bodies() -> usize {
    QUIESCE_BODIES.with(|n| n.get())
}

/// The words of this thread's slot a quiesce may write: the published epoch
/// and the head of the list of batches placed in it.
fn slot_words() -> (u64, u64) {
    with_handle(|h| {
        let slots = h.global().thread_slots(h.tid());
        (slots.epoch[0].load_lo(), slots.first[0].load_lo())
    })
}

/// Retires `n` values stamped with the first epoch: their batches take a node
/// in every active slot.
fn retire_oldest_n(live: &Arc<AtomicUsize>, n: usize) {
    for _ in 0..n {
        Counted::retire_oldest(live);
    }
}

/// A thread that pins once, ends its reservation with `end`, tells the test,
/// then blocks on a channel, as a receiver does while it waits.
struct Blocked {
    release: mpsc::Sender<()>,
    thread: JoinHandle<()>,
}

impl Blocked {
    fn spawn(end: fn()) -> Self {
        let (release, wait) = mpsc::channel::<()>();
        let ready = Hold::new();
        let r = ready.clone();
        let thread = thread::spawn(move || {
            drop(pin());
            end();
            r.arrived.store(true, Ordering::SeqCst);
            let _ = wait.recv_timeout(DEADLINE);
        });
        ready.reached("the blocked thread");
        Self { release, thread }
    }

    fn release(self) {
        let _ = self.release.send(());
        join_within(self.thread, "the blocked thread");
    }
}

fn nothing() {}

/// What `RETIRE_FREQ` values left live after another thread retired them
/// (one full batch, submitted by the last retire) and flushed, while a thread
/// that pinned and ended its reservation with `end` stays blocked; and what
/// is left once it is released.
fn live_while_blocked(end: fn()) -> (usize, usize) {
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        let blocked = Blocked::spawn(end);
        retire_oldest_n(&l, RETIRE_FREQ);
        flush_all(&l);
        let while_blocked = l.load(Ordering::SeqCst);
        blocked.release();
        flush_all(&l);
        (while_blocked, l.load(Ordering::SeqCst))
    })
    .join()
    .unwrap()
}

/// Flushes until `live` reaches 0 or enough rounds passed to free anything
/// that can be freed.
fn flush_all(live: &Arc<AtomicUsize>) {
    for _ in 0..=slot::global().max_threads() {
        super::flush();
        if live.load(Ordering::SeqCst) == 0 {
            return;
        }
        thread::yield_now();
    }
}

/// The defect: values retired after a thread pinned and blocked wait for it
/// as long as it blocks. With `quiesce` before the block they do not.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_thread_that_quiesced_before_blocking_holds_nothing() {
    let _l = lock();
    // Without the call: the batch sits in the blocked thread's slot.
    let (held, after_release) = live_while_blocked(nothing);
    assert_eq!(
        held, RETIRE_FREQ,
        "a thread that pinned and blocked did not hold the batch: the defect is not reached"
    );
    assert_eq!(after_release, 0, "the release did not free the batch");
    // With it: freed while the thread stays blocked.
    let (held, after_release) = live_while_blocked(quiesce);
    assert_eq!(held, 0, "a quiesced, blocked thread still held the batch");
    assert_eq!(after_release, 0);
}

/// `flush` advances the global epoch every time; `quiesce` does not (one
/// quiet attempt in many is enough: other tests of this binary may advance it
/// meanwhile, and a `quiesce` that always advanced would never be quiet).
#[test]
fn quiesce_leaves_the_epoch_where_it_is_and_flush_advances_it() {
    let _l = lock();
    thread::spawn(|| {
        own_tid();
        let before = slot::epoch();
        super::flush();
        assert!(slot::epoch() > before, "flush did not advance the epoch");
        let quiet = (0..1_000).any(|_| {
            drop(pin());
            let before = slot::epoch();
            quiesce();
            slot::epoch() == before
        });
        assert!(quiet, "quiesce advanced the epoch every time");
        // And it is the same release: the reservation ends.
        drop(pin());
        assert_ne!(published_epoch(), 0);
        quiesce();
        assert_eq!(published_epoch(), 0);
    })
    .join()
    .unwrap();
}

/// A quiesce that submits a batch advances the epoch, as every retire that
/// places a batch does: the readers whose slots took it must see the epoch
/// move at their next pin to traverse it. A batch that leaves no slot
/// holding it is freed at once.
#[test]
fn a_quiesce_that_submits_a_batch_advances_the_epoch_and_frees_it() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        spend_first_retire(&l);
        for _ in 0..3 {
            Counted::retire_one(&l);
        }
        assert_eq!(with_handle(|h| h.batch_count.get()), 3);
        let before = slot::epoch();
        quiesce();
        assert_eq!(with_handle(|h| h.batch_count.get()), 0, "batch kept");
        assert!(slot::epoch() > before, "a submitted batch did not advance");
        assert!(eventually(|| l.load(Ordering::SeqCst) == 0));
    })
    .join()
    .unwrap();
}

/// The fast path: a thread that quiesced and neither pinned nor retired since
/// takes no step of the body and writes none of its slot's words; a pin, a
/// retire, or a parked batch sends the next call through the body again.
#[test]
fn a_second_quiesce_runs_nothing() {
    let _l = lock();
    thread::spawn(|| {
        own_tid();
        quiesce();
        let (words, count) = (slot_words(), bodies());
        assert_eq!(words.0, 0);
        for _ in 0..100 {
            quiesce();
        }
        assert_eq!(bodies(), count, "a repeated quiesce ran the body");
        assert_eq!(slot_words(), words);

        drop(pin());
        assert_ne!(published_epoch(), 0);
        quiesce();
        assert_eq!(
            bodies(),
            count + 1,
            "a quiesce after a pin took the fast path"
        );
        assert_eq!(published_epoch(), 0);
        quiesce();
        assert_eq!(bodies(), count + 1);

        let live = Arc::new(AtomicUsize::new(0));
        Counted::retire_one(&live);
        quiesce();
        assert_eq!(
            bodies(),
            count + 2,
            "a quiesce after a retire took the fast path"
        );
        assert!(eventually(|| live.load(Ordering::SeqCst) == 0));
    })
    .join()
    .unwrap();
}

/// With a guard live the reservation stays (the guard needs it) and the
/// partial batch is submitted: placed in this thread's own slot here, so it
/// is freed only after the guard drops and the slot is traversed.
#[test]
fn quiesce_with_a_guard_live_keeps_the_reservation_and_submits_the_batch() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        let guard = pin();
        let held = published_epoch();
        assert_ne!(held, 0);
        retire_oldest_n(&l, 10);
        quiesce();
        assert_eq!(
            with_handle(|h| h.batch_count.get()),
            0,
            "batch not submitted"
        );
        assert_eq!(published_epoch(), held, "the guard's reservation was ended");
        assert_eq!(l.load(Ordering::SeqCst), 10, "freed under a live guard");
        drop(guard);
        // With no guard left, the next quiesce traverses the slot the batch was placed in.
        quiesce();
        assert!(eventually(|| l.load(Ordering::SeqCst) == 0));
    })
    .join()
    .unwrap();
}

/// A value another thread replaced and retired stays valid under a guard
/// that called `quiesce` after loading it.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_loaded_value_survives_a_quiesce_under_its_guard() {
    // Padded to the 192-byte class, which no slab or magazine test counts the chunks of.
    struct Watched(Arc<AtomicBool>, u64, [u8; 140]);
    impl Drop for Watched {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let _l = lock();
    let dropped = Arc::new(AtomicBool::new(false));
    let atom = Arc::new(Atom::new(Watched(Arc::clone(&dropped), 7, [0; 140])));
    let (loaded, replaced) = (Hold::new(), Hold::new());
    let reader = {
        let (atom, dropped) = (Arc::clone(&atom), Arc::clone(&dropped));
        let (loaded, replaced) = (loaded.clone(), replaced.clone());
        thread::spawn(move || {
            let value = atom.load();
            loaded.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| replaced.go.load(Ordering::SeqCst)));
            quiesce();
            // The writer's flushes had every chance to free the old value.
            thread::yield_now();
            assert!(!dropped.load(Ordering::SeqCst), "freed under a live guard");
            assert_eq!((value.1, value.2[0]), (7, 0));
            drop(value);
        })
    };
    loaded.reached("the reader");
    let l = Arc::new(AtomicUsize::new(0));
    atom.store(Watched(Arc::clone(&dropped), 8, [0; 140]));
    // Enough retired values to submit batches placed in the reader's slot.
    retire_oldest_n(&l, RETIRE_FREQ);
    flush_all(&l);
    assert!(!dropped.load(Ordering::SeqCst), "freed under a live guard");
    replaced.release();
    join_within(reader, "the reader");
    drop(atom);
    super::flush();
}

/// A thread that never pinned, with nothing to submit and nothing parked,
/// takes no thread ID and runs no body.
#[test]
fn quiesce_without_a_tid_or_orphans_claims_none() {
    let _l = lock();
    thread::spawn(flush_until_none_parked).join().unwrap();
    let global = slot::global();
    let handed_out = global.max_threads();
    thread::spawn(|| {
        quiesce();
        quiesce();
        assert_eq!(
            with_handle(|h| h.tid.get()),
            None,
            "a quiesce claimed a tid"
        );
        assert_eq!(bodies(), 0);
    })
    .join()
    .unwrap();
    assert_eq!(
        global.max_threads(),
        handed_out,
        "a quiesce took a fresh tid"
    );
}

/// A thread that retired without ever pinning has a batch no pin submitted:
/// `quiesce` claims an ID for it as `flush` does, and frees it.
#[test]
fn quiesce_submits_what_a_thread_retired_without_pinning() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        for _ in 0..10 {
            Counted::retire_one(&l);
        }
        assert_eq!(with_handle(|h| h.batch_count.get()), 10);
        quiesce();
        assert_eq!(with_handle(|h| h.batch_count.get()), 0);
        assert!(eventually(|| l.load(Ordering::SeqCst) == 0));
    })
    .join()
    .unwrap();
}

/// What an exiting thread parked on its ID is adopted by a `quiesce`, on a
/// thread that never pinned too, as a `flush` adopts it: one value parked by
/// an exit while a reader is pinned, adopted and placed by the quiesce with
/// the quiescing thread's own, freed once the reader is gone.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn quiesce_adopts_what_an_exit_parked() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let pinned = Hold::new();
    let done = Hold::new();
    let reader = {
        let (pinned, done) = (pinned.clone(), done.clone());
        thread::spawn(move || {
            let guard = pin();
            pinned.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| done.go.load(Ordering::SeqCst)));
            drop(guard);
        })
    };
    pinned.reached("the reader");
    flush_until_none_parked();
    let l = Arc::clone(&live);
    thread::spawn(move || Counted::retire_oldest(&l))
        .join()
        .unwrap();
    assert!(slot::orphans_parked(), "the exit placed or freed its value");

    let l = Arc::clone(&live);
    thread::spawn(move || {
        assert_eq!(with_handle(|h| h.tid.get()), None);
        retire_oldest_n(&l, 10);
        let before = slot::epoch();
        quiesce();
        assert!(
            with_handle(|h| h.tid.get()).is_some(),
            "no tid to adopt with"
        );
        assert!(!slot::orphans_parked(), "the parked batch stayed parked");
        assert_eq!(with_handle(|h| h.batch_count.get()), 0);
        assert!(slot::epoch() > before);
        // Held by the reader's slot: placed, not freed.
        assert_eq!(l.load(Ordering::SeqCst), 11);
    })
    .join()
    .unwrap();
    done.release();
    join_within(reader, "the reader");
    thread::spawn(move || {
        own_tid();
        flush_until_none_parked();
        super::flush();
    })
    .join()
    .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0, "a parked value was lost");
}

/// A value whose destructor calls `quiesce` is freed by a flush and by an
/// exit (where the call reaches the exiting handle, which is reclaiming and
/// does nothing) without a hang, a double free or a lost value.
#[test]
fn a_destructor_may_quiesce_inside_a_flush_and_an_exit() {
    #[repr(C)]
    struct Quiescing {
        retired: crate::retired::RetiredNode,
        live: Arc<AtomicUsize>,
    }
    impl Drop for Quiescing {
        fn drop(&mut self) {
            quiesce();
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }
    fn retire_one(live: &Arc<AtomicUsize>) {
        live.fetch_add(1, Ordering::SeqCst);
        let node = Box::into_raw(Box::new(Quiescing {
            retired: crate::retired::RetiredNode::new(),
            live: Arc::clone(live),
        }));
        unsafe { crate::guard::retire(node) };
    }
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        retire_one(&l);
        retire_one(&l);
        super::flush();
        assert!(eventually(|| l.load(Ordering::SeqCst) == 0));
        retire_one(&l);
        // Left for the exit to submit and free.
    })
    .join()
    .unwrap();
    assert!(eventually(|| {
        super::flush();
        live.load(Ordering::SeqCst) == 0
    }));
}

/// A submission that checked the epoch of a slot just before its owner's
/// quiesce stored 0 inserts its node after the clear. The owner's next quiesce
/// must not take its fast path (epoch 0, empty batch) over that node: it reads
/// its own list, and frees the batch.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_node_inserted_after_the_clear_is_freed_by_the_next_quiesce() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let (pinned, cleared, again) = (Hold::new(), Hold::new(), Hold::new());
    let reader = {
        let (pinned, cleared, again) = (pinned.clone(), cleared.clone(), again.clone());
        let l = Arc::clone(&live);
        thread::spawn(move || {
            drop(pin());
            pinned.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| cleared.go.load(Ordering::SeqCst)));
            quiesce();
            assert_eq!(published_epoch(), 0);
            cleared.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| again.go.load(Ordering::SeqCst)));
            let before = bodies();
            quiesce();
            assert_eq!(
                bodies(),
                before + 1,
                "a node in the own list was passed over"
            );
            assert!(eventually(|| l.load(Ordering::SeqCst) == 0));
        })
    };
    pinned.reached("the reader");
    let inserting = Hold::new();
    let l = Arc::clone(&live);
    let hold = inserting.clone();
    let writer = thread::spawn(move || {
        hold.arm(Step::InsertReady, own_tid());
        retire_oldest_n(&l, RETIRE_FREQ);
    });
    inserting.reached("the submitting thread");
    cleared.release();
    cleared.reached("the reader's quiesce");
    inserting.release();
    join_within(writer, "the submitting thread");
    assert_eq!(
        live.load(Ordering::SeqCst),
        RETIRE_FREQ,
        "the node was not inserted"
    );
    again.release();
    join_within(reader, "the reader");
}

/// A quiesce whose batch is too small for the active slots (a batch needs
/// more nodes than there are eligible slots) hands it over, as an exit does,
/// instead of keeping it: thread A retires one value and quiesces while three
/// threads hold reservations, then blocks holding nothing; once the others
/// quiesce, the value is freed while A stays blocked.
#[test]
#[cfg_attr(miri, ignore)] // multi-threaded: hits the intentional mixed-size DCAS, outside Miri's model
fn a_quiesce_with_a_small_batch_hands_it_over() {
    const OTHERS: usize = 3;
    let _l = lock();
    thread::spawn(flush_until_none_parked).join().unwrap();
    let live = Arc::new(AtomicUsize::new(0));
    let mut ready = Vec::new();
    let mut steps = Vec::new();
    let mut threads = Vec::new();
    for _ in 0..OTHERS {
        let (up, go, done) = (Hold::new(), Hold::new(), Hold::new());
        let (u, g, d) = (up.clone(), go.clone(), done.clone());
        threads.push(thread::spawn(move || {
            drop(pin());
            u.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| g.go.load(Ordering::SeqCst)));
            quiesce();
            d.arrived.store(true, Ordering::SeqCst);
            assert!(eventually(|| d.go.load(Ordering::SeqCst)));
        }));
        ready.push(up);
        steps.push((go, done));
    }
    for up in &ready {
        up.reached("a thread holding a reservation");
    }
    let (retired, release) = (Hold::new(), Hold::new());
    let (r, rel, l) = (retired.clone(), release.clone(), Arc::clone(&live));
    let a = thread::spawn(move || {
        Counted::retire_oldest(&l);
        quiesce();
        assert_eq!(
            with_handle(|h| h.batch_count.get()),
            0,
            "the quiescing thread kept what it retired"
        );
        r.arrived.store(true, Ordering::SeqCst);
        assert!(eventually(|| rel.go.load(Ordering::SeqCst)));
    });
    retired.reached("the quiescing thread");
    assert_eq!(
        live.load(Ordering::SeqCst),
        1,
        "the value was freed too early"
    );
    for (go, done) in &steps {
        go.release();
        done.reached("a thread that quiesced");
    }
    assert!(
        eventually(|| live.load(Ordering::SeqCst) == 0),
        "the handed-over value was never freed while its thread stayed blocked"
    );
    release.release();
    join_within(a, "the quiescing thread");
    for (_, done) in &steps {
        done.release();
    }
    for t in threads {
        join_within(t, "a thread holding a reservation");
    }
}
