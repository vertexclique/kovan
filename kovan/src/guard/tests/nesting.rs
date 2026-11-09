//! A destructor that retires or pins never nests in the reclamation that
//! runs it: a submission frees nothing itself (the batches it frees wait on
//! the cache), and a guard's drop inside a drain leaves the freeing to that
//! drain. The depth of `free_batch_list` on the thread is the measure, so
//! the tests do not depend on timing.

use super::{eventually, lock, own_tid};
use crate::guard::{flush, pin, retire};
use crate::reclaim::depth;
use crate::retired::RetiredNode;
use crate::slot::RETIRE_FREQ;
use std::boxed::Box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

/// A value whose destructor retires `RETIRE_FREQ` more, the first of them
/// a link of the same kind one shorter: the last retire fills the batch, so
/// it is submitted inside the destructor, and with no other slot active
/// its count is zero at once.
#[repr(C)]
struct Link {
    retired: RetiredNode,
    live: Arc<AtomicUsize>,
    left: usize,
}

impl Link {
    fn retire_new(live: &Arc<AtomicUsize>, left: usize) {
        live.fetch_add(1, Ordering::SeqCst);
        let node = Box::into_raw(Box::new(Link {
            retired: RetiredNode::new(),
            live: Arc::clone(live),
            left,
        }));
        unsafe { retire(node) };
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        if self.left > 0 {
            Link::retire_new(&self.live, self.left - 1);
            for _ in 1..RETIRE_FREQ {
                Link::retire_new(&self.live, 0);
            }
        }
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Eight links deep, each destructor submitting a batch of its own: the
/// frees of the whole chain run one after the other in the flush's drain,
/// never one inside another. A submission that freed in place nested eight
/// deep (a stack overflow at a few thousand links).
#[test]
fn destructors_that_submit_batches_free_one_after_another() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        depth::reset();
        Link::retire_new(&l, 8);
        assert!(
            eventually(|| {
                flush();
                l.load(Ordering::SeqCst) == 0
            }),
            "{} values never freed",
            l.load(Ordering::SeqCst)
        );
        assert_eq!(depth::max(), 1, "a free ran inside another free");
    })
    .join()
    .unwrap();
}

/// A value whose destructor submits a batch (which waits on the cache) and
/// then pins and drops its guard: an outermost section, whose leave finds
/// the cache holding a batch and would free it inside the destructor's
/// frame.
#[repr(C)]
struct Pins {
    retired: RetiredNode,
    live: Arc<AtomicUsize>,
}

impl Drop for Pins {
    fn drop(&mut self) {
        for _ in 0..RETIRE_FREQ {
            Link::retire_new(&self.live, 0);
        }
        drop(pin());
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A guard dropped inside a drain adds to the cache and the drain's loop
/// frees it: with the cache holding a batch while a destructor leaves a
/// section, the free depth stays one.
#[test]
fn a_leave_inside_a_drain_leaves_the_freeing_to_the_drain() {
    let _l = lock();
    let live = Arc::new(AtomicUsize::new(0));
    let l = Arc::clone(&live);
    thread::spawn(move || {
        own_tid();
        depth::reset();
        for _ in 0..2 * RETIRE_FREQ {
            l.fetch_add(1, Ordering::SeqCst);
            let node = Box::into_raw(Box::new(Pins {
                retired: RetiredNode::new(),
                live: Arc::clone(&l),
            }));
            unsafe { retire(node) };
        }
        assert!(
            eventually(|| {
                flush();
                l.load(Ordering::SeqCst) == 0
            }),
            "{} values never freed",
            l.load(Ordering::SeqCst)
        );
        assert_eq!(depth::max(), 1, "a free ran inside another free");
    })
    .join()
    .unwrap();
}
