use kovan::{Atomic, RetiredNode, Shared, pin, retire};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::signal::{AsyncSignal, Notifier, Signal};
use std::collections::LinkedList;
use std::sync::Mutex;

#[repr(C)]
pub(crate) struct Node<T> {
    retired: RetiredNode,
    data: Option<T>,
    next: Atomic<Node<T>>,
}

impl<T> Node<T> {
    fn new(data: Option<T>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            data,
            next: Atomic::null(),
        }))
    }
}

pub(crate) struct Channel<T: 'static> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
    receivers: Mutex<LinkedList<Arc<dyn Notifier>>>,
    /// Number of live Sender handles. When this reaches 0, the channel is disconnected.
    sender_count: AtomicUsize,
    /// Set to true when all senders have been dropped.
    disconnected: AtomicBool,
}

impl<T: 'static> Channel<T> {
    pub(crate) fn new() -> Self {
        let sentinel = Node::new(None);
        Self {
            head: Atomic::new(sentinel),
            tail: Atomic::new(sentinel),
            receivers: Mutex::new(LinkedList::new()),
            sender_count: AtomicUsize::new(1), // Starts at 1 for the initial Sender
            disconnected: AtomicBool::new(false),
        }
    }

    /// Wake all blocked receivers (used when senders disconnect).
    fn wake_all_receivers(&self) {
        let mut receivers = self.receivers.lock().unwrap();
        while let Some(signal) = receivers.pop_front() {
            signal.notify();
        }
    }
}

impl<T: 'static> Drop for Channel<T> {
    fn drop(&mut self) {
        // Sentinel node retirement logic
        //
        // At creation time `head == tail == sentinel` (a dummy `Node` with
        // `data: None`).  As messages are dequeued, `try_recv()` advances `head`
        // and calls `retire(old_head)` on each node that leaves the list.  The
        // *new* head is never retired by `try_recv`; only its predecessor is.
        //
        // Therefore at `Drop` time, `self.head` is always an un-retired node:
        //   - the original sentinel, if nothing was ever sent+received,
        //   - the last node that became head after a dequeue, or
        //   - a data node if messages are still pending.
        //
        // The loop walks head->tail and retires each node exactly once.
        // Empty-channel path: `head == tail == sentinel`, next is null;
        // we retire the sentinel and the loop exits cleanly with no double-free.
        let guard = pin();
        let mut curr = self.head.load(Ordering::Relaxed, &guard);

        while !curr.is_null() {
            let next = unsafe { curr.deref().next.load(Ordering::Relaxed, &guard) };
            unsafe { retire(curr.as_raw()) };
            curr = next;
        }

        // Force-flush retired nodes on this thread to prevent use-after-free
        // during process teardown when kovan's global state is being destroyed.
        drop(guard);
        kovan::flush();
    }
}

/// The sending half of an unbounded channel.
pub struct Sender<T: 'static> {
    inner: Arc<Channel<T>>,
}

impl<T: 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: 'static> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.inner.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last sender dropped — mark channel as disconnected and wake all receivers.
            self.inner.disconnected.store(true, Ordering::Release);
            self.inner.wake_all_receivers();
        }
    }
}

unsafe impl<T: 'static + Send> Send for Sender<T> {}
unsafe impl<T: 'static + Send> Sync for Sender<T> {}

/// The receiving half of an unbounded channel.
pub struct Receiver<T: 'static> {
    inner: Arc<Channel<T>>,
}

impl<T: 'static> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

unsafe impl<T: 'static + Send> Send for Receiver<T> {}
unsafe impl<T: 'static + Send> Sync for Receiver<T> {}

/// Creates a channel of unbounded capacity.
pub fn channel<T: 'static>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Channel::new());
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T: 'static> Sender<T> {
    /// Sends a message into the channel.
    pub fn send(&self, t: T) {
        let node = Node::new(Some(t));
        let guard = pin();

        loop {
            let tail = self.inner.tail.load(Ordering::Acquire, &guard);
            let next = unsafe { tail.deref().next.load(Ordering::Acquire, &guard) };

            if next.is_null() {
                // Tail is pointing to the last node
                // Try to link new node
                let node_shared = unsafe { Shared::from_raw(node) };
                match unsafe {
                    tail.deref().next.compare_exchange(
                        next,
                        node_shared,
                        Ordering::Release,
                        Ordering::Relaxed,
                        &guard,
                    )
                } {
                    Ok(_) => {
                        // Successfully linked. Now try to swing tail.
                        let _ = self.inner.tail.compare_exchange(
                            tail,
                            node_shared,
                            Ordering::Release,
                            Ordering::Relaxed,
                            &guard,
                        );

                        // Notify one receiver
                        let mut receivers = self.inner.receivers.lock().unwrap();
                        if let Some(signal) = receivers.pop_front() {
                            signal.notify();
                        }

                        return;
                    }
                    Err(_) => continue, // Failed to link, retry
                }
            } else {
                // Tail is not pointing to the last node, try to swing it
                let _ = self.inner.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                );
            }
        }
    }
}

impl<T: 'static> Receiver<T> {
    /// Returns `true` if all senders have been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.inner.disconnected.load(Ordering::Acquire)
    }

    /// Attempts to receive a message from the channel without blocking.
    pub fn try_recv(&self) -> Option<T> {
        let guard = pin();

        loop {
            let head = self.inner.head.load(Ordering::Acquire, &guard);
            let tail = self.inner.tail.load(Ordering::Acquire, &guard);
            let next = unsafe { head.deref().next.load(Ordering::Acquire, &guard) };

            if head == tail {
                if next.is_null() {
                    return None; // Empty
                }
                // Tail is falling behind, try to advance it
                let _ = self.inner.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                );
            } else {
                // Head != Tail, queue not empty
                if next.is_null() {
                    // Inconsistent state, retry
                    continue;
                }

                // Read value before CAS
                let data_ptr = unsafe { core::ptr::addr_of_mut!((*next.as_raw()).data) };

                match self.inner.head.compare_exchange(
                    head,
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                ) {
                    Ok(_) => {
                        // We won. We can take the data.
                        // The old head is retired.
                        unsafe { retire(head.as_raw()) };

                        // We take the data from `next`.
                        // `next` is now the sentinel.
                        // Its data is logically gone.
                        //
                        // CRITICAL: use `ptr::replace` (not `ptr::read`) to clear
                        // the source field. `ptr::read` leaves the bit pattern
                        // intact, so when this node is later retired and freed by
                        // kovan, `Node::drop` would drop `data: Option<T>` again
                        // — a double-free. Writing `None` ensures the destructor
                        // sees an empty Option and skips the inner drop.
                        let data = unsafe { ptr::replace(data_ptr, None) };
                        return data;
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    /// Receives a message from the channel, blocking if empty.
    ///
    /// Returns `None` when the channel is empty **and** all senders have been dropped.
    pub fn recv(&self) -> Option<T> {
        if let Some(msg) = self.try_recv() {
            return Some(msg);
        }

        // Fast path: already disconnected and empty
        if self.is_disconnected() {
            return self.try_recv(); // Drain remaining
        }

        loop {
            let signal = Arc::new(Signal::new());
            // Register signal
            {
                let mut receivers = self.inner.receivers.lock().unwrap();
                receivers.push_back(signal.clone());
            }

            // Re-check to avoid race
            if let Some(msg) = self.try_recv() {
                return Some(msg);
            }

            // Check disconnection after registering signal but before parking
            if self.is_disconnected() {
                return self.try_recv(); // Drain remaining
            }

            signal.wait();

            // Woken up — either a message arrived or senders disconnected
            if let Some(msg) = self.try_recv() {
                return Some(msg);
            }

            // Woken by disconnect with empty queue
            if self.is_disconnected() {
                return None;
            }
        }
    }

    /// Returns true if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let guard = pin();
        let head = self.inner.head.load(Ordering::Relaxed, &guard);
        let tail = self.inner.tail.load(Ordering::Relaxed, &guard);
        let next = unsafe { head.deref().next.load(Ordering::Relaxed, &guard) };
        head == tail && next.is_null()
    }

    /// Registers a signal for notification when a message arrives.
    ///
    /// This is used for `select!` implementation.
    pub fn register_signal(&self, signal: Arc<dyn Notifier>) {
        let mut receivers = self.inner.receivers.lock().unwrap();
        receivers.push_back(signal);
    }

    /// Receives a message from the channel asynchronously.
    ///
    /// Returns `None` when the channel is empty and all senders have been dropped.
    pub async fn recv_async(&self) -> Option<T> {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct RecvFuture<'a, T: 'static> {
            receiver: &'a Receiver<T>,
            signal: Arc<AsyncSignal>,
        }

        impl<'a, T: 'static> Unpin for RecvFuture<'a, T> {}

        impl<'a, T: 'static> Future for RecvFuture<'a, T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                if let Some(msg) = this.receiver.try_recv() {
                    return Poll::Ready(Some(msg));
                }

                // Disconnected and empty — done
                if this.receiver.is_disconnected() {
                    return Poll::Ready(this.receiver.try_recv());
                }

                if this.signal.is_notified() {
                    this.signal = Arc::new(AsyncSignal::new());
                }

                this.signal.register(cx.waker());

                // Register signal
                {
                    let mut receivers = this.receiver.inner.receivers.lock().unwrap();
                    receivers.push_back(this.signal.clone());
                }

                // Re-check
                if let Some(msg) = this.receiver.try_recv() {
                    return Poll::Ready(Some(msg));
                }

                if this.receiver.is_disconnected() {
                    return Poll::Ready(None);
                }

                Poll::Pending
            }
        }

        RecvFuture {
            receiver: self,
            signal: Arc::new(AsyncSignal::new()),
        }
        .await
    }
}
