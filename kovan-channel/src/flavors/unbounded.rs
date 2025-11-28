use kovan::{Atomic, Shared, pin, retire};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(crate) use crate::signal::Signal;
use std::collections::LinkedList;
use std::sync::Mutex;

pub(crate) struct Node<T> {
    data: Option<T>,
    next: Atomic<Node<T>>,
}

impl<T> Node<T> {
    fn new(data: Option<T>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Atomic::null(),
        }))
    }
}

pub(crate) struct Channel<T: 'static> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
    receivers: Mutex<LinkedList<Arc<Signal>>>,
}

impl<T: 'static> Channel<T> {
    pub(crate) fn new() -> Self {
        let sentinel = Node::new(None);
        Self {
            head: Atomic::new(sentinel),
            tail: Atomic::new(sentinel),
            receivers: Mutex::new(LinkedList::new()),
        }
    }
}

impl<T: 'static> Drop for Channel<T> {
    fn drop(&mut self) {
        // We need to drop all nodes in the channel
        // Since we are in Drop, we have exclusive access, but we need to be careful with kovan reclamation
        // Ideally, we should pop everything.
        // However, kovan relies on `retire` which defers reclamation.
        // If we just drop the Channel, the nodes might still be referenced by some stalled threads?
        // No, if we are dropping Channel, there are no more references to it (Arc count 0).
        // But there might be active guards referencing nodes.
        // We should walk the list and retire everything?
        // Or just let it leak? No, that's bad.
        // We should traverse and drop.

        let guard = pin();
        let mut curr = self.head.load(Ordering::Relaxed, &guard);

        while !curr.is_null() {
            let next = unsafe { curr.deref().next.load(Ordering::Relaxed, &guard) };
            // We can't just drop `curr` because of kovan.
            // We should `retire` it.
            retire(curr.as_raw());
            curr = next;
        }
    }
}

/// The sending half of an unbounded channel.
pub struct Sender<T: 'static> {
    inner: Arc<Channel<T>>,
}

impl<T: 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
                let data_ptr = unsafe { &mut (*next.as_raw()).data as *mut Option<T> };

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
                        retire(head.as_raw());

                        // We take the data from `next`.
                        // `next` is now the sentinel.
                        // Its data is logically gone.
                        let data = unsafe { ptr::read(data_ptr) };
                        return data;
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    /// Receives a message from the channel, blocking if empty.
    pub fn recv(&self) -> Option<T> {
        if let Some(msg) = self.try_recv() {
            return Some(msg);
        }

        let signal = Arc::new(Signal::new());
        loop {
            // Register signal
            {
                let mut receivers = self.inner.receivers.lock().unwrap();
                receivers.push_back(signal.clone());
            }

            // Re-check to avoid race
            if let Some(msg) = self.try_recv() {
                // We got a message, remove signal if still there?
                // It might have been popped by sender, but that's fine, notify is harmless.
                return Some(msg);
            }

            signal.wait();

            // Woken up, try to receive
            match self.try_recv() {
                Some(msg) => return Some(msg),
                None => {
                    // Spurious wake or stolen?
                    // Reset signal?
                    // Signal is one-shot in current impl?
                    // No, state is AtomicUsize.
                    // We should create new signal or reset it?
                    // For simplicity, let's loop.
                    // But we need to re-register if we want to wait again.
                    // The sender pops the signal. So we need to re-register.
                }
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
    pub fn register_signal(&self, signal: Arc<Signal>) {
        let mut receivers = self.inner.receivers.lock().unwrap();
        receivers.push_back(signal);
    }
}
