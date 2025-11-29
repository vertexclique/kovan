use crate::flavors::unbounded;
use crate::signal::{AsyncSignal, Notifier, Signal};
use crossbeam_utils::Backoff;
use std::collections::LinkedList;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct Channel<T: 'static> {
    sender: unbounded::Sender<T>,
    receiver: unbounded::Receiver<T>,
    capacity: usize,
    len: AtomicUsize,
    senders: Mutex<LinkedList<Arc<dyn Notifier>>>,
    receivers: Mutex<LinkedList<Arc<dyn Notifier>>>,
}

/// The sending half of a bounded channel.
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

/// The receiving half of a bounded channel.
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

impl<T: 'static> Channel<T> {
    fn new(capacity: usize) -> Self {
        let (sender, receiver) = unbounded::channel();
        Self {
            sender,
            receiver,
            capacity,
            len: AtomicUsize::new(0),
            senders: Mutex::new(LinkedList::new()),
            receivers: Mutex::new(LinkedList::new()),
        }
    }
}

impl<T: 'static> Sender<T> {
    /// Sends a message into the channel, blocking if full.
    pub fn send(&self, t: T) {
        let backoff = Backoff::new();
        loop {
            let len = self.inner.len.load(Ordering::Relaxed);
            if len >= self.inner.capacity {
                // Blocking path
                let signal = Arc::new(Signal::new());
                {
                    let mut senders = self.inner.senders.lock().unwrap();
                    if self.inner.len.load(Ordering::Relaxed) >= self.inner.capacity {
                        senders.push_back(signal.clone());
                    } else {
                        // Capacity available, retry loop
                        continue;
                    }
                }
                signal.wait();
                continue;
            }

            // Try to reserve a slot
            if self
                .inner
                .len
                .compare_exchange(len, len + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                // Success, send the message
                self.inner.sender.send(t);

                // Notify one receiver
                let mut receivers = self.inner.receivers.lock().unwrap();
                if let Some(signal) = receivers.pop_front() {
                    signal.notify();
                }

                return;
            }
            // Failed to reserve, retry
            backoff.snooze();
        }
    }
    /// Returns true if the channel is full.
    pub fn is_full(&self) -> bool {
        self.inner.len.load(Ordering::Relaxed) >= self.inner.capacity
    }

    /// Registers a signal for notification when space becomes available.
    ///
    /// This is used for `select!` implementation.
    pub fn register_signal(&self, signal: Arc<dyn Notifier>) {
        let mut senders = self.inner.senders.lock().unwrap();
        senders.push_back(signal);
    }

    /// Sends a message into the channel asynchronously.
    pub async fn send_async(&self, t: T) {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct SendFuture<'a, T: 'static> {
            sender: &'a Sender<T>,
            t: Option<T>,
            signal: Arc<AsyncSignal>,
        }

        impl<'a, T: 'static> Unpin for SendFuture<'a, T> {}

        impl<'a, T: 'static> Future for SendFuture<'a, T> {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                let len = this.sender.inner.len.load(Ordering::Relaxed);
                if len < this.sender.inner.capacity
                    && this
                        .sender
                        .inner
                        .len
                        .compare_exchange(len, len + 1, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                {
                    // Success, send the message
                    let t = this.t.take().unwrap();
                    this.sender.inner.sender.send(t);

                    // Notify one receiver
                    let mut receivers = this.sender.inner.receivers.lock().unwrap();
                    if let Some(signal) = receivers.pop_front() {
                        signal.notify();
                    }

                    return Poll::Ready(());
                }

                if this.signal.is_notified() {
                    this.signal = Arc::new(AsyncSignal::new());
                }
                this.signal.register(cx.waker());

                {
                    let mut senders = this.sender.inner.senders.lock().unwrap();
                    // Only push if full
                    if this.sender.inner.len.load(Ordering::Relaxed) >= this.sender.inner.capacity {
                        senders.push_back(this.signal.clone());
                    }
                }

                // Re-check
                let len = this.sender.inner.len.load(Ordering::Relaxed);
                if len < this.sender.inner.capacity
                    && this
                        .sender
                        .inner
                        .len
                        .compare_exchange(len, len + 1, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                {
                    // Success, send the message
                    let t = this.t.take().unwrap();
                    this.sender.inner.sender.send(t);

                    // Notify one receiver
                    let mut receivers = this.sender.inner.receivers.lock().unwrap();
                    if let Some(signal) = receivers.pop_front() {
                        signal.notify();
                    }

                    return Poll::Ready(());
                }

                Poll::Pending
            }
        }

        SendFuture {
            sender: self,
            t: Some(t),
            signal: Arc::new(AsyncSignal::new()),
        }
        .await
    }
}

impl<T: 'static> Receiver<T> {
    /// Returns true if the channel is empty.
    pub fn is_empty(&self) -> bool {
        // This is approximate
        self.inner.receiver.is_empty()
    }

    /// Registers a signal for notification when a message arrives.
    ///
    /// This is used for `select!` implementation.
    pub fn register_signal(&self, signal: Arc<dyn Notifier>) {
        let mut receivers = self.inner.receivers.lock().unwrap();
        receivers.push_back(signal);
    }
    /// Attempts to receive a message from the channel without blocking.
    pub fn try_recv(&self) -> Option<T> {
        // Try to receive from the underlying queue
        // Note: unbounded::Receiver::try_recv returns None if empty
        match self.inner.receiver.try_recv() {
            Some(msg) => {
                // Decrement length
                self.inner.len.fetch_sub(1, Ordering::Release);

                // Notify one sender
                let mut senders = self.inner.senders.lock().unwrap();
                if let Some(signal) = senders.pop_front() {
                    signal.notify();
                }

                Some(msg)
            }
            None => None,
        }
    }

    /// Receives a message from the channel, blocking if empty.
    pub fn recv(&self) -> Option<T> {
        if let Some(msg) = self.try_recv() {
            return Some(msg);
        }

        loop {
            let signal = Arc::new(Signal::new());
            // Register signal
            {
                let mut receivers = self.inner.receivers.lock().unwrap();
                receivers.push_back(signal.clone());
            }

            // Re-check
            if let Some(msg) = self.try_recv() {
                return Some(msg);
            }

            signal.wait();

            if let Some(msg) = self.try_recv() {
                return Some(msg);
            }
        }
    }

    /// Receives a message from the channel asynchronously.
    pub async fn recv_async(&self) -> T {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct RecvFuture<'a, T: 'static> {
            receiver: &'a Receiver<T>,
            signal: Arc<AsyncSignal>,
        }

        impl<'a, T: 'static> Unpin for RecvFuture<'a, T> {}

        impl<'a, T: 'static> Future for RecvFuture<'a, T> {
            type Output = T;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                if let Some(msg) = this.receiver.try_recv() {
                    return Poll::Ready(msg);
                }

                if this.signal.is_notified() {
                    this.signal = Arc::new(AsyncSignal::new());
                }
                this.signal.register(cx.waker());

                {
                    let mut receivers = this.receiver.inner.receivers.lock().unwrap();
                    receivers.push_back(this.signal.clone());
                }

                if let Some(msg) = this.receiver.try_recv() {
                    return Poll::Ready(msg);
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

/// Creates a channel of bounded capacity.
pub fn channel<T: 'static>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Channel::new(cap));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}
