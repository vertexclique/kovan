#[cfg(not(target_arch = "wasm32"))]
use crate::flavors::RecvDeadline;
use crate::flavors::unbounded;
#[cfg(not(target_arch = "wasm32"))]
use crate::signal::Signal;
use crate::signal::{AsyncSignal, Notifier};
use crate::waitlist::{WaitList, wakeup_fence};
#[cfg(not(target_arch = "wasm32"))]
use crossbeam_utils::Backoff;
use std::sync::Arc;
use std::sync::atomic::Ordering;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

// Shuttle-instrumented so `len`/`sender_count`/`disconnected` -- raced by
// a blocking `send`/`recv`'s register/recheck against the other side's
// publish/notify, see `crate::waitlist` -- are checker scheduling points,
// matching `signal.rs`'s `Signal` swap.
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct Channel<T: 'static> {
    sender: unbounded::Sender<T>,
    receiver: unbounded::Receiver<T>,
    capacity: usize,
    len: AtomicUsize,
    senders: WaitList,
    receivers: WaitList,
    /// Number of live bounded Sender handles.
    sender_count: AtomicUsize,
    /// Set when all bounded senders are dropped.
    disconnected: AtomicBool,
}

/// The sending half of a bounded channel.
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
            self.inner.disconnected.store(true, Ordering::Release);
            // Loss-free wakeup: pairs with every receiver's
            // register-then-recheck. See `crate::waitlist` module docs.
            wakeup_fence();
            self.inner.receivers.notify_all();
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
            senders: WaitList::new(),
            receivers: WaitList::new(),
            sender_count: AtomicUsize::new(1),
            disconnected: AtomicBool::new(false),
        }
    }
}

impl<T: 'static> Sender<T> {
    /// Sends a message into the channel, blocking if full.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn send(&self, t: T) {
        let backoff = Backoff::new();
        loop {
            let len = self.inner.len.load(Ordering::Relaxed);
            if len >= self.inner.capacity {
                // Register unconditionally, then recheck -- the loss-free
                // wakeup protocol documented in `crate::waitlist`.
                let signal = Arc::new(Signal::new());
                self.inner.senders.register(signal.clone());
                wakeup_fence();

                if self.inner.len.load(Ordering::Acquire) < self.inner.capacity {
                    // Capacity freed up between our first check and
                    // registering: we won't actually wait, so mark our
                    // own entry stale (see `waitlist` docs) and retry.
                    signal.notify();
                    continue;
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
                wakeup_fence();
                self.inner.receivers.notify_one();

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
    /// This is the primitive `select!` builds on to wait across multiple
    /// channels with a single `Signal`. It exposes step 1 of the crate's
    /// register -> fence -> recheck -> park loss-free wakeup protocol (see
    /// the `crate::waitlist` module docs) without the other three steps:
    /// the caller owns the rest of the contract -- after registering on
    /// every channel it cares about, it MUST call
    /// `crate::waitlist::wakeup_fence` (or an equivalent `SeqCst` fence)
    /// and then recheck every channel's condition *before*
    /// parking/awaiting on `signal`. Skipping the fence or the recheck
    /// reopens the exact store-buffering race the protocol exists to
    /// close, and a real wakeup can be silently lost.
    ///
    /// `signal` must be a fresh registration, not a `Notifier` that was
    /// already notified (for example, reused across loop iterations
    /// without resetting): an already-notified entry is indistinguishable
    /// from a stale one, and `notify_one` will pop and discard it unread,
    /// silently starving this registration forever.
    pub fn register_signal(&self, signal: Arc<dyn Notifier>) {
        debug_assert!(
            !signal.is_notified(),
            "register_signal: registering an already-notified Notifier; it will be treated as \
             a stale entry and skipped by notify_one, silently losing this registration's wakeup"
        );
        self.inner.senders.register(signal);
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
                    wakeup_fence();
                    this.sender.inner.receivers.notify_one();

                    return Poll::Ready(());
                }

                if this.signal.is_notified() {
                    this.signal = Arc::new(AsyncSignal::new());
                }
                this.signal.register(cx.waker());
                this.sender.inner.senders.register(this.signal.clone());
                wakeup_fence();

                // Re-check: capacity may have freed up since the fast-path
                // attempt above.
                let len = this.sender.inner.len.load(Ordering::Acquire);
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
                    wakeup_fence();
                    this.sender.inner.receivers.notify_one();

                    // We registered but are not going to wait after all:
                    // mark our own entry stale (see `waitlist` docs).
                    this.signal.notify();

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
    /// This is the primitive `select!` builds on to wait across multiple
    /// channels with a single `Signal`. It exposes step 1 of the crate's
    /// register -> fence -> recheck -> park loss-free wakeup protocol (see
    /// the `crate::waitlist` module docs) without the other three steps:
    /// the caller owns the rest of the contract -- after registering on
    /// every channel it cares about, it MUST call
    /// `crate::waitlist::wakeup_fence` (or an equivalent `SeqCst` fence)
    /// and then recheck every channel's condition *before*
    /// parking/awaiting on `signal`. Skipping the fence or the recheck
    /// reopens the exact store-buffering race the protocol exists to
    /// close, and a real wakeup can be silently lost.
    ///
    /// `signal` must be a fresh registration, not a `Notifier` that was
    /// already notified (for example, reused across loop iterations
    /// without resetting): an already-notified entry is indistinguishable
    /// from a stale one, and `notify_one` will pop and discard it unread,
    /// silently starving this registration forever.
    pub fn register_signal(&self, signal: Arc<dyn Notifier>) {
        debug_assert!(
            !signal.is_notified(),
            "register_signal: registering an already-notified Notifier; it will be treated as \
             a stale entry and skipped by notify_one, silently losing this registration's wakeup"
        );
        self.inner.receivers.register(signal);
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
                wakeup_fence();
                self.inner.senders.notify_one();

                Some(msg)
            }
            None => None,
        }
    }

    /// Returns `true` if all senders have been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.inner.disconnected.load(Ordering::Acquire)
    }

    /// Receives a message from the channel, blocking if empty.
    ///
    /// Returns `None` when the channel is empty **and** all senders have been dropped.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv(&self) -> Option<T> {
        if let Some(msg) = self.try_recv() {
            return Some(msg);
        }

        if self.is_disconnected() {
            return self.try_recv();
        }

        loop {
            // Register unconditionally, then recheck -- the loss-free
            // wakeup protocol documented in `crate::waitlist`.
            let signal = Arc::new(Signal::new());
            self.inner.receivers.register(signal.clone());
            wakeup_fence();

            if let Some(msg) = self.try_recv() {
                // We registered but found a message already: not going to
                // wait, so mark our own entry stale (see `waitlist` docs).
                signal.notify();
                return Some(msg);
            }

            if self.is_disconnected() {
                signal.notify();
                return self.try_recv();
            }

            signal.wait();

            if let Some(msg) = self.try_recv() {
                return Some(msg);
            }

            // Drain once more rather than trusting the try_recv
            // immediately above: that try_recv and this is_disconnected
            // read are independent atomics with no ordering link forcing
            // them to agree, so observing disconnected here does not by
            // itself prove that same-thread, immediately-prior try_recv
            // already saw every message a sender published before
            // disconnecting -- see `unbounded::Receiver::recv`'s final
            // check for the full argument (found by shuttle once the
            // blocking path's own atomics became checker scheduling
            // points).
            if self.is_disconnected() {
                return self.try_recv();
            }
        }
    }

    /// Receives a message from the channel, blocking until either a
    /// message arrives or `deadline` passes.
    ///
    /// Mirrors [`recv`](Self::recv)'s register -> fence -> recheck -> park
    /// loop (see `crate::waitlist` for the loss-free wakeup contract),
    /// swapping the unbounded park for [`Signal::wait_deadline`] so a
    /// caller never blocks past its budget. `Timeout` and `Disconnected`
    /// are kept distinct (see [`RecvDeadline`]) because a caller bounding
    /// an internal wait wants to treat them differently: a timeout is
    /// worth retrying or escalating, a disconnect never will be.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_deadline(&self, deadline: Instant) -> RecvDeadline<T> {
        if let Some(msg) = self.try_recv() {
            return RecvDeadline::Msg(msg);
        }

        if self.is_disconnected() {
            return match self.try_recv() {
                Some(msg) => RecvDeadline::Msg(msg),
                None => RecvDeadline::Disconnected,
            };
        }

        loop {
            // Register unconditionally, then recheck -- the loss-free
            // wakeup protocol documented in `crate::waitlist`.
            let signal = Arc::new(Signal::new());
            self.inner.receivers.register(signal.clone());
            wakeup_fence();

            if let Some(msg) = self.try_recv() {
                // We registered but found a message already: not going to
                // wait, so mark our own entry stale (see `waitlist` docs).
                signal.notify();
                return RecvDeadline::Msg(msg);
            }

            if self.is_disconnected() {
                signal.notify();
                return match self.try_recv() {
                    Some(msg) => RecvDeadline::Msg(msg),
                    None => RecvDeadline::Disconnected,
                };
            }

            if !signal.wait_deadline(deadline) {
                // Timed out: mark our own entry stale (see `waitlist`
                // docs) and do one last recheck -- a message or disconnect
                // may have landed in the instant the deadline passed.
                signal.notify();
                if let Some(msg) = self.try_recv() {
                    return RecvDeadline::Msg(msg);
                }
                if !self.is_disconnected() {
                    return RecvDeadline::Timeout;
                }
                // Disconnected: drain once more rather than trusting the
                // try_recv just above (see `unbounded::Receiver::recv`'s
                // final check for why that try_recv and this
                // is_disconnected read can legitimately disagree).
                return match self.try_recv() {
                    Some(msg) => RecvDeadline::Msg(msg),
                    None => RecvDeadline::Disconnected,
                };
            }

            // Woken -- either a message arrived or senders disconnected.
            if let Some(msg) = self.try_recv() {
                return RecvDeadline::Msg(msg);
            }

            if self.is_disconnected() {
                return match self.try_recv() {
                    Some(msg) => RecvDeadline::Msg(msg),
                    None => RecvDeadline::Disconnected,
                };
            }

            // Spurious wake with nothing to show: loop back and
            // re-register. `wait_deadline` re-derives "has `deadline`
            // passed" from the clock on every call, so this cannot
            // overrun its budget no matter how many spurious iterations
            // happen first.
        }
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

                // Disconnected and empty: done (drain any remaining buffered messages).
                if this.receiver.is_disconnected() {
                    return Poll::Ready(this.receiver.try_recv());
                }

                if this.signal.is_notified() {
                    this.signal = Arc::new(AsyncSignal::new());
                }
                this.signal.register(cx.waker());
                this.receiver.inner.receivers.register(this.signal.clone());
                wakeup_fence();

                // Re-check: a send or disconnect may have landed between the first
                // check and signal registration.
                if let Some(msg) = this.receiver.try_recv() {
                    this.signal.notify();
                    return Poll::Ready(Some(msg));
                }

                if this.receiver.is_disconnected() {
                    this.signal.notify();
                    // Drain once more rather than trusting the try_recv
                    // just above (see `unbounded::Receiver::recv`'s final
                    // check for why that try_recv and this
                    // is_disconnected read can legitimately disagree).
                    return Poll::Ready(this.receiver.try_recv());
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
    assert!(
        cap > 0,
        "bounded channel capacity must be greater than zero"
    );
    let inner = Arc::new(Channel::new(cap));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}
