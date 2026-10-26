use kovan::{Atomic, RetiredNode, Shared, alloc_node, pin, retire_node};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

// Shuttle-instrumented so `sender_count`/`disconnected` -- raced by a
// blocking `recv`'s register/recheck against `Sender::drop`'s
// disconnect-then-wake, see `crate::waitlist` -- are checker scheduling
// points, matching `signal.rs`'s `Signal` swap.
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[cfg(not(target_arch = "wasm32"))]
use crate::flavors::RecvDeadline;
use crate::signal::Notifier;
#[cfg(not(target_arch = "wasm32"))]
use crate::signal::Signal;
use crate::waitlist::{AsyncWait, WaitList, wakeup_fence};

#[repr(C)]
pub(crate) struct Node<T> {
    retired: RetiredNode,
    data: Option<T>,
    next: Atomic<Node<T>>,
}

impl<T> Node<T> {
    #[inline]
    fn new(data: Option<T>) -> *mut Self {
        alloc_node(Self {
            retired: RetiredNode::new(),
            data,
            next: Atomic::null(),
        })
    }
}

pub(crate) struct Channel<T: 'static> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
    receivers: WaitList,
    /// Number of live Sender handles. When this reaches 0, the channel is disconnected.
    sender_count: AtomicUsize,
    /// Set to true when all senders have been dropped.
    disconnected: AtomicBool,
    /// Number of live Receiver handles.
    receiver_count: AtomicUsize,
    /// Set when every Receiver is dropped: nothing sent can be received, so a
    /// send drops its value.
    receivers_gone: AtomicBool,
}

impl<T: 'static> Channel<T> {
    pub(crate) fn new() -> Self {
        let sentinel = Node::new(None);
        Self {
            head: Atomic::new(sentinel),
            tail: Atomic::new(sentinel),
            receivers: WaitList::new(),
            sender_count: AtomicUsize::new(1), // Starts at 1 for the initial Sender
            disconnected: AtomicBool::new(false),
            receiver_count: AtomicUsize::new(1),
            receivers_gone: AtomicBool::new(false),
        }
    }

    /// Wake all blocked receivers (used when senders disconnect).
    fn wake_all_receivers(&self) {
        // Loss-free wakeup: pairs with every receiver's
        // register-then-recheck. See `crate::waitlist` module docs.
        wakeup_fence();
        self.receivers.notify_all();
    }
}

impl<T: 'static> Drop for Channel<T> {
    fn drop(&mut self) {
        // At creation time `head == tail == sentinel` (a dummy `Node` with
        // `data: None`). As messages are dequeued, `try_recv()` advances
        // `head` and retires the old head, whose value it moved out; the new
        // head is never retired by `try_recv`. So at `Drop` time every node
        // from `head` on is linked and never retired: the sentinel or the
        // last node a dequeue moved the head to, and the nodes of the
        // messages still pending.
        //
        // The channel drops with its last handle, and every operation
        // borrows a handle, so no thread is inside one and none reaches
        // these nodes: each is dropped and freed here, the values still
        // inside on this thread (retiring them would run a value's
        // destructor on whichever thread kovan frees it on, which needs
        // `T: Send`).
        let guard = pin();
        let mut curr = self.head.load(Ordering::Relaxed, &guard);

        while !curr.is_null() {
            let next = unsafe { curr.deref().next.load(Ordering::Relaxed, &guard) };
            // SAFETY: linked, never retired, and unreachable by any other thread (see above).
            unsafe { kovan::drop_node(curr.as_raw()) };
            curr = next;
        }

        // Submit the old heads this thread retired, so they do not wait for
        // its next batch.
        drop(guard);
        kovan::flush_pending();
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
            // Last sender dropped - mark channel as disconnected and wake all receivers.
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
        self.inner.receiver_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.inner.receiver_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Nothing sent can be received any more: a send drops its value
            // from now on, and what is queued is dropped here, not kept until
            // the last sender goes. A send that read the flag before this
            // store may still link one value; the channel's drop drops it.
            self.inner.receivers_gone.store(true, Ordering::Release);
            while self.try_recv().is_some() {}
        }
    }
}

unsafe impl<T: 'static + Send> Send for Receiver<T> {}
unsafe impl<T: 'static + Send> Sync for Receiver<T> {}

/// A receiver whose channel has no sender and never disconnects, what
/// [`never`](crate::never) hands out: `Channel::new` starts the count of
/// senders at one, and with no `Sender` to drop it stays there. Only the
/// receivers hold the channel, so the last one's drop frees it.
pub(crate) fn receiver_without_sender<T: 'static>() -> Receiver<T> {
    Receiver {
        inner: Arc::new(Channel::new()),
    }
}

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
    ///
    /// Once every receiver is gone the value is dropped and `send` returns at
    /// once: no receiver can ever take it ([`is_disconnected`](Self::is_disconnected)).
    pub fn send(&self, t: T) {
        if self.inner.receivers_gone.load(Ordering::Acquire) {
            drop(t);
            return;
        }
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
                        wakeup_fence();
                        self.inner.receivers.notify_one();

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

impl<T: 'static> Sender<T> {
    /// Returns `true` if every receiver has been dropped: a send then drops
    /// its value and returns at once.
    pub fn is_disconnected(&self) -> bool {
        self.inner.receivers_gone.load(Ordering::Acquire)
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
                        unsafe { retire_node(head.as_raw()) };

                        // We take the data from `next`.
                        // `next` is now the sentinel.
                        // Its data is logically gone.
                        //
                        // CRITICAL: use `ptr::replace` (not `ptr::read`) to clear
                        // the source field. `ptr::read` leaves the bit pattern
                        // intact, so when this node is later retired and freed by
                        // kovan, `Node::drop` would drop `data: Option<T>` again
                        // - a double-free. Writing `None` ensures the destructor
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
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv(&self) -> Option<T> {
        if let Some(msg) = self.try_recv() {
            return Some(msg);
        }

        // Fast path: already disconnected and empty
        if self.is_disconnected() {
            return self.try_recv(); // Drain remaining
        }

        loop {
            // Register unconditionally, then recheck -- the loss-free
            // wakeup protocol documented in `crate::waitlist`.
            let signal = Arc::new(Signal::new());
            let slot = self.inner.receivers.register(signal.clone());
            wakeup_fence();

            if let Some(msg) = self.try_recv() {
                // We registered but found a message already: not going to
                // wait, so abandon our own entry (see `waitlist` docs).
                self.inner.receivers.abandon(slot, &*signal);
                return Some(msg);
            }

            // Check disconnection after registering signal but before parking
            if self.is_disconnected() {
                self.inner.receivers.abandon(slot, &*signal);
                return self.try_recv(); // Drain remaining
            }

            signal.wait();

            // Woken up - either a message arrived or senders disconnected
            if let Some(msg) = self.try_recv() {
                return Some(msg);
            }

            // Woken by disconnect with empty queue. Drain once more rather
            // than trusting the try_recv immediately above: that try_recv
            // and this is_disconnected read are independent atomics with
            // no ordering link forcing them to agree with each other, so
            // observing disconnected here does not by itself prove that
            // same-thread, immediately-prior try_recv already saw every
            // message a sender published before disconnecting -- the
            // exact independent-location gap `crate::waitlist`'s fence
            // exists to close for registration vs. the condition it
            // guards, reopened here because nothing fences try_recv
            // against is_disconnected. Found by shuttle once the blocking
            // path's own atomics became checker scheduling points.
            if self.is_disconnected() {
                return self.try_recv(); // Drain remaining
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
            let slot = self.inner.receivers.register(signal.clone());
            wakeup_fence();

            if let Some(msg) = self.try_recv() {
                // We registered but found a message already: not going to
                // wait, so abandon our own entry (see `waitlist` docs).
                self.inner.receivers.abandon(slot, &*signal);
                return RecvDeadline::Msg(msg);
            }

            if self.is_disconnected() {
                self.inner.receivers.abandon(slot, &*signal);
                return match self.try_recv() {
                    Some(msg) => RecvDeadline::Msg(msg),
                    None => RecvDeadline::Disconnected,
                };
            }

            if !signal.wait_deadline(deadline) {
                // Timed out: mark our own entry stale (see `waitlist`
                // docs) and do one last recheck -- a message or disconnect
                // may have landed in the instant the deadline passed.
                self.inner.receivers.abandon(slot, &*signal);
                if let Some(msg) = self.try_recv() {
                    return RecvDeadline::Msg(msg);
                }
                if !self.is_disconnected() {
                    return RecvDeadline::Timeout;
                }
                // Disconnected: drain once more rather than trusting the
                // try_recv just above (see `recv`'s final check for why
                // that try_recv and this is_disconnected read can
                // legitimately disagree).
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

    /// Returns true if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let guard = pin();
        let head = self.inner.head.load(Ordering::Relaxed, &guard);
        let tail = self.inner.tail.load(Ordering::Relaxed, &guard);
        let next = unsafe { head.deref().next.load(Ordering::Relaxed, &guard) };
        head == tail && next.is_null()
    }

    /// When this channel's next message is due by the clock: always `None`,
    /// as only the channels of [`after`](crate::after) and
    /// [`tick`](crate::tick) deliver by the clock. `select!` waits until
    /// the earliest due time of its channels.
    pub fn next_due(&self) -> Option<Instant> {
        None
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
    /// A signal another channel already notified may be registered (the
    /// one `Signal` of a `select!` is registered on its channels one after
    /// another): its entry is stale and skipped, and the caller's wait
    /// returns at once. A caller that stops waiting takes the signal back
    /// from every channel it registered on
    /// ([`unregister_signal`](Self::unregister_signal)); one that was not
    /// woken also withdraws the signal
    /// ([`Signal::cancel`](crate::signal::Signal::cancel)) and, when that
    /// answers `false`, passes the wakeup it took on with
    /// [`wake_one_waiter`](Self::wake_one_waiter) on every channel it
    /// registered on.
    pub fn register_signal(&self, signal: Arc<dyn Notifier>) {
        self.inner.receivers.register(signal);
    }

    /// Withdraws `signal` from this channel's wait list: a caller of
    /// [`register_signal`](Self::register_signal) does this when it stops
    /// waiting, woken or not, so its registration holds nothing once the
    /// wait is over.
    pub fn unregister_signal(&self, signal: &dyn Notifier) {
        self.inner.receivers.unregister(signal);
    }

    /// Wakes one receiver waiting on this channel, if any: how a caller of
    /// [`register_signal`](Self::register_signal) passes on a wakeup its
    /// signal took and did not use.
    pub fn wake_one_waiter(&self) {
        self.inner.receivers.notify_one();
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
            wait: AsyncWait,
        }

        impl<'a, T: 'static> Unpin for RecvFuture<'a, T> {}

        impl<'a, T: 'static> Future for RecvFuture<'a, T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                let list = &this.receiver.inner.receivers;
                this.wait.begin_poll();
                if let Some(msg) = this.receiver.try_recv() {
                    this.wait.finish(list);
                    return Poll::Ready(Some(msg));
                }

                // Disconnected and empty - done
                if this.receiver.is_disconnected() {
                    this.wait.finish(list);
                    return Poll::Ready(this.receiver.try_recv());
                }

                this.wait.register(list, cx.waker());
                wakeup_fence();

                // Re-check
                if let Some(msg) = this.receiver.try_recv() {
                    this.wait.finish(list);
                    return Poll::Ready(Some(msg));
                }

                if this.receiver.is_disconnected() {
                    this.wait.finish(list);
                    // Drain once more rather than trusting the try_recv
                    // just above (see `Receiver::recv`'s final check for
                    // why that try_recv and this is_disconnected read can
                    // legitimately disagree).
                    return Poll::Ready(this.receiver.try_recv());
                }

                Poll::Pending
            }
        }

        // Dropped while pending: its entry must not take a wakeup meant for
        // a receiver that waits.
        impl<'a, T: 'static> Drop for RecvFuture<'a, T> {
            fn drop(&mut self) {
                self.wait.finish(&self.receiver.inner.receivers);
            }
        }

        RecvFuture {
            receiver: self,
            wait: AsyncWait::new(),
        }
        .await
    }
}

#[cfg(test)]
mod never_tests {
    use std::sync::Arc;

    /// A channel that never delivers is held by its receivers alone, so the
    /// last receiver's drop frees it; it was leaked (the sender that
    /// `never` made was forgotten, holding the channel for good).
    #[test]
    fn a_never_channel_is_held_only_by_its_receivers() {
        let r = crate::never::<u32>();
        assert_eq!(Arc::strong_count(&r.inner), 1);
        let r2 = r.clone();
        assert_eq!(Arc::strong_count(&r.inner), 2);
        drop(r2);
        assert_eq!(Arc::strong_count(&r.inner), 1);
        assert_eq!(r.try_recv(), None);
    }
}
