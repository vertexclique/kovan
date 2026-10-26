#[cfg(not(target_arch = "wasm32"))]
use crate::flavors::RecvDeadline;
use crate::flavors::clock::Clock;
use crate::flavors::unbounded;
use crate::signal::Notifier;
#[cfg(not(target_arch = "wasm32"))]
use crate::signal::Signal;
use crate::waitlist::{AsyncWait, WaitList, wakeup_fence};
#[cfg(not(target_arch = "wasm32"))]
use crossbeam_utils::Backoff;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

// Shuttle-instrumented so `len`/`sender_count`/`disconnected` -- raced by
// a blocking `send`/`recv`'s register/recheck against the other side's
// publish/notify, see `crate::waitlist` -- are checker scheduling points,
// matching `signal.rs`'s `Signal` swap.
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

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
    /// Number of live Receiver handles.
    receiver_count: AtomicUsize,
    /// Set when every Receiver is dropped: no value sent can be taken, so a
    /// send no longer waits for room.
    receivers_gone: AtomicBool,
    /// The clock of a channel of `after` or `tick`, which has no sender.
    clock: Option<Timer<T>>,
    /// The state of a channel of capacity 0 (`None` for every other).
    rendezvous: Option<Box<Rendezvous>>,
}

/// A channel of capacity 0, a rendezvous: it holds one slot, and a send whose
/// value went in waits until a receiver has taken it. One send at a time has
/// its value in the slot (the slot's reservation lets in one), so values go
/// in, and are taken, in the order of their tickets.
struct Rendezvous {
    /// The values sent: the next send's ticket.
    sent: AtomicU64,
    /// The values taken: a send whose ticket is below it is done.
    taken: AtomicU64,
    /// Sends waiting for their value to be taken.
    acks: WaitList,
}

impl Rendezvous {
    fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            taken: AtomicU64::new(0),
            acks: WaitList::new(),
        }
    }

    /// Whether the value of `ticket` was taken.
    fn is_taken(&self, ticket: u64) -> bool {
        self.taken.load(Ordering::Acquire) > ticket
    }
}

/// A channel's clock: its messages are read from the clock (see
/// `flavors::clock`), and made into a `T` (an `Instant`) by the function
/// beside it.
type Timer<T> = (Clock, fn(Instant) -> T);

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
        self.inner.receiver_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.inner.receiver_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.receivers_gone.store(true, Ordering::Release);
            // Loss-free wakeup: pairs with every sender's register-then-
            // recheck. See `crate::waitlist` module docs.
            wakeup_fence();
            self.inner.senders.notify_all();
            if let Some(r) = &self.inner.rendezvous {
                r.acks.notify_all();
            }
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
            receiver_count: AtomicUsize::new(1),
            receivers_gone: AtomicBool::new(false),
            clock: None,
            rendezvous: None,
        }
    }
}

/// The receiver of a channel whose messages come from `clock`, for `after`
/// and `tick`. It has no sender and never disconnects (`Channel::new`
/// counts one sender, and none is ever dropped), and holds no thread: the
/// receiving thread reads the clock.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))] // no `after` or `tick` there
pub(crate) fn clock_receiver(clock: Clock) -> Receiver<Instant> {
    let mut channel = Channel::new(1);
    channel.clock = Some((clock, |at| at));
    Receiver {
        inner: Arc::new(channel),
    }
}

impl<T: 'static> Sender<T> {
    /// Sends a message into the channel, blocking if full.
    ///
    /// Once every receiver is gone the value is dropped and `send` returns at
    /// once: no receiver can ever take it ([`is_disconnected`](Self::is_disconnected)).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn send(&self, t: T) {
        let backoff = Backoff::new();
        loop {
            if self.is_disconnected() {
                return;
            }
            let len = self.inner.len.load(Ordering::Relaxed);
            if len >= self.inner.capacity {
                // Register unconditionally, then recheck -- the loss-free
                // wakeup protocol documented in `crate::waitlist`.
                let signal = Arc::new(Signal::new());
                let slot = self.inner.senders.register(signal.clone());
                wakeup_fence();

                if self.inner.len.load(Ordering::Acquire) < self.inner.capacity
                    || self.is_disconnected()
                {
                    // Capacity freed up (or every receiver went) between our
                    // first check and registering: we won't actually wait,
                    // so abandon our own entry (see `waitlist` docs) and retry.
                    self.inner.senders.abandon(slot, &*signal);
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
                // A rendezvous: the slot let this send in alone, so its
                // ticket is its value's place in the order of the values.
                let ticket = self.ticket();
                // Success, send the message
                self.inner.sender.send(t);

                // Notify one receiver
                wakeup_fence();
                self.inner.receivers.notify_one();

                if let (Some(r), Some(ticket)) = (&self.inner.rendezvous, ticket) {
                    self.wait_taken(r, ticket);
                }
                return;
            }
            // Failed to reserve, retry
            backoff.snooze();
        }
    }
    /// A rendezvous send's ticket, once its slot is reserved.
    fn ticket(&self) -> Option<u64> {
        self.inner
            .rendezvous
            .as_ref()
            .map(|r| r.sent.fetch_add(1, Ordering::Relaxed))
    }

    /// A rendezvous send's wait, its value in the channel, until a receiver
    /// has taken it or every receiver is gone.
    #[cfg(not(target_arch = "wasm32"))]
    fn wait_taken(&self, r: &Rendezvous, ticket: u64) {
        while !r.is_taken(ticket) && !self.is_disconnected() {
            // Register unconditionally, then recheck -- the loss-free
            // wakeup protocol documented in `crate::waitlist`.
            let signal = Arc::new(Signal::new());
            let slot = r.acks.register(signal.clone());
            wakeup_fence();
            if r.is_taken(ticket) || self.is_disconnected() {
                r.acks.abandon(slot, &*signal);
                return;
            }
            signal.wait();
        }
    }

    /// Returns `true` if every receiver has been dropped: a send then drops
    /// its value and returns at once.
    pub fn is_disconnected(&self) -> bool {
        self.inner.receivers_gone.load(Ordering::Acquire)
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
        self.inner.senders.register(signal);
    }

    /// Withdraws `signal` from this channel's wait list: a caller of
    /// [`register_signal`](Self::register_signal) does this when it stops
    /// waiting, woken or not, so its registration holds nothing once the
    /// wait is over.
    pub fn unregister_signal(&self, signal: &dyn Notifier) {
        self.inner.senders.unregister(signal);
    }

    /// Wakes one sender waiting on this channel, if any: how a caller of
    /// [`register_signal`](Self::register_signal) passes on a wakeup its
    /// signal took and did not use.
    pub fn wake_one_waiter(&self) {
        self.inner.senders.notify_one();
    }

    /// Sends a message into the channel asynchronously.
    pub async fn send_async(&self, t: T) {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct SendFuture<'a, T: 'static> {
            sender: &'a Sender<T>,
            t: Option<T>,
            wait: AsyncWait,
            /// A rendezvous send's ticket, once its value is in: the send
            /// then waits on the channel's acks, not for room.
            ticket: Option<u64>,
        }

        impl<'a, T: 'static> Unpin for SendFuture<'a, T> {}

        impl<'a, T: 'static> SendFuture<'a, T> {
            /// Reserves a slot and sends, when there is room.
            fn try_send(&mut self) -> bool {
                let inner = &self.sender.inner;
                let len = inner.len.load(Ordering::Acquire);
                if len < inner.capacity
                    && inner
                        .len
                        .compare_exchange(len, len + 1, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                {
                    self.ticket = self.sender.ticket();
                    if let Some(t) = self.t.take() {
                        inner.sender.send(t);
                    }
                    // Notify one receiver
                    wakeup_fence();
                    inner.receivers.notify_one();
                    true
                } else {
                    false
                }
            }
        }

        impl<'a, T: 'static> SendFuture<'a, T> {
            /// A rendezvous send, its value in: Ready once a receiver has
            /// taken it, or every receiver is gone.
            fn poll_taken(&mut self, ticket: u64, cx: &mut Context<'_>) -> Poll<()> {
                let Some(r) = self.sender.inner.rendezvous.as_deref() else {
                    return Poll::Ready(());
                };
                self.wait.begin_poll();
                if r.is_taken(ticket) || self.sender.is_disconnected() {
                    self.wait.finish(&r.acks);
                    return Poll::Ready(());
                }
                self.wait.register(&r.acks, cx.waker());
                wakeup_fence();
                if r.is_taken(ticket) || self.sender.is_disconnected() {
                    self.wait.finish(&r.acks);
                    return Poll::Ready(());
                }
                Poll::Pending
            }
        }

        impl<'a, T: 'static> Future for SendFuture<'a, T> {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                if let Some(ticket) = this.ticket {
                    return this.poll_taken(ticket, cx);
                }
                this.wait.begin_poll();
                // Every receiver gone: the value is dropped, as `send` does.
                if this.sender.is_disconnected() || this.try_send() {
                    this.t = None;
                    this.wait.finish(&this.sender.inner.senders);
                    return match this.ticket {
                        Some(ticket) => this.poll_taken(ticket, cx),
                        None => Poll::Ready(()),
                    };
                }

                this.wait.register(&this.sender.inner.senders, cx.waker());
                wakeup_fence();

                // Re-check: capacity may have freed up, or every receiver
                // gone, since the fast-path attempt above.
                if this.sender.is_disconnected() || this.try_send() {
                    this.t = None;
                    this.wait.finish(&this.sender.inner.senders);
                    return match this.ticket {
                        Some(ticket) => this.poll_taken(ticket, cx),
                        None => Poll::Ready(()),
                    };
                }

                Poll::Pending
            }
        }

        // Dropped while pending: its entry must not take a wakeup meant for
        // a sender that waits. (A rendezvous send dropped after its value
        // went in leaves the value for a receiver to take.)
        impl<'a, T: 'static> Drop for SendFuture<'a, T> {
            fn drop(&mut self) {
                match (self.ticket, self.sender.inner.rendezvous.as_deref()) {
                    (Some(_), Some(r)) => self.wait.finish(&r.acks),
                    _ => self.wait.finish(&self.sender.inner.senders),
                }
            }
        }

        SendFuture {
            sender: self,
            t: Some(t),
            wait: AsyncWait::new(),
            ticket: None,
        }
        .await
    }
}

impl<T: 'static> Receiver<T> {
    /// Returns true if the channel is empty.
    pub fn is_empty(&self) -> bool {
        match &self.inner.clock {
            Some((clock, _)) => !clock.is_due(),
            // This is approximate
            None => self.inner.receiver.is_empty(),
        }
    }

    /// When this channel's next message is due by the clock: for a channel
    /// of [`after`](crate::after) or [`tick`](crate::tick), the time it
    /// delivers next (`None` once an `after` delivered); `None` for every
    /// other channel. `select!` waits until the earliest of these.
    pub fn next_due(&self) -> Option<Instant> {
        self.inner.clock.as_ref().and_then(|(clock, _)| clock.due())
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
    ///
    /// A channel of `after` or `tick` has no sender to notify a signal, so
    /// none is registered on it (nothing would ever take it off): its next
    /// message is found by [`next_due`](Self::next_due) instead.
    pub fn register_signal(&self, signal: Arc<dyn Notifier>) {
        if self.inner.clock.is_none() {
            self.inner.receivers.register(signal);
        }
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
    /// Attempts to receive a message from the channel without blocking.
    pub fn try_recv(&self) -> Option<T> {
        // Try to receive from the underlying queue
        // Note: unbounded::Receiver::try_recv returns None if empty
        match self.inner.receiver.try_recv() {
            Some(msg) => {
                // Decrement length
                self.inner.len.fetch_sub(1, Ordering::Release);
                // A rendezvous: the send of this value is done.
                if let Some(r) = &self.inner.rendezvous {
                    r.taken.fetch_add(1, Ordering::Release);
                }

                // Notify one sender
                wakeup_fence();
                if let Some(r) = &self.inner.rendezvous {
                    r.acks.notify_one();
                }
                self.inner.senders.notify_one();

                Some(msg)
            }
            None => {
                let (clock, make) = self.inner.clock.as_ref()?;
                clock.take().map(make)
            }
        }
    }

    /// Returns `true` if all senders have been dropped (for a channel of
    /// [`after`](crate::after), once its message came due).
    pub fn is_disconnected(&self) -> bool {
        match &self.inner.clock {
            Some((clock, _)) => clock.is_disconnected(),
            None => self.inner.disconnected.load(Ordering::Acquire),
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

        if let Some((clock, make)) = &self.inner.clock {
            return clock.wait(None).map(make);
        }

        if self.is_disconnected() {
            return self.try_recv();
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

            if self.is_disconnected() {
                self.inner.receivers.abandon(slot, &*signal);
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

        if let Some((clock, make)) = &self.inner.clock {
            // Disconnected: an `after` whose message came due since the
            // wait's last look, so it is taken once more first.
            return match clock.wait(Some(deadline)) {
                Some(at) => RecvDeadline::Msg(make(at)),
                None if clock.is_disconnected() => clock
                    .take()
                    .map_or(RecvDeadline::Disconnected, |at| RecvDeadline::Msg(make(at))),
                None => RecvDeadline::Timeout,
            };
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
            wait: AsyncWait,
            /// The due time a clock channel's wait last handed the runtime's
            /// timer, so a poll before it schedules nothing more.
            scheduled: Option<Instant>,
        }

        impl<'a, T: 'static> Unpin for RecvFuture<'a, T> {}

        impl<'a, T: 'static> Future for RecvFuture<'a, T> {
            type Output = Option<T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                if let Some((clock, make)) = &this.receiver.inner.clock {
                    // No channel wakes a task when a clock's message is due:
                    // the runtime's timer does (`set_timer`), or, with none,
                    // the task asks to be polled again until it is. One that
                    // never comes due waits for good, as `recv` does.
                    return match clock.take() {
                        Some(at) => Poll::Ready(Some(make(at))),
                        // Came due since the take: taken once more first.
                        None if clock.is_disconnected() => Poll::Ready(clock.take().map(make)),
                        None => match (clock.due(), crate::flavors::clock::timer()) {
                            (None, _) => Poll::Pending,
                            (Some(due), Some(timer)) => {
                                if this.scheduled != Some(due) {
                                    this.scheduled = Some(due);
                                    timer(due, cx.waker().clone());
                                }
                                Poll::Pending
                            }
                            (Some(_), None) => {
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                        },
                    };
                }
                let list = &this.receiver.inner.receivers;
                this.wait.begin_poll();
                if let Some(msg) = this.receiver.try_recv() {
                    this.wait.finish(list);
                    return Poll::Ready(Some(msg));
                }

                // Disconnected and empty: done (drain any remaining buffered messages).
                if this.receiver.is_disconnected() {
                    this.wait.finish(list);
                    return Poll::Ready(this.receiver.try_recv());
                }

                this.wait.register(list, cx.waker());
                wakeup_fence();

                // Re-check: a send or disconnect may have landed between the first
                // check and signal registration.
                if let Some(msg) = this.receiver.try_recv() {
                    this.wait.finish(list);
                    return Poll::Ready(Some(msg));
                }

                if this.receiver.is_disconnected() {
                    this.wait.finish(list);
                    // Drain once more rather than trusting the try_recv
                    // just above (see `unbounded::Receiver::recv`'s final
                    // check for why that try_recv and this
                    // is_disconnected read can legitimately disagree).
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
            scheduled: None,
        }
        .await
    }
}

/// Creates a channel of bounded capacity.
///
/// A capacity of 0 makes a rendezvous channel: a send waits until a receiver
/// has taken its value (or every receiver is gone), and `try_recv` finds a
/// value only while a send waits with it.
pub fn channel<T: 'static>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let mut channel = Channel::new(cap.max(1));
    if cap == 0 {
        channel.rendezvous = Some(Box::new(Rendezvous::new()));
    }
    let inner = Arc::new(channel);
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod clock_tests {
    use std::sync::Arc;
    use std::time::Duration;

    /// Nothing but its receivers holds a channel of `after` or `tick`: no
    /// thread stands behind it, and the last receiver's drop frees it. Each
    /// spawned a thread that held the channel, and a tick's never stopped.
    #[test]
    fn a_clock_channel_is_held_only_by_its_receivers() {
        for r in [
            crate::after(Duration::from_millis(1)),
            crate::tick(Duration::from_millis(1)),
            crate::after(Duration::MAX),
            crate::tick(Duration::MAX),
        ] {
            assert_eq!(Arc::strong_count(&r.inner), 1);
            let r2 = r.clone();
            assert_eq!(Arc::strong_count(&r.inner), 2);
            drop(r2);
            assert_eq!(Arc::strong_count(&r.inner), 1);
        }
    }
}
