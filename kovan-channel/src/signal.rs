use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;
use std::thread::{self, Thread};

/// A mechanism for thread synchronization and notification.
/// A trait for notification mechanisms (thread or async task).
pub trait Notifier: Send + Sync {
    /// Notifies the waiter.
    fn notify(&self);
}

/// A signal for blocking thread synchronization.
pub struct Signal {
    state: AtomicUsize,
    thread: Thread,
}

impl Notifier for Signal {
    fn notify(&self) {
        self.state.store(1, Ordering::Release);
        self.thread.unpark();
    }
}

impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}

impl Signal {
    /// Creates a new signal for the current thread.
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            thread: thread::current(),
        }
    }

    /// Waits for the signal to be notified.
    pub fn wait(&self) {
        while self.state.load(Ordering::Acquire) == 0 {
            thread::park();
        }
    }

    /// Notifies the signal, waking up the waiting thread.
    pub fn notify(&self) {
        Notifier::notify(self);
    }

    /// Returns true if the signal has been notified.
    pub fn is_notified(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }
}

/// A signal for async tasks.
pub struct AsyncSignal {
    state: AtomicUsize,
    waker: Mutex<Option<Waker>>,
}

impl Default for AsyncSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncSignal {
    /// Creates a new async signal.
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            waker: Mutex::new(None),
        }
    }

    /// Registers a waker for notification.
    pub fn register(&self, waker: &Waker) {
        let mut guard = self.waker.lock().unwrap();
        *guard = Some(waker.clone());
    }

    /// Returns true if the signal has been notified.
    pub fn is_notified(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }
}

impl Notifier for AsyncSignal {
    fn notify(&self) {
        self.state.store(1, Ordering::Release);
        let guard = self.waker.lock().unwrap();
        if let Some(waker) = &*guard {
            waker.wake_by_ref();
        }
    }
}

/// A token used for identification.
pub struct Token(pub usize);
