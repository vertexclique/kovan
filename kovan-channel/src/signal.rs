use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, Thread};

/// A mechanism for thread synchronization and notification.
pub struct Signal {
    state: AtomicUsize,
    thread: Thread,
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
        self.state.store(1, Ordering::Release);
        self.thread.unpark();
    }

    /// Returns true if the signal has been notified.
    pub fn is_notified(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }
}

/// A token used for identification.
pub struct Token(pub usize);
