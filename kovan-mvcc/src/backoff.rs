/// Action to take after a backoff attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffAction {
    /// Retry the operation
    Retry,
    /// Yield the current thread and retry
    Yield,
    /// Abort the operation
    Abort,
}

/// Pluggable backoff strategy for lock conflicts during reads
pub trait BackoffStrategy: Send + Sync {
    /// Determine the action to take for a given attempt number (0-indexed)
    fn backoff(&self, attempt: u32) -> BackoffAction;
}

/// Default backoff: retry with yield for first 3 attempts, then short sleep, then abort at 8.
pub struct DefaultBackoff;

impl BackoffStrategy for DefaultBackoff {
    fn backoff(&self, attempt: u32) -> BackoffAction {
        match attempt {
            0..3 => BackoffAction::Yield,
            3..8 => {
                std::thread::sleep(std::time::Duration::from_micros(100 << (attempt - 3)));
                BackoffAction::Retry
            }
            _ => BackoffAction::Abort,
        }
    }
}
