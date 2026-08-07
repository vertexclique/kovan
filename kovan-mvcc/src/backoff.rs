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
///
/// # wasm32
///
/// `std::thread::sleep` is not uniformly available on wasm. `std`'s dispatch
/// (`sys/thread/mod.rs`) routes `target_os = "wasi"` to the unix implementation,
/// so it sleeps normally on `wasm32-wasip1`; but `wasm32-unknown-unknown`
/// without the `atomics` target-feature falls through to
/// `unsupported::sleep`, which is `panic!("can't sleep")`. Browser builds are
/// exactly that target, so the sleep is replaced there with a bounded spin of
/// equivalent escalation.
///
/// Spinning rather than sleeping is also the semantically honest choice on a
/// single-threaded runtime: the lock holder is not running while we wait, so
/// elapsed time cannot resolve the conflict. The spin only paces the retry.
///
/// The native path is deliberately left byte-identical -- the `cfg` wraps the
/// single sleep statement, nothing else.
pub struct DefaultBackoff;

impl BackoffStrategy for DefaultBackoff {
    fn backoff(&self, attempt: u32) -> BackoffAction {
        match attempt {
            0..3 => BackoffAction::Yield,
            3..8 => {
                #[cfg(not(target_arch = "wasm32"))]
                std::thread::sleep(std::time::Duration::from_micros(100 << (attempt - 3)));
                #[cfg(target_arch = "wasm32")]
                for _ in 0..(64_u32 << (attempt - 3)) {
                    core::hint::spin_loop();
                }
                BackoffAction::Retry
            }
            _ => BackoffAction::Abort,
        }
    }
}
