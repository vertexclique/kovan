#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! High-performance STM using Kovan for memory reclamation.
//!
//! # Architecture
//!
//! This STM uses a TL2-style (Transactional Locking II) algorithm with a global
//! version clock.
//!
//! - **Reads**: Optimistic. No locks are acquired. Validity is checked via versioning.
//!   Memory safety is guaranteed by Kovan's safe memory reclamation.
//! - **Writes**: Buffered locally. Locks are acquired only during the commit phase.
//! - **Reclamation**: Old data versions are passed to Kovan's `retire` mechanism.

extern crate alloc;

mod errors;
mod node;
mod transaction;
mod var;

pub use errors::StmError;
pub use transaction::Transaction;
pub use var::TVar;

use kovan::pin;
use std::sync::atomic::AtomicU64;

/// The STM engine containing global state (like the version clock).
pub struct Stm {
    /// Global version clock.
    /// Even numbers represent stable timestamps.
    /// Odd numbers are not used for global clock, but typically indicate locked states in vars.
    global_clock: AtomicU64,
}

impl Stm {
    /// Create a new STM engine instance.
    pub const fn new() -> Self {
        Self {
            global_clock: AtomicU64::new(0),
        }
    }
}

impl Default for Stm {
    fn default() -> Self {
        Self::new()
    }
}

impl Stm {
    /// Execute a closure atomically.
    ///
    /// This function handles transaction retries automatically.
    ///
    /// DANGER AHEAD: Don't use IO bound operations inside the closure. It will block the entire STM.
    /// They might be retried multiple times, which is not what you may want for IO bound operations.
    ///
    /// # Example
    ///
    /// ```
    /// use kovan_stm::Stm;
    /// let stm = Stm::new();
    /// let var = stm.tvar(10);
    ///
    /// stm.atomically(|tx| {
    ///     let val = tx.load(&var)?;
    ///     tx.store(&var, val + 1)?;
    ///     Ok(())
    /// });
    /// ```
    pub fn atomically<F, T>(&self, mut f: F) -> T
    where
        F: FnMut(&mut Transaction) -> Result<T, StmError>,
    {
        loop {
            // 1. Enter Kovan critical section
            // This ensures any pointer we read from Kovan Atomics remains valid
            // throughout the transaction attempt.
            let guard = pin();

            // 2. Create transaction context
            let mut tx = Transaction::new(self, &guard);

            // 3. Run user logic
            match f(&mut tx) {
                Ok(result) => {
                    // 4. Attempt to commit
                    if tx.commit() {
                        return result;
                    }
                    // Commit failed (conflict), retry loop
                }
                Err(e) => {
                    // User explicitly aborted or raised error
                    if let StmError::Retry = e {
                        // In a real scheduler, we might backoff/park here
                        std::thread::yield_now();
                        continue;
                    }
                    panic!("Transaction failed with error: {:?}", e);
                }
            }
        }
    }

    /// Create a new transactional variable managed by this STM.
    pub fn tvar<T: Send + Sync + 'static>(&self, data: T) -> TVar<T> {
        TVar::new(data)
    }
}

/// Global singleton for easier usage if preferred, though explicitly passing Stm is cleaner.
pub static STM: Stm = Stm::new();

/// Convenience helper using the global STM instance.
pub fn atomically<F, T>(f: F) -> T
where
    F: FnMut(&mut Transaction) -> Result<T, StmError>,
{
    STM.atomically(f)
}
