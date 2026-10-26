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
    /// # Panics
    ///
    /// If the closure returns an error other than [`StmError::Retry`] (a
    /// [`StmError::User`]): [`try_atomically`](Self::try_atomically) answers it
    /// instead.
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
    pub fn atomically<F, T>(&self, f: F) -> T
    where
        F: FnMut(&mut Transaction) -> Result<T, StmError>,
    {
        match self.try_atomically(f) {
            Ok(result) => result,
            Err(e) => panic!("Transaction failed with error: {:?}", e),
        }
    }

    /// Execute a closure atomically, answering the error it returns.
    ///
    /// As [`atomically`](Self::atomically): a conflict or an
    /// [`StmError::Retry`] runs the closure again; any other error (a
    /// [`StmError::User`]) rolls the transaction back (its rollback hooks run,
    /// nothing it stored is published) and is returned.
    ///
    /// # Example
    ///
    /// ```
    /// use kovan_stm::{Stm, StmError};
    /// let stm = Stm::new();
    /// let balance = stm.tvar(5);
    ///
    /// let withdrawn = stm.try_atomically(|tx| {
    ///     let b = tx.load(&balance)?;
    ///     if b < 10 {
    ///         return Err(StmError::User("insufficient funds".into()));
    ///     }
    ///     tx.store(&balance, b - 10)?;
    ///     Ok(b - 10)
    /// });
    /// assert!(matches!(withdrawn, Err(StmError::User(_))));
    /// assert_eq!(stm.atomically(|tx| tx.load(&balance)), 5);
    /// ```
    pub fn try_atomically<F, T>(&self, mut f: F) -> Result<T, StmError>
    where
        F: FnMut(&mut Transaction) -> Result<T, StmError>,
    {
        // Commits that failed in a row: each waits a little longer before the
        // next attempt, so transactions that keep meeting on a hot variable
        // stop aborting each other at full speed.
        #[cfg(not(feature = "shuttle"))]
        let mut conflicts = 0u32;
        loop {
            let committed = {
                // 1. Enter Kovan critical section
                // This ensures any pointer we read from Kovan Atomics remains valid
                // throughout the transaction attempt.
                let guard = pin();

                // 2. Create transaction context
                let mut tx = Transaction::new(self, &guard);

                // 3. Run user logic
                match f(&mut tx) {
                    // 4. Attempt to commit
                    Ok(result) => tx.commit().then_some(result),
                    // User explicitly asked to retry
                    Err(StmError::Retry) => {
                        drop(tx);
                        drop(guard);
                        // In a real scheduler, we might backoff/park here.
                        // Under shuttle this yield must be the scheduler's
                        // own (see the fairness note below); a real
                        // `std::thread::yield_now()` is a no-op for
                        // shuttle's cooperative scheduler (every other task
                        // is genuinely parked, not merely de-prioritized).
                        #[cfg(feature = "shuttle")]
                        shuttle::hint::spin_loop();
                        #[cfg(not(feature = "shuttle"))]
                        std::thread::yield_now();
                        continue;
                    }
                    // A user error: rolled back (the transaction's drop runs
                    // its rollback hooks), and answered.
                    Err(e) => return Err(e),
                }
            };
            if let Some(result) = committed {
                return Ok(result);
            }
            // Commit failed (conflict): retry, the guard already dropped.
            //
            // vertexia: under shuttle, an explicit yield here (not
            // just the instrumented `version_lock` reads/CASes the
            // next attempt performs) is required for the scheduler
            // to guarantee the *other* side of the conflict a turn.
            // Without it, PCT/random can keep re-selecting this
            // thread's retries indefinitely under an unlucky
            // priority draw -- both sides livelocking each other
            // out until shuttle's step budget is exhausted
            // ("exceeded max_steps bound", an unfair schedule, not
            // a real bug -- see `kovan-map`'s `resize_spin_hint` for
            // the identical failure mode and full reasoning).
            #[cfg(feature = "shuttle")]
            shuttle::hint::spin_loop();
            #[cfg(not(feature = "shuttle"))]
            back_off(&mut conflicts);
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

/// [`Stm::try_atomically`] on the global STM instance.
pub fn try_atomically<F, T>(f: F) -> Result<T, StmError>
where
    F: FnMut(&mut Transaction) -> Result<T, StmError>,
{
    STM.try_atomically(f)
}

/// The wait after the `conflicts`-th failed commit in a row: spins that
/// double up to 64, then a yield of the thread.
#[cfg(not(feature = "shuttle"))]
fn back_off(conflicts: &mut u32) {
    if *conflicts < 6 {
        for _ in 0..1u32 << *conflicts {
            core::hint::spin_loop();
        }
        *conflicts += 1;
    } else {
        std::thread::yield_now();
    }
}
