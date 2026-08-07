/// Waits on multiple concurrent branches.
///
/// # Examples
///
/// With a `default` case, so the example is safe to compile on every
/// target -- including `wasm32-*`, where the blocking arm below (no
/// `default`, parking on a shared [`crate::signal::Signal`]) is gated out.
/// See [`crate::signal::Signal`] for a blocking `select!` example.
///
/// ```
/// use kovan_channel::{unbounded, select};
///
/// let (s1, r1) = unbounded::<i32>();
///
/// select! {
///     v1 = r1 => panic!("Should not receive");
///     default => println!("No message available"),
/// }
/// ```
#[macro_export]
macro_rules! select {
    // Case with default
    (
        $($name:pat = $rx:expr => $body:expr),* ;
        default => $default_body:expr $(,)?
    ) => {
        {
            loop {
                // 1. Try all
                $(
                    if let Some($name) = $rx.try_recv() {
                        #[allow(unreachable_code)]
                        break $body;
                    }
                )*

                // 2. Default
                break $default_body;
            }
        }
    };
    // Case without default
    (
        $($name:pat = $rx:expr => $body:expr),* $(,)?
    ) => {
        {
            use std::sync::Arc;
            use std::sync::atomic::{Ordering, fence};
            use $crate::signal::Signal;

            loop {
                // 1. Try all
                $(
                    if let Some($name) = $rx.try_recv() {
                        #[allow(unreachable_code)]
                        break $body;
                    }
                )*

                // 2. Register all. A fresh `Signal` every iteration:
                // `Signal` has no reset, so reusing one across loop passes
                // would leave `state` stuck at "notified" after the first
                // wakeup -- every later `wait()` on it then returns
                // immediately (hot-spinning step 1..4 instead of actually
                // parking) and every later registration is a stale entry
                // piling up, unbounded, in each channel's `WaitList`.
                let signal = Arc::new(Signal::new());
                $(
                    $rx.register_signal(signal.clone());
                )*

                // Loss-free wakeup: pairs registration with the recheck
                // below (a sender that publishes after we start step 1
                // must still see this registration). See the
                // `crate::waitlist` module docs for the full proof.
                fence(Ordering::SeqCst);

                // 3. Re-check all
                let mut ready = false;
                $(
                    if !$rx.is_empty() {
                        ready = true;
                    }
                )*

                if ready {
                    continue;
                }

                // 4. Wait
                signal.wait();
            }
        }
    };
}
