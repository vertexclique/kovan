/// Waits on multiple concurrent branches.
///
/// # Examples
///
/// ```
/// use kovan_channel::{unbounded, select};
///
/// let (s1, r1) = unbounded::<i32>();
/// let (s2, r2) = unbounded::<i32>();
///
/// s1.send(10);
///
/// select! {
///     v1 = r1 => assert_eq!(v1, 10),
///     v2 = r2 => panic!("Should receive from r1"),
/// }
/// ```
///
/// With default case:
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
            use std::sync::Arc;
            use $crate::signal::Signal;

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
