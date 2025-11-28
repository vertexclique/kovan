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
            use $crate::signal::Signal;

            let signal = Arc::new(Signal::new());
            loop {
                // 1. Try all
                $(
                    if let Some($name) = $rx.try_recv() {
                        #[allow(unreachable_code)]
                        break $body;
                    }
                )*

                // 2. Register all
                $(
                    $rx.register_signal(signal.clone());
                )*

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
