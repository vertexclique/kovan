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
///
/// Without a `default` arm, `select!` waits until an arm can run. Once every
/// channel is disconnected and empty none ever can: a `disconnected` arm then
/// runs, and without one `select!` panics rather than wait for good.
///
/// ```
/// use kovan_channel::{unbounded, select};
///
/// let (s1, r1) = unbounded::<i32>();
/// drop(s1);
///
/// let gone = select! {
///     v1 = r1 => v1 == 0;
///     disconnected => true,
/// };
/// assert!(gone);
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
    // Case with an arm for every channel disconnected and empty
    (
        $($name:pat = $rx:expr => $body:expr),* ;
        disconnected => $disconnected_body:expr $(,)?
    ) => {
        $crate::select!(@wait $($name = $rx => $body),* ; { $disconnected_body })
    };
    // Case without default: no arm can ever run once every channel is
    // disconnected and empty, so that is a panic, not a hang
    (
        $($name:pat = $rx:expr => $body:expr),* $(,)?
    ) => {
        $crate::select!(@wait $($name = $rx => $body),* ; {
            panic!("select!: every channel is disconnected and empty, so no arm can ever run")
        })
    };
    (@wait $($name:pat = $rx:expr => $body:expr),* ; $disconnected_body:block) => {
        {
            use std::sync::Arc;
            use std::sync::atomic::{Ordering, fence};
            use $crate::signal::Signal;

            // A wakeup this select! took, which may have been meant for a
            // waiter on another channel than the arm it takes: it passes one
            // on to every channel that still holds a value.
            let mut woken = false;
            let pass_on = || {
                $(
                    if !$rx.is_empty() {
                        $rx.wake_one_waiter();
                    }
                )*
            };

            loop {
                // 1. Try all
                $(
                    if let Some($name) = $rx.try_recv() {
                        if woken {
                            pass_on();
                        }
                        #[allow(unreachable_code)]
                        break $body;
                    }
                )*

                // Every channel disconnected and empty: no arm can run.
                // (A channel can disconnect after its try_recv above, so it
                // is drained once more first.)
                if true $(&& $rx.is_disconnected())* {
                    $(
                        if let Some($name) = $rx.try_recv() {
                            if woken {
                                pass_on();
                            }
                            #[allow(unreachable_code)]
                            break $body;
                        }
                    )*
                    // Without a `disconnected` arm this is a panic, which never
                    // breaks: the lint below is the macro's choice, not the caller's.
                    #[allow(unreachable_code, clippy::diverging_sub_expression)]
                    break $disconnected_body;
                }

                // 2. Register on every channel that can still deliver. A
                // fresh `Signal` every iteration: `Signal` has no reset. A
                // disconnected channel gets none: nothing is left to wake
                // its waiters but its disconnect, which already happened.
                // A wakeup taken by an earlier pass found nothing: none is
                // owed now.
                woken = false;
                let signal = Arc::new(Signal::new());
                let mut live = 0usize;
                $(
                    if !$rx.is_disconnected() {
                        $rx.register_signal(signal.clone());
                        live += 1;
                    }
                )*

                // Loss-free wakeup: pairs registration with the recheck
                // below (a sender that publishes after we start step 1
                // must still see this registration). See the
                // `crate::waitlist` module docs for the full proof.
                fence(Ordering::SeqCst);

                // 3. Re-check all: a value, or a channel that disconnected
                // since it was registered on, ends the wait.
                let mut ready = false;
                let mut live_now = 0usize;
                $(
                    if !$rx.is_empty() {
                        ready = true;
                    }
                    if !$rx.is_disconnected() {
                        live_now += 1;
                    }
                )*

                // No channel registered on: each disconnected after the
                // check above, and nothing is left to end the wait, so that
                // check runs again.
                if ready || live == 0 || live_now < live {
                    // Not going to wait: take the signal back, withdraw it,
                    // and pass on a wakeup a channel delivered to it first.
                    $(
                        $rx.unregister_signal(&*signal);
                    )*
                    if !signal.cancel() {
                        $(
                            $rx.wake_one_waiter();
                        )*
                    }
                    continue;
                }

                // 4. Wait: until a channel wakes the signal, or until the
                // earliest message a clock (`after`, `tick`) has due, which
                // no channel wakes it for.
                let mut due: Option<std::time::Instant> = None;
                $(
                    if let Some(at) = $rx.next_due() {
                        due = Some(due.map_or(at, |due| due.min(at)));
                    }
                )*
                match due {
                    Some(due) => {
                        signal.wait_deadline(due);
                    }
                    None => signal.wait(),
                }
                // Take the signal back from every channel. A cancel that
                // loses means a channel woke it: that wakeup is this pass's.
                $(
                    $rx.unregister_signal(&*signal);
                )*
                woken = !signal.cancel();
            }
        }
    };
}
