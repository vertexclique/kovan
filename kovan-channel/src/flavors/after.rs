use crate::flavors::bounded;
use std::thread;
use std::time::{Duration, Instant};

/// Creates a channel that delivers a message after a specified duration.
pub fn after(duration: Duration) -> bounded::Receiver<Instant> {
    let (s, r) = bounded::channel(1);
    thread::spawn(move || {
        thread::sleep(duration);
        s.send(Instant::now());
    });
    r
}
