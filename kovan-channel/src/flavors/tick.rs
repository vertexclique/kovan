use crate::flavors::bounded;
use std::thread;
use std::time::{Duration, Instant};

/// Creates a channel that delivers messages periodically.
pub fn tick(duration: Duration) -> bounded::Receiver<Instant> {
    let (s, r) = bounded::channel(1);
    thread::spawn(move || {
        loop {
            thread::sleep(duration);
            s.send(Instant::now());
        }
    });
    r
}
