use crate::flavors::bounded;
use crate::flavors::clock::Clock;
use std::time::{Duration, Instant};

/// Creates a channel that delivers a message, the instant it was due,
/// every `duration`, the first `duration` from now. The next message is due
/// one `duration` after a receive takes one: ticks nobody received while
/// they were due are skipped, not queued.
///
/// No thread stands behind it: a receive reads the clock, and a blocking
/// one sleeps on the receiving thread until a message is due. A task
/// awaiting it (`recv_async`) sleeps on the runtime's timer until then, once
/// a runtime has installed one ([`set_timer`](crate::set_timer)); with none,
/// it asks to be polled again until then.
pub fn tick(duration: Duration) -> bounded::Receiver<Instant> {
    bounded::clock_receiver(Clock::tick(duration))
}
