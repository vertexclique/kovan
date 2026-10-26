use crate::flavors::bounded;
use crate::flavors::clock::Clock;
use std::time::{Duration, Instant};

/// Creates a channel that delivers one message, the instant it was due,
/// `duration` from now.
///
/// No thread stands behind it: a receive reads the clock, and a blocking
/// one sleeps on the receiving thread until the message is due. A task
/// awaiting it (`recv_async`) sleeps on the runtime's timer until then, once
/// a runtime has installed one ([`set_timer`](crate::set_timer)); with none,
/// it asks to be polled again until then.
pub fn after(duration: Duration) -> bounded::Receiver<Instant> {
    bounded::clock_receiver(Clock::after(duration))
}
