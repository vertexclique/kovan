pub mod flavors;
pub mod select;
pub mod signal;

pub use flavors::bounded;
pub use flavors::unbounded;

/// Creates a channel of unbounded capacity.
///
/// This channel has a growable buffer that can hold any number of messages.
pub fn unbounded<T: 'static>() -> (unbounded::Sender<T>, unbounded::Receiver<T>) {
    unbounded::channel()
}

/// Creates a channel of bounded capacity.
///
/// This channel has a buffer of fixed capacity.
pub fn bounded<T: 'static>(cap: usize) -> (bounded::Sender<T>, bounded::Receiver<T>) {
    bounded::channel(cap)
}

pub use flavors::after::after;
pub use flavors::never::never;
pub use flavors::tick::tick;
