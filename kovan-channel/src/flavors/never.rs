use crate::flavors::unbounded;

/// Creates a channel that never delivers a message.
///
/// The channel has no sender, and its count of senders stays at one, so it
/// never disconnects: a receive waits for good. It is freed once its last
/// receiver drops.
pub fn never<T: 'static>() -> unbounded::Receiver<T> {
    unbounded::receiver_without_sender()
}
