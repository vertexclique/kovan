/// Channel that delivers a message after a duration.
pub mod after;
/// Bounded channel implementation.
pub mod bounded;
/// Channel that never delivers a message.
pub mod never;
/// Channel that delivers messages periodically.
pub mod tick;
/// Unbounded channel implementation.
pub mod unbounded;

/// The outcome of `unbounded::Receiver::recv_deadline` /
/// `bounded::Receiver::recv_deadline`.
///
/// Kept distinct from a bare `Option<T>` (which could only report `None`
/// for both "the deadline passed" and "every sender disconnected") because
/// a caller bounding an internal wait needs to treat the two differently:
/// a timeout is worth retrying, cancelling, or escalating with a loud log;
/// a disconnect never will succeed no matter how long it waits, so there
/// is nothing left to retry.
#[derive(Debug, PartialEq, Eq)]
pub enum RecvDeadline<T> {
    /// A message was received before the deadline.
    Msg(T),
    /// The deadline passed with no message and no disconnect.
    Timeout,
    /// Every sender disconnected and the channel was (and remains) empty.
    Disconnected,
}
