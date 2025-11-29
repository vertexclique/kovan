use crate::flavors::unbounded;

/// Creates a channel that never delivers a message.
pub fn never<T: 'static>() -> unbounded::Receiver<T> {
    let (s, r) = unbounded::channel();
    std::mem::forget(s);
    r
}
