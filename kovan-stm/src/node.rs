use kovan::RetiredNode;

/// The internal node structure stored in Kovan Atomics.
///
/// # Safety Layout
///
/// `RetiredNode` **must** be the first field. Kovan casts `*mut T` to `*mut RetiredNode`
/// internally. If this layout is violated, the reclamation system will corrupt memory.
#[repr(C)]
pub struct StmNode<T> {
    /// The header required by Kovan for memory reclamation.
    pub(crate) header: RetiredNode,
    /// The actual user data.
    pub(crate) data: T,
}

impl<T> StmNode<T> {
    pub fn new(data: T) -> Self {
        Self {
            header: RetiredNode::new(),
            data,
        }
    }
}
