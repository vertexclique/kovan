use core::fmt::Display;
use core::fmt::Result;

#[derive(Debug)]
pub enum StmError {
    /// The transaction encountered a conflict or invalid state and should be retried.
    Retry,
    /// A generic error string (for user logic).
    User(String),
}

impl Display for StmError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result {
        match self {
            StmError::Retry => write!(f, "Transaction retry required"),
            StmError::User(s) => write!(f, "Transaction error: {}", s),
        }
    }
}

impl std::error::Error for StmError {}
