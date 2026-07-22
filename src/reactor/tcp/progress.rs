//! Explicit outcome from verifying one nonblocking TCP connect attempt.

/// Progress from verifying one nonblocking TCP connect attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ConnectProgress {
    /// The OS has not completed this connection attempt yet.
    Pending,
    /// Readiness verified the connection and moved it into the open phase.
    Opened,
    /// The connection was already verified open.
    AlreadyOpen,
}
