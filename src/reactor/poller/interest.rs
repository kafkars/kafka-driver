//! Nonempty read and write interests armed for one resource.

use mio::Interest;

/// Useful nonblocking progress currently requested from the OS poller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum PollInterest {
    /// Read progress only.
    Readable,
    /// Write progress only.
    Writable,
    /// Either read or write progress.
    ReadWrite,
}

impl PollInterest {
    pub(super) const fn into_mio(self) -> Interest {
        match self {
            Self::Readable => Interest::READABLE,
            Self::Writable => Interest::WRITABLE,
            Self::ReadWrite => Interest::READABLE.add(Interest::WRITABLE),
        }
    }
}
