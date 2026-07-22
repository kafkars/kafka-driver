//! Explicit socket, framing, and ordered-write progress failures.

use std::{fmt, io};

use kafka_driver_transport::{FrameDecodeError, WriteProgressError};

/// Why a plaintext socket could not continue bounded progress.
#[derive(Debug)]
pub(in crate::reactor) enum PlaintextError {
    /// A nonblocking TCP connect attempt failed verification.
    Connect(io::Error),
    /// A nonblocking socket read failed.
    Read(io::Error),
    /// A nonblocking socket write failed.
    Write(io::Error),
    /// A nonempty queued slice reported zero write progress.
    WriteZero,
    /// Peer-controlled Kafka framing became invalid.
    Frame(FrameDecodeError),
    /// Socket progress contradicted the ordered writer's FIFO front.
    WriteProgress(WriteProgressError),
}

impl fmt::Display for PlaintextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(_) => formatter.write_str("plaintext socket connect failed"),
            Self::Read(_) => formatter.write_str("plaintext socket read failed"),
            Self::Write(_) => formatter.write_str("plaintext socket write failed"),
            Self::WriteZero => formatter.write_str("plaintext socket made zero write progress"),
            Self::Frame(error) => write!(formatter, "plaintext framing failed: {error}"),
            Self::WriteProgress(error) => {
                write!(formatter, "ordered write progress failed: {error}")
            }
        }
    }
}

impl std::error::Error for PlaintextError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(source) | Self::Read(source) | Self::Write(source) => Some(source),
            Self::Frame(source) => Some(source),
            Self::WriteProgress(source) => Some(source),
            Self::WriteZero => None,
        }
    }
}

impl From<FrameDecodeError> for PlaintextError {
    fn from(source: FrameDecodeError) -> Self {
        Self::Frame(source)
    }
}

impl From<WriteProgressError> for PlaintextError {
    fn from(source: WriteProgressError) -> Self {
        Self::WriteProgress(source)
    }
}
