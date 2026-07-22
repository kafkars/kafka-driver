//! Terminal peer-framing faults and recoverable decoder admission failures.

use std::{error::Error, fmt};

/// Why bytes could not be admitted or decoded as a Kafka frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameDecodeError {
    /// The decoder already observed a terminal peer-framing fault.
    DecoderFailed,
    /// Admitting one input chunk would exceed the aggregate unread-byte bound.
    BufferCapacityExceeded {
        /// Bytes already retained by the decoder.
        buffered: usize,
        /// Bytes in the rejected input chunk.
        incoming: usize,
        /// Configured aggregate unread-byte limit.
        limit: usize,
    },
    /// Kafka frame lengths are signed and may not be negative.
    NegativeFrameLength {
        /// Signed length declared by the peer.
        length: i32,
    },
    /// A peer-declared frame body exceeds the configured per-frame bound.
    FrameTooLarge {
        /// Body length declared by the peer.
        length: usize,
        /// Configured per-frame body limit.
        limit: usize,
    },
}

impl fmt::Display for FrameDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecoderFailed => formatter.write_str("the frame decoder has already failed"),
            Self::BufferCapacityExceeded {
                buffered,
                incoming,
                limit,
            } => write!(
                formatter,
                "admitting {incoming} bytes with {buffered} buffered exceeds the {limit}-byte limit"
            ),
            Self::NegativeFrameLength { length } => {
                write!(
                    formatter,
                    "peer declared negative Kafka frame length {length}"
                )
            }
            Self::FrameTooLarge { length, limit } => write!(
                formatter,
                "peer declared {length}-byte Kafka frame above the {limit}-byte limit"
            ),
        }
    }
}

impl Error for FrameDecodeError {}
