//! Recoverable admission ownership and FIFO progress contract violations.

use std::{error::Error, fmt};

use bytes::Bytes;
use kafka_driver_core::{Delivery, EffectId};

/// Pending identity category that a new frame attempted to reuse.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteIdentityKind {
    /// Public call identity.
    Call,
    /// External write-effect identity.
    Effect,
}

/// Why a complete frame could not enter the ordered writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteAdmissionFailure {
    /// The encoded request does not contain a complete Kafka length prefix.
    FrameTooShort {
        /// Bytes in the rejected frame.
        bytes: usize,
        /// Minimum bytes required for the signed Kafka length prefix.
        minimum: usize,
    },
    /// A call or effect identity is still owned by a queued frame.
    IdentityInUse(WriteIdentityKind),
    /// The configured queued-frame count was reached.
    FrameCapacityReached {
        /// Configured maximum queued frames.
        limit: usize,
    },
    /// Retaining the frame would exceed the configured byte bound.
    ByteCapacityReached {
        /// Original bytes already retained by the writer.
        buffered: usize,
        /// Bytes in the rejected frame.
        incoming: usize,
        /// Configured retained-byte maximum.
        limit: usize,
    },
}

/// Rejected admission that returns ownership of the unsent encoded frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAdmissionError {
    failure: WriteAdmissionFailure,
    frame: Bytes,
}

impl WriteAdmissionError {
    pub(super) const fn new(failure: WriteAdmissionFailure, frame: Bytes) -> Self {
        Self { failure, frame }
    }

    /// Returns the explicit admission failure.
    pub const fn failure(&self) -> WriteAdmissionFailure {
        self.failure
    }

    /// Returns certainty for a frame the writer never accepted.
    pub const fn delivery(&self) -> Delivery {
        Delivery::NotSent
    }

    /// Recovers ownership of the unadmitted encoded frame.
    pub fn into_frame(self) -> Bytes {
        self.frame
    }
}

impl fmt::Display for WriteAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "write admission rejected: {}", self.failure)
    }
}

impl Error for WriteAdmissionError {}

impl fmt::Display for WriteAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooShort { bytes, minimum } => {
                write!(
                    formatter,
                    "frame has {bytes} bytes; at least {minimum} required"
                )
            }
            Self::IdentityInUse(kind) => write!(formatter, "{kind} identity is already queued"),
            Self::FrameCapacityReached { limit } => {
                write!(formatter, "queued-frame limit {limit} reached")
            }
            Self::ByteCapacityReached {
                buffered,
                incoming,
                limit,
            } => write!(
                formatter,
                "retaining {incoming} bytes with {buffered} buffered exceeds limit {limit}"
            ),
        }
    }
}

impl fmt::Display for WriteIdentityKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Call => formatter.write_str("call"),
            Self::Effect => formatter.write_str("effect"),
        }
    }
}

/// Why reported socket progress could not apply to the FIFO queue front.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteProgressError {
    /// No encoded frame currently awaits socket progress.
    NoPendingWrite,
    /// Progress named a frame other than the FIFO front.
    OutOfOrderEffect {
        /// Effect identity currently at the FIFO front.
        expected: EffectId,
        /// Effect identity supplied with progress.
        received: EffectId,
    },
    /// Reported bytes exceed the queue front's remaining bytes.
    ExceedsRemaining {
        /// Bytes reported written.
        written: usize,
        /// Bytes remaining before the report.
        remaining: usize,
    },
}

impl fmt::Display for WriteProgressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPendingWrite => formatter.write_str("no ordered write is pending"),
            Self::OutOfOrderEffect { expected, received } => write!(
                formatter,
                "write progress named effect {received:?}; FIFO front is {expected:?}"
            ),
            Self::ExceedsRemaining { written, remaining } => write!(
                formatter,
                "write progress reports {written} bytes with only {remaining} remaining"
            ),
        }
    }
}

impl Error for WriteProgressError {}
