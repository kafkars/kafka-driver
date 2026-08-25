//! Configuration and protocol failures at the Bornera Kafka framing boundary.

use std::fmt;

use kafka_driver_transport::FrameDecodeError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum KafkaFrameDecoderConfigError {
    RetainedLimitOverflow {
        buffered: usize,
        max_input: usize,
    },
    InvalidEffectiveLimits {
        max_frame: usize,
        max_buffered: usize,
    },
}

impl fmt::Display for KafkaFrameDecoderConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RetainedLimitOverflow {
                buffered,
                max_input,
            } => write!(
                formatter,
                "Kafka decoder bound {buffered} plus two {max_input}-byte input quanta overflows retained-byte accounting"
            ),
            Self::InvalidEffectiveLimits {
                max_frame,
                max_buffered,
            } => write!(
                formatter,
                "Kafka decoder effective limits are invalid: {max_frame}-byte frames in a {max_buffered}-byte coalesced buffer"
            ),
        }
    }
}

impl std::error::Error for KafkaFrameDecoderConfigError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum KafkaFrameDecodeError {
    DecoderFailed,
    InputChunkTooLarge { incoming: usize, limit: usize },
    AllocationCapacityExceeded { observed: usize, limit: usize },
    Framing(FrameDecodeError),
}

impl fmt::Display for KafkaFrameDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecoderFailed => formatter.write_str("the Kafka decoder has already failed"),
            Self::InputChunkTooLarge { incoming, limit } => write!(
                formatter,
                "Bornera supplied a {incoming}-byte decoder chunk above the {limit}-byte quantum"
            ),
            Self::AllocationCapacityExceeded { observed, limit } => write!(
                formatter,
                "Kafka decoder allocation retained {observed} bytes above the {limit}-byte bound"
            ),
            Self::Framing(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for KafkaFrameDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Framing(error) => Some(error),
            Self::DecoderFailed
            | Self::InputChunkTooLarge { .. }
            | Self::AllocationCapacityExceeded { .. } => None,
        }
    }
}
