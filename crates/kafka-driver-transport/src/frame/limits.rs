//! Validated byte bounds for one streaming Kafka frame decoder.

use std::{error::Error, fmt, num::NonZeroUsize};

pub(super) const LENGTH_PREFIX_BYTES: usize = size_of::<i32>();

const DEFAULT_MAX_FRAME_BYTES: NonZeroUsize = nonzero(128 * 1024 * 1024);
const DEFAULT_MAX_BUFFERED_BYTES: NonZeroUsize = nonzero(128 * 1024 * 1024 + LENGTH_PREFIX_BYTES);

/// Byte bounds applied before peer-controlled frame allocation or extraction.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    max_frame_bytes: NonZeroUsize,
    max_buffered_bytes: NonZeroUsize,
}

impl FrameLimits {
    /// Validates frame and aggregate buffered-byte limits.
    pub fn new(
        max_frame_bytes: NonZeroUsize,
        max_buffered_bytes: NonZeroUsize,
    ) -> Result<Self, FrameLimitsError> {
        let maximum_protocol_frame = usize::try_from(i32::MAX).unwrap_or(usize::MAX);
        if max_frame_bytes.get() > maximum_protocol_frame {
            return Err(FrameLimitsError::FrameExceedsProtocolMaximum {
                requested: max_frame_bytes.get(),
                maximum: maximum_protocol_frame,
            });
        }
        let Some(required) = max_frame_bytes.get().checked_add(LENGTH_PREFIX_BYTES) else {
            return Err(FrameLimitsError::FrameExceedsProtocolMaximum {
                requested: max_frame_bytes.get(),
                maximum: maximum_protocol_frame,
            });
        };
        if max_buffered_bytes.get() < required {
            return Err(FrameLimitsError::BufferCannotHoldMaximumFrame {
                required,
                configured: max_buffered_bytes.get(),
            });
        }
        Ok(Self {
            max_frame_bytes,
            max_buffered_bytes,
        })
    }

    /// Returns the maximum peer-declared frame body length.
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes.get()
    }

    /// Returns the maximum aggregate unread bytes retained by the decoder.
    pub const fn max_buffered_bytes(self) -> usize {
        self.max_buffered_bytes.get()
    }
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
        }
    }
}

/// Why requested frame limits cannot describe a valid decoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameLimitsError {
    /// Kafka's signed 32-bit length prefix cannot represent the requested body.
    FrameExceedsProtocolMaximum {
        /// Requested maximum body bytes.
        requested: usize,
        /// Largest body length representable by the protocol prefix.
        maximum: usize,
    },
    /// The aggregate buffer could never contain one maximum-size framed body.
    BufferCannotHoldMaximumFrame {
        /// Bytes required for the maximum body and its prefix.
        required: usize,
        /// Configured aggregate unread-byte limit.
        configured: usize,
    },
}

impl fmt::Display for FrameLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameExceedsProtocolMaximum { requested, maximum } => write!(
                formatter,
                "requested {requested}-byte frame limit exceeds Kafka's {maximum}-byte maximum"
            ),
            Self::BufferCannotHoldMaximumFrame {
                required,
                configured,
            } => write!(
                formatter,
                "frame buffer needs {required} bytes but is limited to {configured}"
            ),
        }
    }
}

impl Error for FrameLimitsError {}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("frame defaults must be nonzero");
    };
    value
}
