//! Incremental bounded decoding of Kafka's signed length-prefixed frames.

use bytes::BytesMut;

use super::{FrameBody, FrameDecodeError, FrameLimits, limits::LENGTH_PREFIX_BYTES};

/// Single-stream accumulator for complete Kafka frame bodies.
#[derive(Debug)]
pub struct FrameDecoder {
    limits: FrameLimits,
    buffered: BytesMut,
    failed: bool,
}

impl FrameDecoder {
    /// Creates an empty decoder with explicit validated byte limits.
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            limits,
            buffered: BytesMut::new(),
            failed: false,
        }
    }

    /// Admits one byte chunk without interpreting incomplete frame data.
    pub fn feed(&mut self, input: &[u8]) -> Result<(), FrameDecodeError> {
        if self.failed {
            return Err(FrameDecodeError::DecoderFailed);
        }
        let accepted = self.buffered.len().checked_add(input.len());
        if accepted.is_none_or(|bytes| bytes > self.limits.max_buffered_bytes()) {
            return Err(FrameDecodeError::BufferCapacityExceeded {
                buffered: self.buffered.len(),
                incoming: input.len(),
                limit: self.limits.max_buffered_bytes(),
            });
        }
        self.buffered.extend_from_slice(input);
        Ok(())
    }

    /// Extracts at most one complete frame body while retaining later bytes.
    pub fn next_frame(&mut self) -> Result<Option<FrameBody>, FrameDecodeError> {
        if self.failed {
            return Err(FrameDecodeError::DecoderFailed);
        }
        if self.buffered.len() < LENGTH_PREFIX_BYTES {
            return Ok(None);
        }
        let prefix = [
            self.buffered[0],
            self.buffered[1],
            self.buffered[2],
            self.buffered[3],
        ];
        let signed_length = i32::from_be_bytes(prefix);
        if signed_length < 0 {
            return self.fail(FrameDecodeError::NegativeFrameLength {
                length: signed_length,
            });
        }
        let length = usize::try_from(signed_length).unwrap_or(usize::MAX);
        if length > self.limits.max_frame_bytes() {
            return self.fail(FrameDecodeError::FrameTooLarge {
                length,
                limit: self.limits.max_frame_bytes(),
            });
        }
        let total = LENGTH_PREFIX_BYTES + length;
        if self.buffered.len() < total {
            return Ok(None);
        }
        let frame = self.buffered.split_to(total).freeze();
        Ok(Some(FrameBody::new(frame.slice(LENGTH_PREFIX_BYTES..))))
    }

    /// Returns unread bytes retained across incomplete or coalesced frames.
    pub fn buffered_bytes(&self) -> usize {
        self.buffered.len()
    }

    fn fail<T>(&mut self, error: FrameDecodeError) -> Result<T, FrameDecodeError> {
        self.failed = true;
        Err(error)
    }
}
