//! Exact-retention Kafka reply framing for Bornera.

use std::num::NonZeroUsize;

use bornera_core::FrameDecoder;
use bytes::Bytes;
use calandria::{Retained, RetainedBytes};
use kafka_driver_transport::{
    FrameBody, FrameDecodeError, FrameDecoder as TransportFrameDecoder, FrameLimits,
};

use super::{KafkaFrameDecodeError, KafkaFrameDecoderConfigError};

const LENGTH_PREFIX_BYTES: usize = size_of::<i32>();
const MIN_GROWTH_BYTES: usize = 8;

/// One complete Kafka response body with an exact allocation contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct KafkaFrame {
    bytes: Box<[u8]>,
}

impl KafkaFrame {
    fn copy_from_body(body: &FrameBody) -> Self {
        Self {
            bytes: body.as_bytes().to_vec().into_boxed_slice(),
        }
    }

    pub(in crate::reactor) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Makes the copy into existing response decoding explicit at the adapter edge.
    pub(in crate::reactor) fn into_bytes(self) -> Bytes {
        Bytes::from(self.bytes)
    }

    #[cfg(test)]
    pub(super) fn copy_from_slice(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec().into_boxed_slice(),
        }
    }
}

impl Retained for KafkaFrame {
    fn retained_bytes(&self) -> RetainedBytes {
        retained(self.bytes.len())
    }
}

/// Bornera decoder backed by the driver's canonical Kafka framing rules.
#[derive(Debug)]
pub(in crate::reactor) struct KafkaFrameDecoder {
    #[cfg(test)]
    configured_max_buffered_bytes: usize,
    coalesced_limits: FrameLimits,
    max_input_bytes: NonZeroUsize,
    bornera_retained_limit: RetainedBytes,
    buffered: Vec<u8>,
    failed: bool,
}

impl KafkaFrameDecoder {
    /// Configures honest `Vec` retention and bounded coalescing/read headroom.
    /// `FrameLimits::max_buffered_bytes` remains the configured logical buffer.
    /// The adapter may temporarily coalesce one complete input quantum beyond
    /// it so a chunk that completes one frame can retain the next frame's prefix.
    /// Bornera admission adds one further quantum because it checks current
    /// retained `Vec` capacity plus borrowed input before calling `feed`.
    /// `max_input_bytes` must equal the slot's `IoLimits::chunk_bytes` value.
    pub(in crate::reactor) fn new(
        limits: FrameLimits,
        max_input_bytes: NonZeroUsize,
    ) -> Result<Self, KafkaFrameDecoderConfigError> {
        let configured_max_buffered_bytes = limits.max_buffered_bytes();
        let Some(max_coalesced_bytes) =
            configured_max_buffered_bytes.checked_add(max_input_bytes.get())
        else {
            return Err(KafkaFrameDecoderConfigError::RetainedLimitOverflow {
                buffered: configured_max_buffered_bytes,
                max_input: max_input_bytes.get(),
            });
        };
        let Some(admission_bytes) = max_coalesced_bytes.checked_add(max_input_bytes.get()) else {
            return Err(KafkaFrameDecoderConfigError::RetainedLimitOverflow {
                buffered: configured_max_buffered_bytes,
                max_input: max_input_bytes.get(),
            });
        };
        let Some(max_frame_bytes) = NonZeroUsize::new(limits.max_frame_bytes()) else {
            return Err(KafkaFrameDecoderConfigError::InvalidEffectiveLimits {
                max_frame: limits.max_frame_bytes(),
                max_buffered: max_coalesced_bytes,
            });
        };
        let Some(max_coalesced_bytes) = NonZeroUsize::new(max_coalesced_bytes) else {
            return Err(KafkaFrameDecoderConfigError::InvalidEffectiveLimits {
                max_frame: limits.max_frame_bytes(),
                max_buffered: max_coalesced_bytes,
            });
        };
        let coalesced_limits =
            FrameLimits::new(max_frame_bytes, max_coalesced_bytes).map_err(|_| {
                KafkaFrameDecoderConfigError::InvalidEffectiveLimits {
                    max_frame: max_frame_bytes.get(),
                    max_buffered: max_coalesced_bytes.get(),
                }
            })?;
        let Ok(bornera_retained_limit) = RetainedBytes::try_from(admission_bytes) else {
            return Err(KafkaFrameDecoderConfigError::RetainedLimitOverflow {
                buffered: configured_max_buffered_bytes,
                max_input: max_input_bytes.get(),
            });
        };
        Ok(Self {
            #[cfg(test)]
            configured_max_buffered_bytes,
            coalesced_limits,
            max_input_bytes,
            bornera_retained_limit,
            buffered: Vec::new(),
            failed: false,
        })
    }

    /// Returns the limit to supply to Bornera's `FrameDriver`.
    ///
    /// If the configured logical buffer is `B` and the input quantum is `Q`,
    /// the adapter's coalesced buffer is bounded by `B + Q`. This admission
    /// limit is `B + 2Q`, leaving one more borrowed quantum beyond an honestly
    /// reported `Vec` capacity at the effective coalesced bound.
    pub(in crate::reactor) const fn bornera_retained_limit(&self) -> RetainedBytes {
        self.bornera_retained_limit
    }

    #[cfg(test)]
    pub(super) const fn buffer_limits(&self) -> (usize, usize) {
        (
            self.configured_max_buffered_bytes,
            self.coalesced_limits.max_buffered_bytes(),
        )
    }

    fn ensure_capacity(&mut self, accepted: usize) -> Result<(), KafkaFrameDecodeError> {
        if accepted <= self.buffered.capacity() {
            return Ok(());
        }
        let target = accepted
            .max(self.buffered.capacity().saturating_mul(2))
            .max(MIN_GROWTH_BYTES)
            .min(self.coalesced_limits.max_buffered_bytes());
        self.buffered
            .reserve_exact(target.saturating_sub(self.buffered.len()));
        let observed = self.buffered.capacity();
        if observed > self.coalesced_limits.max_buffered_bytes() {
            self.buffered = Vec::new();
            self.failed = true;
            return Err(KafkaFrameDecodeError::AllocationCapacityExceeded {
                observed,
                limit: self.coalesced_limits.max_buffered_bytes(),
            });
        }
        Ok(())
    }
}

impl FrameDecoder for KafkaFrameDecoder {
    type Frame = KafkaFrame;
    type Error = KafkaFrameDecodeError;

    fn feed(&mut self, input: &[u8]) -> Result<(), Self::Error> {
        if self.failed {
            return Err(KafkaFrameDecodeError::DecoderFailed);
        }
        if input.len() > self.max_input_bytes.get() {
            return Err(KafkaFrameDecodeError::InputChunkTooLarge {
                incoming: input.len(),
                limit: self.max_input_bytes.get(),
            });
        }
        let Some(accepted) = self.buffered.len().checked_add(input.len()) else {
            return Err(KafkaFrameDecodeError::Framing(
                FrameDecodeError::BufferCapacityExceeded {
                    buffered: self.buffered.len(),
                    incoming: input.len(),
                    limit: self.coalesced_limits.max_buffered_bytes(),
                },
            ));
        };
        if accepted > self.coalesced_limits.max_buffered_bytes() {
            return Err(KafkaFrameDecodeError::Framing(
                FrameDecodeError::BufferCapacityExceeded {
                    buffered: self.buffered.len(),
                    incoming: input.len(),
                    limit: self.coalesced_limits.max_buffered_bytes(),
                },
            ));
        }
        self.ensure_capacity(accepted)?;
        self.buffered.extend_from_slice(input);
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        if self.failed {
            return Err(KafkaFrameDecodeError::DecoderFailed);
        }
        let mut decoder = TransportFrameDecoder::new(self.coalesced_limits);
        if let Err(error) = decoder.feed(&self.buffered) {
            self.failed = true;
            return Err(KafkaFrameDecodeError::Framing(error));
        }
        let body = match decoder.next_frame() {
            Ok(body) => body,
            Err(error) => {
                self.failed = true;
                return Err(KafkaFrameDecodeError::Framing(error));
            }
        };
        let Some(body) = body else {
            return Ok(None);
        };
        let consumed = LENGTH_PREFIX_BYTES.saturating_add(body.len());
        self.buffered.drain(..consumed);
        if self.buffered.is_empty() {
            self.buffered = Vec::new();
        }
        Ok(Some(KafkaFrame::copy_from_body(&body)))
    }

    fn retained_bytes(&self) -> RetainedBytes {
        retained(self.buffered.capacity())
    }
}

fn retained(bytes: usize) -> RetainedBytes {
    RetainedBytes::try_from(bytes).unwrap_or(RetainedBytes::new(u64::MAX))
}
