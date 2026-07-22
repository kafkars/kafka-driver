//! Bounded FIFO storage and exact partial-write advancement.

use std::{collections::VecDeque, fmt, num::NonZeroUsize};

use bytes::Bytes;
use kafka_driver_core::{CallId, EffectId};

use super::{
    DiscardedWrites, WriteAccepted, WriteAdmissionError, WriteAdmissionFailure, WriteIdentityKind,
    WriteProgress, WriteProgressError, WriteQueueLimits, WriteSlice,
};

const KAFKA_LENGTH_PREFIX_BYTES: usize = size_of::<i32>();

/// Single-transport owner of complete encoded request frames in wire order.
pub struct WriteQueue {
    limits: WriteQueueLimits,
    frames: VecDeque<QueuedWrite>,
    buffered_bytes: usize,
}

impl fmt::Debug for WriteQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteQueue")
            .field("limits", &self.limits)
            .field("queued_frames", &self.frames.len())
            .field("buffered_bytes", &self.buffered_bytes)
            .finish()
    }
}

impl WriteQueue {
    /// Creates an empty writer with explicit count and byte bounds.
    pub fn new(limits: WriteQueueLimits) -> Self {
        Self {
            limits,
            frames: VecDeque::new(),
            buffered_bytes: 0,
        }
    }

    /// Accepts ownership of one complete frame or returns it untouched.
    pub fn admit(
        &mut self,
        call_id: CallId,
        effect_id: EffectId,
        frame: Bytes,
    ) -> Result<WriteAccepted, WriteAdmissionError> {
        if frame.len() < KAFKA_LENGTH_PREFIX_BYTES {
            return Err(reject(
                WriteAdmissionFailure::FrameTooShort {
                    bytes: frame.len(),
                    minimum: KAFKA_LENGTH_PREFIX_BYTES,
                },
                frame,
            ));
        }
        if self.frames.iter().any(|queued| queued.call_id == call_id) {
            return Err(reject(
                WriteAdmissionFailure::IdentityInUse(WriteIdentityKind::Call),
                frame,
            ));
        }
        if self
            .frames
            .iter()
            .any(|queued| queued.effect_id == effect_id)
        {
            return Err(reject(
                WriteAdmissionFailure::IdentityInUse(WriteIdentityKind::Effect),
                frame,
            ));
        }
        if self.frames.len() >= self.limits.max_queued_frames() {
            return Err(reject(
                WriteAdmissionFailure::FrameCapacityReached {
                    limit: self.limits.max_queued_frames(),
                },
                frame,
            ));
        }
        let Some(accepted_bytes) = self.buffered_bytes.checked_add(frame.len()) else {
            return Err(reject(
                WriteAdmissionFailure::ByteCapacityReached {
                    buffered: self.buffered_bytes,
                    incoming: frame.len(),
                    limit: self.limits.max_buffered_bytes(),
                },
                frame,
            ));
        };
        if accepted_bytes > self.limits.max_buffered_bytes() {
            return Err(reject(
                WriteAdmissionFailure::ByteCapacityReached {
                    buffered: self.buffered_bytes,
                    incoming: frame.len(),
                    limit: self.limits.max_buffered_bytes(),
                },
                frame,
            ));
        }
        let accepted = WriteAccepted::new(call_id, effect_id, frame.len());
        self.buffered_bytes = accepted_bytes;
        self.frames.push_back(QueuedWrite {
            call_id,
            effect_id,
            frame,
            written: 0,
        });
        Ok(accepted)
    }

    /// Borrows at most `max_bytes` from only the FIFO queue front.
    pub fn front(&self, max_bytes: NonZeroUsize) -> Option<WriteSlice<'_>> {
        let queued = self.frames.front()?;
        let remaining = &queued.frame[queued.written..];
        let length = remaining.len().min(max_bytes.get());
        Some(WriteSlice::new(
            queued.call_id,
            queued.effect_id,
            &remaining[..length],
        ))
    }

    /// Applies exact socket progress to the named FIFO-front effect.
    pub fn advance(
        &mut self,
        effect_id: EffectId,
        written: usize,
    ) -> Result<WriteProgress, WriteProgressError> {
        let Some(front) = self.frames.front_mut() else {
            return Err(WriteProgressError::NoPendingWrite);
        };
        if front.effect_id != effect_id {
            return Err(WriteProgressError::OutOfOrderEffect {
                expected: front.effect_id,
                received: effect_id,
            });
        }
        let remaining = front.frame.len() - front.written;
        if written > remaining {
            return Err(WriteProgressError::ExceedsRemaining { written, remaining });
        }
        front.written += written;
        if front.written < front.frame.len() {
            return Ok(WriteProgress::Pending {
                call_id: front.call_id,
                effect_id,
                remaining: front.frame.len() - front.written,
            });
        }
        let Some(completed) = self.frames.pop_front() else {
            return Err(WriteProgressError::NoPendingWrite);
        };
        self.buffered_bytes -= completed.frame.len();
        Ok(WriteProgress::Complete {
            call_id: completed.call_id,
            effect_id,
            frame_bytes: completed.frame.len(),
        })
    }

    /// Discards every retained frame when its transport epoch ends.
    pub fn discard_all(&mut self) -> DiscardedWrites {
        let discarded = DiscardedWrites {
            frames: self.frames.len(),
            bytes: self.buffered_bytes,
        };
        self.frames.clear();
        self.buffered_bytes = 0;
        discarded
    }

    /// Returns complete frames currently retained in wire order.
    pub fn queued_frames(&self) -> usize {
        self.frames.len()
    }

    /// Returns original encoded bytes retained by all queued frames.
    pub const fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }
}

fn reject(failure: WriteAdmissionFailure, frame: Bytes) -> WriteAdmissionError {
    WriteAdmissionError::new(failure, frame)
}

struct QueuedWrite {
    call_id: CallId,
    effect_id: EffectId,
    frame: Bytes,
    written: usize,
}
