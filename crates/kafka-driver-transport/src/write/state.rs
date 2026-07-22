//! Immutable acceptance, socket-slice, progress, and discard observations.

use kafka_driver_core::{CallId, Delivery, EffectId};

/// Complete frame ownership accepted by the ordered writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteAccepted {
    call_id: CallId,
    effect_id: EffectId,
    frame_bytes: usize,
}

impl WriteAccepted {
    pub(super) const fn new(call_id: CallId, effect_id: EffectId, frame_bytes: usize) -> Self {
        Self {
            call_id,
            effect_id,
            frame_bytes,
        }
    }

    /// Returns the public call whose frame was accepted.
    pub const fn call_id(self) -> CallId {
        self.call_id
    }

    /// Returns the write effect satisfied by queue ownership.
    pub const fn effect_id(self) -> EffectId {
        self.effect_id
    }

    /// Returns the complete encoded frame byte count.
    pub const fn frame_bytes(self) -> usize {
        self.frame_bytes
    }

    /// Returns delivery certainty after complete-frame admission.
    pub const fn delivery(self) -> Delivery {
        Delivery::PossiblySent
    }
}

/// Borrowed byte slice from only the FIFO queue front.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteSlice<'a> {
    call_id: CallId,
    effect_id: EffectId,
    bytes: &'a [u8],
}

impl<'a> WriteSlice<'a> {
    pub(super) const fn new(call_id: CallId, effect_id: EffectId, bytes: &'a [u8]) -> Self {
        Self {
            call_id,
            effect_id,
            bytes,
        }
    }

    /// Returns the public call owning this byte slice.
    pub const fn call_id(self) -> CallId {
        self.call_id
    }

    /// Returns the effect identity required by the progress report.
    pub const fn effect_id(self) -> EffectId {
        self.effect_id
    }

    /// Returns the next contiguous bytes available for one socket write.
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }
}

/// Result of applying byte progress to the FIFO queue front.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteProgress {
    /// The same frame remains at the FIFO front.
    Pending {
        /// Public call owning the frame.
        call_id: CallId,
        /// Write effect owning the frame.
        effect_id: EffectId,
        /// Frame bytes not yet written to the socket.
        remaining: usize,
    },
    /// The complete frame left the FIFO writer.
    Complete {
        /// Public call whose frame completed.
        call_id: CallId,
        /// Write effect whose frame completed.
        effect_id: EffectId,
        /// Original complete frame byte count.
        frame_bytes: usize,
    },
}

/// Queue resources released when a transport epoch is abandoned.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DiscardedWrites {
    /// Complete frames removed from the queue.
    pub frames: usize,
    /// Original encoded bytes released with those frames.
    pub bytes: usize,
}
