//! Construction bounds and per-turn progress budgets for one plaintext socket.

use std::num::NonZeroUsize;

use kafka_driver_transport::{FrameLimits, WriteQueueLimits};

const DEFAULT_READ_CHUNK_BYTES: NonZeroUsize = nonzero(64 * 1_024);

/// Persistent decoder, writer, and scratch-buffer bounds for one connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct PlaintextLimits {
    frame: FrameLimits,
    write: WriteQueueLimits,
    read_chunk_bytes: NonZeroUsize,
}

impl PlaintextLimits {
    pub(in crate::reactor) const fn new(
        frame: FrameLimits,
        write: WriteQueueLimits,
        read_chunk_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            frame,
            write,
            read_chunk_bytes,
        }
    }

    pub(super) const fn frame(self) -> FrameLimits {
        self.frame
    }

    pub(super) const fn write(self) -> WriteQueueLimits {
        self.write
    }

    pub(in crate::reactor) const fn outbound_frame_bytes(self) -> usize {
        self.write
            .max_buffered_bytes()
            .saturating_sub(size_of::<i32>())
    }

    pub(super) const fn read_chunk_bytes(self) -> NonZeroUsize {
        self.read_chunk_bytes
    }
}

impl Default for PlaintextLimits {
    fn default() -> Self {
        Self::new(
            FrameLimits::default(),
            WriteQueueLimits::default(),
            DEFAULT_READ_CHUNK_BYTES,
        )
    }
}

/// Maximum bytes and complete frames consumed by one read drive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ReadBudget {
    bytes: NonZeroUsize,
    frames: NonZeroUsize,
}

impl ReadBudget {
    pub(in crate::reactor) const fn new(bytes: NonZeroUsize, frames: NonZeroUsize) -> Self {
        Self { bytes, frames }
    }

    pub(super) const fn bytes(self) -> usize {
        self.bytes.get()
    }

    pub(super) const fn frames(self) -> usize {
        self.frames.get()
    }
}

/// Maximum bytes handed to the socket by one write drive.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct WriteBudget(NonZeroUsize);

impl WriteBudget {
    pub(in crate::reactor) const fn new(bytes: NonZeroUsize) -> Self {
        Self(bytes)
    }

    pub(super) const fn bytes(self) -> usize {
        self.0.get()
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("plaintext defaults must be nonzero");
    };
    value
}
