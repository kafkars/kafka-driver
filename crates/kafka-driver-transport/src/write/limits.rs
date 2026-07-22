//! Explicit frame-count and retained-byte bounds for one ordered writer.

use std::num::NonZeroUsize;

const DEFAULT_MAX_QUEUED_FRAMES: NonZeroUsize = nonzero(256);
const DEFAULT_MAX_BUFFERED_BYTES: NonZeroUsize = nonzero(128 * 1024 * 1024);

/// Resource bounds for complete frames retained by one ordered writer.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteQueueLimits {
    max_queued_frames: NonZeroUsize,
    max_buffered_bytes: NonZeroUsize,
}

impl WriteQueueLimits {
    /// Creates explicit frame-count and retained-byte bounds.
    pub const fn new(max_queued_frames: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Self {
        Self {
            max_queued_frames,
            max_buffered_bytes,
        }
    }

    /// Returns the maximum complete frames retained by the writer.
    pub const fn max_queued_frames(self) -> usize {
        self.max_queued_frames.get()
    }

    /// Returns the maximum original frame bytes retained by the writer.
    pub const fn max_buffered_bytes(self) -> usize {
        self.max_buffered_bytes.get()
    }
}

impl Default for WriteQueueLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_QUEUED_FRAMES, DEFAULT_MAX_BUFFERED_BYTES)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("write queue defaults must be nonzero");
    };
    value
}
