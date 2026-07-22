//! Zero-copy ownership of one complete Kafka frame body without its length prefix.

use kafka_wire_core::Bytes;

/// One complete Kafka response frame body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameBody {
    bytes: Bytes,
}

impl FrameBody {
    pub(super) const fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    /// Returns the body byte count, excluding the four-byte length prefix.
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the peer sent an empty frame body.
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Borrows the complete body bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the frame body without copying its bytes.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}
