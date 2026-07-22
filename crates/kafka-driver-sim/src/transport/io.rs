//! Typed transport calls and outcomes with explicit epoch ownership.

use std::num::NonZeroUsize;

use kafka_driver_core::{ConnectionEpoch, TransportId};

/// Connection epoch and transport identity carried by one I/O observation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransportIdentity {
    epoch: ConnectionEpoch,
    transport_id: TransportId,
}

impl TransportIdentity {
    /// Creates an explicit transport identity.
    pub const fn new(epoch: ConnectionEpoch, transport_id: TransportId) -> Self {
        Self {
            epoch,
            transport_id,
        }
    }

    /// Returns the connection epoch.
    pub const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the transport identity.
    pub const fn transport_id(self) -> TransportId {
        self.transport_id
    }
}

/// One bounded read call expected by a transport script.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    identity: TransportIdentity,
    max_bytes: NonZeroUsize,
}

impl ReadRequest {
    /// Creates a bounded read request.
    pub const fn new(identity: TransportIdentity, max_bytes: NonZeroUsize) -> Self {
        Self {
            identity,
            max_bytes,
        }
    }

    /// Returns the transport identity making the call.
    pub const fn identity(self) -> TransportIdentity {
        self.identity
    }

    /// Returns the maximum bytes accepted by this call.
    pub const fn max_bytes(self) -> usize {
        self.max_bytes.get()
    }
}

/// One exact write call expected by a transport script.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRequest {
    identity: TransportIdentity,
    bytes: Vec<u8>,
}

impl WriteRequest {
    /// Creates a write request owning the exact offered bytes.
    pub const fn new(identity: TransportIdentity, bytes: Vec<u8>) -> Self {
        Self { identity, bytes }
    }

    /// Returns the transport identity making the call.
    pub const fn identity(&self) -> TransportIdentity {
        self.identity
    }

    /// Returns the exact bytes offered to the transport.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Sanitized transport failure usable across operating systems.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportFault {
    /// The peer reset the connection.
    ConnectionReset,
    /// A write found a closed peer.
    BrokenPipe,
    /// The operation exceeded an external timeout.
    TimedOut,
    /// Test-specific injected failure code.
    Injected(u32),
}

/// Scripted result of one bounded transport read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadResult {
    /// Bytes read, possibly fewer than requested.
    Bytes(Vec<u8>),
    /// No read progress is currently possible.
    WouldBlock,
    /// The peer closed its write side cleanly.
    Closed,
    /// The read failed.
    Failed(TransportFault),
}

/// Scripted result of one transport write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteResult {
    /// Bytes written, possibly fewer than offered.
    Written(usize),
    /// No write progress is currently possible.
    WouldBlock,
    /// The write failed.
    Failed(TransportFault),
}

/// Independently identified transport result that may intentionally be stale.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportOutcome<T> {
    identity: TransportIdentity,
    result: T,
}

impl<T> TransportOutcome<T> {
    /// Creates an explicitly identified transport result.
    pub const fn new(identity: TransportIdentity, result: T) -> Self {
        Self { identity, result }
    }

    /// Returns the identity carried by the result.
    pub const fn identity(&self) -> TransportIdentity {
        self.identity
    }

    /// Borrows the transport result.
    pub const fn result(&self) -> &T {
        &self.result
    }

    /// Consumes the outcome and returns its transport result.
    pub fn into_result(self) -> T {
        self.result
    }
}
