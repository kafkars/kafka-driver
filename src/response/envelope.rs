//! Inspected response ownership awaiting deterministic connection approval.

use kafka_driver_core::CorrelationId;
use kafka_wire_core::Bytes;

/// Header-inspected frame body that has not yet completed a typed call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResponseEnvelope {
    correlation_id: CorrelationId,
    body: Bytes,
}

impl ResponseEnvelope {
    pub(super) const fn new(correlation_id: CorrelationId, body: Bytes) -> Self {
        Self {
            correlation_id,
            body,
        }
    }

    /// Returns the correlation observed in the response header.
    pub(crate) const fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }

    /// Returns encoded response-body bytes after the consumed header.
    #[cfg(test)]
    pub(crate) const fn body_bytes(&self) -> usize {
        self.body.len()
    }

    pub(super) fn into_body(self) -> Bytes {
        self.body
    }
}
