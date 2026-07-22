//! Object-safe request preparation consumed only by the reactor owner.

use std::time::Duration;

use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire_core::Bytes;

use crate::{RequestError, response::ResponseRegistry};

/// One generated request whose response type remains owned behind its completion.
pub(crate) trait ErasedRequest: Send {
    /// Returns the public logical call identity allocated before mailbox admission.
    fn call_id(&self) -> CallId;

    /// Returns the relative timeout to map onto the reactor clock at admission.
    fn timeout(&self) -> Duration;

    /// Encodes the request and transfers its typed completion into FIFO ownership.
    fn prepare(
        self: Box<Self>,
        correlation_id: CorrelationId,
        responses: &mut ResponseRegistry,
    ) -> Result<Bytes, RequestError>;

    /// Settles a request that cannot reach typed FIFO response ownership.
    fn fail(self: Box<Self>, failure: RequestError);
}
