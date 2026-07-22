//! Object-safe request preparation consumed only by the reactor owner.

use kafka_driver_core::{CallId, CorrelationId, Moment};
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::{ApiKey, ApiVersion, Bytes};

use crate::{RequestError, RouteReceipt, TrafficClass, response::ResponseRegistry};

/// One generated request whose response type remains owned behind its completion.
pub(crate) trait ErasedRequest: Send {
    /// Returns the public logical call identity allocated before mailbox admission.
    fn call_id(&self) -> CallId;

    /// Returns the generated Kafka API key whose version must be negotiated.
    fn api_key(&self) -> ApiKey;

    /// Returns the semantic connection lane that must own this call.
    fn traffic_class(&self) -> TrafficClass;

    /// Establishes the absolute deadline once, or returns the existing deadline.
    fn establish_deadline(&mut self, start: Moment) -> Result<Moment, RequestError>;

    /// Returns an encoded-work estimate used by bounded waiting queues.
    fn retained_bytes(&self) -> usize;

    /// Records the exact semantic route selected before broker ownership.
    fn record_route(&mut self, receipt: RouteReceipt) -> Result<(), RouteReceipt>;

    /// Encodes the request and transfers its typed completion into FIFO ownership.
    fn prepare(
        self: Box<Self>,
        correlation_id: CorrelationId,
        version: ApiVersion,
        outbound_limits: OutboundFrameLimits,
        responses: &mut ResponseRegistry,
    ) -> Result<Bytes, RequestError>;

    /// Settles a request that cannot reach typed FIFO response ownership.
    fn fail(self: Box<Self>, failure: RequestError);
}
