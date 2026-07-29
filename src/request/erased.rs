//! Object-safe request preparation consumed only by the reactor owner.

use std::time::Instant;

use kafka_driver_core::{CallId, CorrelationId, Moment, NegotiatedApi, OutcomeStamp};
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::{ApiKey, ApiVersion, Bytes};

use crate::{RequestError, TrafficClass, api::RouteFact, response::ResponseRegistry};

/// One generated request whose response type remains owned behind its completion.
pub(crate) trait ErasedRequest: Send {
    /// Returns the public logical call identity allocated before mailbox admission.
    fn call_id(&self) -> CallId;

    /// Returns the generated Kafka API key whose version must be negotiated.
    fn api_key(&self) -> ApiKey;

    /// Returns the semantic connection lane that must own this call.
    fn traffic_class(&self) -> TrafficClass;

    /// Selects this request's version within the active negotiated overlap.
    fn select_version(&mut self, negotiated: NegotiatedApi) -> Result<ApiVersion, RequestError>;

    /// Establishes the absolute deadline once, or returns the existing deadline.
    fn establish_deadline(&mut self, start: Moment) -> Result<Moment, RequestError>;

    /// Returns the current deep admission weight for bounded queues.
    fn retained_bytes(&self) -> usize;

    /// Records first reactor ownership for public lifecycle observation.
    fn mark_reactor(&mut self, at: Instant);

    /// Records first resolved semantic-route ownership.
    fn mark_routed(&mut self, at: Instant);

    /// Records the exact semantic route selected before broker ownership.
    fn record_route(&mut self, route: RouteFact) -> Result<(), RouteFact>;

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

    /// Settles a route-bound request after an externally observed transport failure.
    fn fail_observed(self: Box<Self>, failure: RequestError, observed_at: OutcomeStamp);
}
