//! Generated request encoding paired atomically with its typed completion sender.

use std::time::Instant;

#[cfg(test)]
use kafka_driver_core::CorrelationId;
use kafka_driver_core::{CallId, OutcomeStamp};
use kafka_wire::{OutboundFrameLimits, RequestResponsePair};
#[cfg(test)]
use kafka_wire_core::Bytes;
use kafka_wire_core::{ApiVersion, DecodeLimits, StrBytes};

#[cfg(test)]
use crate::response::ResponseRegistry;
use crate::{
    RequestError, TrafficClass,
    api::RouteFact,
    observation::{CallOutcome, CallTimeline},
    request::{BorneraRequestPreparation, RequestCompletion, RequestPolicy},
};

use super::{ErasedRequest, footprint::retained_bytes};

pub(super) struct TypedRequest<R>
where
    R: RequestResponsePair,
{
    pub(super) call_id: CallId,
    traffic_class: TrafficClass,
    pub(super) request: R,
    policy: RequestPolicy,
    selected_version: Option<ApiVersion>,
    retained_bytes: usize,
    pub(super) completion: RequestCompletion<R::Response>,
    pub(super) lifecycle: RequestLifecycle,
}

pub(super) struct RequestLifecycle {
    pub(super) timeline: Option<CallTimeline>,
}

impl RequestLifecycle {
    pub(super) const fn unobserved() -> Self {
        Self { timeline: None }
    }

    pub(super) const fn observed(timeline: CallTimeline) -> Self {
        Self {
            timeline: Some(timeline),
        }
    }
}

impl<R> TypedRequest<R>
where
    R: RequestResponsePair,
{
    pub(super) fn new(
        call_id: CallId,
        traffic_class: TrafficClass,
        request: R,
        policy: RequestPolicy,
        completion: RequestCompletion<R::Response>,
        lifecycle: RequestLifecycle,
    ) -> Self {
        let retained_bytes = retained_bytes(&request, &completion);
        Self {
            call_id,
            traffic_class,
            request,
            policy,
            selected_version: None,
            retained_bytes,
            completion,
            lifecycle,
        }
    }
}

impl<R> ErasedRequest for TypedRequest<R>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    fn call_id(&self) -> CallId {
        self.call_id
    }

    fn api_key(&self) -> kafka_wire_core::ApiKey {
        R::API_KEY
    }

    fn traffic_class(&self) -> TrafficClass {
        self.traffic_class
    }

    fn rejects_after_route_failure(&self) -> bool {
        self.policy.rejects_after_route_failure()
    }

    fn select_version(
        &mut self,
        negotiated: kafka_driver_core::NegotiatedApi,
    ) -> Result<ApiVersion, RequestError> {
        let version = self.policy.select_version(negotiated)?;
        self.selected_version = Some(version);
        Ok(version)
    }

    fn establish_deadline(
        &mut self,
        start: kafka_driver_core::Moment,
    ) -> Result<kafka_driver_core::Moment, RequestError> {
        self.policy.establish_deadline(start)
    }

    fn retained_bytes(&self) -> usize {
        self.retained_bytes
            .saturating_add(self.completion.route_heap_bytes())
    }

    fn mark_reactor(&mut self, at: Instant) {
        if let Some(timeline) = &mut self.lifecycle.timeline {
            timeline.mark_reactor(at);
        }
    }

    fn mark_routed(&mut self, at: Instant) {
        if let Some(timeline) = &mut self.lifecycle.timeline {
            timeline.mark_routed(at);
        }
    }

    fn record_route(&mut self, route: RouteFact) -> Result<(), RouteFact> {
        self.mark_routed(Instant::now());
        self.completion.record_route(route)
    }

    #[cfg(test)]
    fn prepare(
        self: Box<Self>,
        correlation_id: CorrelationId,
        version: ApiVersion,
        client_id: Option<&StrBytes>,
        outbound_limits: OutboundFrameLimits,
        responses: &mut ResponseRegistry,
    ) -> Result<Bytes, RequestError> {
        super::typed_legacy::prepare(
            *self,
            correlation_id,
            version,
            client_id,
            outbound_limits,
            responses,
        )
    }

    fn prepare_bornera(
        self: Box<Self>,
        version: ApiVersion,
        client_id: Option<&StrBytes>,
        outbound_limits: OutboundFrameLimits,
        decode_limits: DecodeLimits,
    ) -> Result<BorneraRequestPreparation, RequestError> {
        super::typed_bornera::prepare(*self, version, client_id, outbound_limits, decode_limits)
    }

    fn fail(self: Box<Self>, failure: RequestError) {
        let delivered = self
            .completion
            .complete_unobserved(Err(failure.clone()), self.selected_version);
        if let Some(timeline) = self.lifecycle.timeline {
            timeline.finish(CallOutcome::Failed(&failure), delivered);
        }
    }

    fn fail_observed(self: Box<Self>, failure: RequestError, observed_at: OutcomeStamp) {
        let delivered = self.completion.complete_observed_failure(
            Err(failure.clone()),
            self.selected_version,
            observed_at,
        );
        if let Some(timeline) = self.lifecycle.timeline {
            timeline.finish(CallOutcome::Failed(&failure), delivered);
        }
    }
}

pub(super) fn settle_failure<T, U>(
    completion: RequestCompletion<T>,
    timeline: Option<CallTimeline>,
    failure: RequestError,
    selected_version: Option<ApiVersion>,
) -> Result<U, RequestError> {
    let delivered = completion.complete_unobserved(Err(failure.clone()), selected_version);
    if let Some(timeline) = timeline {
        timeline.finish(CallOutcome::Failed(&failure), delivered);
    }
    Err(failure)
}
