//! Generated request encoding paired atomically with its typed completion sender.

use std::time::Instant;

use bytes::BytesMut;
use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire::{OutboundFrameLimits, RequestResponsePair, encode_request};
use kafka_wire_core::{ApiVersion, Bytes};

use crate::{
    RequestError, TrafficClass,
    api::RouteFact,
    observation::{CallOutcome, CallTimeline},
    request::{RequestCompletion, RequestDeadline},
    response::{ResponseAdmissionError, ResponseRegistry},
};

use super::{ErasedRequest, footprint::retained_bytes};

pub(super) struct TypedRequest<R>
where
    R: RequestResponsePair,
{
    call_id: CallId,
    traffic_class: TrafficClass,
    request: R,
    deadline: RequestDeadline,
    retained_bytes: usize,
    completion: RequestCompletion<R::Response>,
    lifecycle: RequestLifecycle,
}

pub(super) struct RequestLifecycle {
    timeline: Option<CallTimeline>,
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
        deadline: RequestDeadline,
        completion: RequestCompletion<R::Response>,
        lifecycle: RequestLifecycle,
    ) -> Self {
        let retained_bytes = retained_bytes(&request, &completion);
        Self {
            call_id,
            traffic_class,
            request,
            deadline,
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

    fn establish_deadline(
        &mut self,
        start: kafka_driver_core::Moment,
    ) -> Result<kafka_driver_core::Moment, RequestError> {
        self.deadline.establish(start)
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

    fn prepare(
        self: Box<Self>,
        correlation_id: CorrelationId,
        version: ApiVersion,
        outbound_limits: OutboundFrameLimits,
        responses: &mut ResponseRegistry,
    ) -> Result<Bytes, RequestError> {
        let Self {
            call_id,
            request,
            completion,
            lifecycle,
            ..
        } = *self;
        let mut timeline = lifecycle.timeline;
        let header_version =
            match responses.validate_admission::<R>(call_id, correlation_id, version) {
                Ok(header_version) => header_version,
                Err(source) => {
                    return fail(completion, timeline, admission_failure(source));
                }
            };
        let mut frame = BytesMut::new();
        if let Err(source) = encode_request(
            &mut frame,
            correlation_id.get(),
            None,
            &request,
            version,
            outbound_limits,
        ) {
            return fail(completion, timeline, RequestError::Encode(source));
        }
        if let Some(timeline) = &mut timeline {
            timeline.mark_prepared(Instant::now());
        }
        responses.insert_validated::<R>(
            call_id,
            correlation_id,
            version,
            header_version,
            completion,
            timeline,
        );
        Ok(frame.freeze())
    }

    fn fail(self: Box<Self>, failure: RequestError) {
        let delivered = self.completion.complete_unobserved(Err(failure.clone()));
        if let Some(timeline) = self.lifecycle.timeline {
            timeline.finish(CallOutcome::Failed(&failure), delivered);
        }
    }
}

fn fail<T>(
    completion: RequestCompletion<T>,
    timeline: Option<CallTimeline>,
    failure: RequestError,
) -> Result<Bytes, RequestError> {
    let delivered = completion.complete_unobserved(Err(failure.clone()));
    if let Some(timeline) = timeline {
        timeline.finish(CallOutcome::Failed(&failure), delivered);
    }
    Err(failure)
}

const fn admission_failure(source: ResponseAdmissionError) -> RequestError {
    match source {
        ResponseAdmissionError::CapacityReached { limit } => {
            RequestError::ResponseCapacityReached { limit }
        }
        ResponseAdmissionError::UnsupportedVersion { message, version } => {
            RequestError::UnsupportedVersion { message, version }
        }
        ResponseAdmissionError::CallInUse { .. }
        | ResponseAdmissionError::CorrelationInUse { .. } => RequestError::IdentityConflict,
    }
}
