//! Typed request construction with optional public lifecycle observation.

use std::time::Duration;

use kafka_driver_core::CallId;
use kafka_wire::RequestResponsePair;

use crate::{
    Call, RequestError, RoutedCall, TrafficClass, completion::completion_pair,
    observation::CallTimeline,
};

use super::{
    ErasedRequest, RequestCompletion, RequestDeadline,
    typed::{RequestLifecycle, TypedRequest},
};

pub(crate) type ErasedRequestPair<T> = (Call<Result<T, RequestError>>, Box<dyn ErasedRequest>);
pub(crate) type RoutedRequestPair<T> = (RoutedCall<T>, Box<dyn ErasedRequest>);

#[cfg(test)]
pub(crate) fn erased_request<R>(
    call_id: CallId,
    request: R,
    timeout: Duration,
) -> ErasedRequestPair<R::Response>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    erased_request_in(call_id, TrafficClass::Interactive, request, timeout)
}

pub(crate) fn erased_request_in<R>(
    call_id: CallId,
    traffic_class: TrafficClass,
    request: R,
    timeout: Duration,
) -> ErasedRequestPair<R::Response>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    let (receiver, completion) = completion_pair();
    let request = TypedRequest::new(
        call_id,
        traffic_class,
        request,
        RequestDeadline::new(timeout),
        RequestCompletion::plain(completion),
        RequestLifecycle::unobserved(),
    );
    (Call::new(receiver), Box::new(request))
}

pub(crate) fn observed_request<R>(
    call_id: CallId,
    request: R,
    timeout: Duration,
    timeline: CallTimeline,
) -> ErasedRequestPair<R::Response>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    observed_request_in(
        call_id,
        TrafficClass::Interactive,
        request,
        timeout,
        timeline,
    )
}

pub(crate) fn observed_request_in<R>(
    call_id: CallId,
    traffic_class: TrafficClass,
    request: R,
    timeout: Duration,
    timeline: CallTimeline,
) -> ErasedRequestPair<R::Response>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    let (receiver, completion) = completion_pair();
    let request = TypedRequest::new(
        call_id,
        traffic_class,
        request,
        RequestDeadline::new(timeout),
        RequestCompletion::plain(completion),
        RequestLifecycle::observed(timeline),
    );
    (Call::new(receiver), Box::new(request))
}

pub(crate) fn observed_routed_request_in<R>(
    call_id: CallId,
    traffic_class: TrafficClass,
    request: R,
    timeout: Duration,
    timeline: CallTimeline,
) -> RoutedRequestPair<R::Response>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    let (receiver, completion) = completion_pair();
    let request = TypedRequest::new(
        call_id,
        traffic_class,
        request,
        RequestDeadline::new(timeout),
        RequestCompletion::routed(completion),
        RequestLifecycle::observed(timeline),
    );
    (RoutedCall::new(Call::new(receiver)), Box::new(request))
}
