//! Generated request encoding paired atomically with its typed completion sender.

use std::time::Duration;

use bytes::BytesMut;
use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire::{OutboundFrameLimits, RequestResponsePair, encode_request};
use kafka_wire_core::{ApiVersion, Bytes};

use crate::{
    Call, RequestError, RouteReceipt, RoutedCall, TrafficClass,
    api::{RouteReceiptWriter, route_receipt_pair},
    completion::{CompletionSender, completion_pair},
    request::RequestDeadline,
    response::{ResponseAdmissionError, ResponseRegistry},
};

use super::ErasedRequest;

pub(crate) type ErasedRequestPair<T> = (Call<Result<T, RequestError>>, Box<dyn ErasedRequest>);
pub(crate) type RoutedRequestPair<T> = (RoutedCall<T>, Box<dyn ErasedRequest>);

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
    let retained_bytes = retained_bytes(&request);
    let request = TypedRequest {
        call_id,
        traffic_class,
        request,
        deadline: RequestDeadline::new(timeout),
        retained_bytes,
        completion,
        receipt: None,
    };
    (Call::new(receiver), Box::new(request))
}

pub(crate) fn routed_request_in<R>(
    call_id: CallId,
    traffic_class: TrafficClass,
    request: R,
    timeout: Duration,
) -> RoutedRequestPair<R::Response>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    let (receiver, completion) = completion_pair();
    let (receipt, writer) = route_receipt_pair();
    let retained_bytes = retained_bytes(&request);
    let request = TypedRequest {
        call_id,
        traffic_class,
        request,
        deadline: RequestDeadline::new(timeout),
        retained_bytes,
        completion,
        receipt: Some(writer),
    };
    (
        RoutedCall::new(Call::new(receiver), receipt),
        Box::new(request),
    )
}

struct TypedRequest<R>
where
    R: RequestResponsePair,
{
    call_id: CallId,
    traffic_class: TrafficClass,
    request: R,
    deadline: RequestDeadline,
    retained_bytes: usize,
    completion: CompletionSender<Result<R::Response, RequestError>>,
    receipt: Option<RouteReceiptWriter>,
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
    }

    fn record_route(&mut self, receipt: RouteReceipt) -> Result<(), RouteReceipt> {
        self.receipt
            .as_ref()
            .map_or(Ok(()), |writer| writer.publish(receipt))
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
            ..
        } = *self;
        let header_version =
            match responses.validate_admission::<R>(call_id, correlation_id, version) {
                Ok(header_version) => header_version,
                Err(source) => return fail(completion, admission_failure(source)),
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
            return fail(completion, RequestError::Encode(source));
        }
        responses.insert_validated::<R>(
            call_id,
            correlation_id,
            version,
            header_version,
            completion,
        );
        Ok(frame.freeze())
    }

    fn fail(self: Box<Self>, failure: RequestError) {
        drop(self.completion.complete(Err(failure)));
    }
}

fn retained_bytes<R>(request: &R) -> usize
where
    R: RequestResponsePair,
{
    let descriptor = R::API_DESCRIPTOR;
    let version = descriptor
        .latest_stable_version()
        .unwrap_or(descriptor.supported_versions.max());
    request
        .encoded_len(version)
        .ok()
        .and_then(|encoded| encoded.checked_add(size_of::<TypedRequest<R>>()))
        .unwrap_or(usize::MAX)
}

fn fail<T>(
    completion: CompletionSender<Result<T, RequestError>>,
    failure: RequestError,
) -> Result<Bytes, RequestError> {
    drop(completion.complete(Err(failure.clone())));
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
