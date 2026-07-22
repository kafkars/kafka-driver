//! Generated request encoding paired atomically with its typed completion sender.

use std::time::Duration;

use bytes::BytesMut;
use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire::{RequestResponsePair, encode_request};
use kafka_wire_core::{ApiVersion, Bytes};

use crate::{
    Call, RequestError,
    completion::{CompletionSender, completion_pair},
    response::{ResponseAdmissionError, ResponseRegistry},
};

use super::ErasedRequest;

pub(crate) type ErasedRequestPair<T> = (Call<Result<T, RequestError>>, Box<dyn ErasedRequest>);

pub(crate) fn erased_request<R>(
    call_id: CallId,
    request: R,
    version: ApiVersion,
    timeout: Duration,
) -> ErasedRequestPair<R::Response>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    let (receiver, completion) = completion_pair();
    let request = TypedRequest {
        call_id,
        request,
        version,
        timeout,
        completion,
    };
    (Call::new(receiver), Box::new(request))
}

struct TypedRequest<R>
where
    R: RequestResponsePair,
{
    call_id: CallId,
    request: R,
    version: ApiVersion,
    timeout: Duration,
    completion: CompletionSender<Result<R::Response, RequestError>>,
}

impl<R> ErasedRequest for TypedRequest<R>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    fn call_id(&self) -> CallId {
        self.call_id
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn prepare(
        self: Box<Self>,
        correlation_id: CorrelationId,
        responses: &mut ResponseRegistry,
    ) -> Result<Bytes, RequestError> {
        let Self {
            call_id,
            request,
            version,
            completion,
            ..
        } = *self;
        if let Err(source) = responses.validate_admission::<R>(call_id, correlation_id, version) {
            return fail(completion, admission_failure(source));
        }
        let mut frame = BytesMut::new();
        if let Err(source) =
            encode_request(&mut frame, correlation_id.get(), None, &request, version)
        {
            return fail(completion, RequestError::Encode(source));
        }
        responses.insert_validated::<R>(call_id, correlation_id, version, completion);
        Ok(frame.freeze())
    }

    fn fail(self: Box<Self>, failure: RequestError) {
        drop(self.completion.complete(Err(failure)));
    }
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
