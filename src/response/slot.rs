//! Type-erased FIFO ownership with generated response decoding at completion.

use std::time::Instant;

use kafka_driver_core::{CallId, CorrelationId, OutcomeStamp};
use kafka_wire_core::{ApiVersion, Bytes, DecodeError, DecodeLimits, Decoder, KafkaDecode};

use crate::{
    observation::{CallOutcome, CallTimeline},
    request::RequestCompletion,
};

use super::{CompletionDisposition, RequestError, ResponseFailure};

pub(super) trait PendingResponse: Send {
    fn call_id(&self) -> CallId;
    fn correlation_id(&self) -> CorrelationId;
    fn header_version(&self) -> ApiVersion;
    fn mark_writer(&mut self, at: Instant);
    fn decode(
        self: Box<Self>,
        body: Bytes,
        limits: DecodeLimits,
        observed_at: OutcomeStamp,
    ) -> Result<CompletionDisposition, SlotDecodeError>;
    fn fail(self: Box<Self>, failure: RequestError) -> CompletionDisposition;
}

pub(super) struct TypedSlot<T> {
    call_id: CallId,
    correlation_id: CorrelationId,
    version: ApiVersion,
    header_version: ApiVersion,
    completion: RequestCompletion<T>,
    timeline: Option<CallTimeline>,
}

impl<T> TypedSlot<T> {
    pub(super) const fn new(
        call_id: CallId,
        correlation_id: CorrelationId,
        version: ApiVersion,
        header_version: ApiVersion,
        completion: RequestCompletion<T>,
        timeline: Option<CallTimeline>,
    ) -> Self {
        Self {
            call_id,
            correlation_id,
            version,
            header_version,
            completion,
            timeline,
        }
    }
}

impl<T> PendingResponse for TypedSlot<T>
where
    T: KafkaDecode + Send + 'static,
{
    fn call_id(&self) -> CallId {
        self.call_id
    }

    fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }

    fn header_version(&self) -> ApiVersion {
        self.header_version
    }

    fn mark_writer(&mut self, at: Instant) {
        if let Some(timeline) = &mut self.timeline {
            timeline.mark_writer(at);
        }
    }

    fn decode(
        self: Box<Self>,
        body: Bytes,
        limits: DecodeLimits,
        observed_at: OutcomeStamp,
    ) -> Result<CompletionDisposition, SlotDecodeError> {
        let Self {
            version,
            completion,
            timeline,
            ..
        } = *self;
        let decoded = Decoder::new(body, limits).and_then(|mut decoder| {
            let response = T::decode(&mut decoder, version)?;
            decoder.finish()?;
            Ok(response)
        });
        match decoded {
            Ok(response) => {
                let delivered = completion.complete_observed(Ok(response), observed_at);
                finish(timeline, CallOutcome::Succeeded, delivered);
                Ok(disposition(delivered))
            }
            Err(error) => {
                let failure = ResponseFailure::Decode(error.clone());
                let delivered = completion.complete_observed(Err(failure.clone()), observed_at);
                finish(timeline, CallOutcome::Failed(&failure), delivered);
                Err(SlotDecodeError {
                    error,
                    completion: disposition(delivered),
                })
            }
        }
    }

    fn fail(self: Box<Self>, failure: RequestError) -> CompletionDisposition {
        let delivered = self.completion.complete_unobserved(Err(failure.clone()));
        finish(self.timeline, CallOutcome::Failed(&failure), delivered);
        disposition(delivered)
    }
}

pub(super) struct SlotDecodeError {
    pub(super) error: DecodeError,
    pub(super) completion: CompletionDisposition,
}

const fn disposition(delivered: bool) -> CompletionDisposition {
    if delivered {
        CompletionDisposition::Delivered
    } else {
        CompletionDisposition::ReceiverAbandoned
    }
}

fn finish(timeline: Option<CallTimeline>, outcome: CallOutcome<'_>, delivered: bool) {
    if let Some(timeline) = timeline {
        timeline.finish(outcome, delivered);
    }
}
