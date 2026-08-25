//! Type-erased generated response decoder and terminal completion ownership.

use std::{mem::size_of, time::Instant};

use calandria::RetainedBytes;
use kafka_driver_core::OutcomeStamp;
use kafka_wire_core::{ApiVersion, Decoder, KafkaDecode};

use crate::{
    observation::{CallOutcome, CallTimeline},
    request::{ALLOCATION_ALLOWANCE_BYTES, RequestCompletion},
};

use super::{
    CompletionDisposition, PublicResponseCompletionError, PublicResponseFailure, RequestError,
};

const RESPONSE_OWNER_ALLOCATIONS: usize = 2;

pub(super) trait TypedPublicResponse: Send {
    fn mark_prepared(&mut self, at: Instant);
    fn mark_writer(&mut self, at: Instant);
    fn decode(
        self: Box<Self>,
        decoder: Decoder,
        version: ApiVersion,
        observed_at: OutcomeStamp,
    ) -> Result<CompletionDisposition, PublicResponseCompletionError>;
    fn fail(
        self: Box<Self>,
        failure: RequestError,
        version: Option<ApiVersion>,
    ) -> CompletionDisposition;
    fn fail_observed(
        self: Box<Self>,
        failure: RequestError,
        version: ApiVersion,
        observed_at: OutcomeStamp,
    ) -> CompletionDisposition;
}

pub(super) struct TypedResponse<T> {
    completion: RequestCompletion<T>,
    timeline: Option<CallTimeline>,
}

pub(super) fn typed_response<T>(
    completion: RequestCompletion<T>,
    timeline: Option<CallTimeline>,
) -> (Box<dyn TypedPublicResponse>, RetainedBytes)
where
    T: KafkaDecode + Send + 'static,
{
    let retained = retained_charge::<T>(&completion);
    (
        Box::new(TypedResponse {
            completion,
            timeline,
        }),
        retained,
    )
}

impl<T> TypedPublicResponse for TypedResponse<T>
where
    T: KafkaDecode + Send + 'static,
{
    fn mark_prepared(&mut self, at: Instant) {
        if let Some(timeline) = &mut self.timeline {
            timeline.mark_prepared(at);
        }
    }

    fn mark_writer(&mut self, at: Instant) {
        if let Some(timeline) = &mut self.timeline {
            timeline.mark_writer(at);
        }
    }

    fn decode(
        self: Box<Self>,
        mut decoder: Decoder,
        version: ApiVersion,
        observed_at: OutcomeStamp,
    ) -> Result<CompletionDisposition, PublicResponseCompletionError> {
        let decoded = T::decode(&mut decoder, version).and_then(|value| {
            decoder.finish()?;
            Ok(value)
        });
        match decoded {
            Ok(value) => {
                let delivered = self
                    .completion
                    .complete_observed(Ok(value), version, observed_at);
                finish(self.timeline, CallOutcome::Succeeded, delivered);
                Ok(disposition(delivered))
            }
            Err(error) => {
                let failure = RequestError::Decode(error.clone());
                let completion = self.fail_observed(failure, version, observed_at);
                Err(PublicResponseCompletionError {
                    failure: PublicResponseFailure::BodyDecode(error),
                    completion,
                })
            }
        }
    }

    fn fail(
        self: Box<Self>,
        failure: RequestError,
        version: Option<ApiVersion>,
    ) -> CompletionDisposition {
        let delivered = self
            .completion
            .complete_unobserved(Err(failure.clone()), version);
        finish(self.timeline, CallOutcome::Failed(&failure), delivered);
        disposition(delivered)
    }

    fn fail_observed(
        self: Box<Self>,
        failure: RequestError,
        version: ApiVersion,
        observed_at: OutcomeStamp,
    ) -> CompletionDisposition {
        let delivered =
            self.completion
                .complete_observed(Err(failure.clone()), version, observed_at);
        finish(self.timeline, CallOutcome::Failed(&failure), delivered);
        disposition(delivered)
    }
}

fn retained_charge<T>(completion: &RequestCompletion<T>) -> RetainedBytes {
    // Context count bounds the map node and inline envelope. This charge covers
    // the erased owner box, its typed payload, the separately allocated shared
    // completion payload, route heap, and one conservative allocator allowance
    // for each owner allocation. It is captured once; binding is fixed-size.
    let allowance = ALLOCATION_ALLOWANCE_BYTES
        .checked_mul(RESPONSE_OWNER_ALLOCATIONS)
        .unwrap_or(usize::MAX);
    let bytes = size_of::<TypedResponse<T>>()
        .checked_add(completion.retained_state_bytes())
        .and_then(|bytes| bytes.checked_add(completion.route_heap_bytes()))
        .and_then(|bytes| bytes.checked_add(allowance))
        .unwrap_or(usize::MAX);
    RetainedBytes::new(u64::try_from(bytes).unwrap_or(u64::MAX))
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
