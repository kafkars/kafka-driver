//! Public typed response completion owned by one Bornera operation context.

use std::{fmt, time::Instant};

use calandria::RetainedBytes;
use kafka_driver_core::{CallFailure, CallId, CorrelationId, Delivery, OutcomeStamp};
use kafka_wire::ResponseHeader;
use kafka_wire_core::{ApiVersion, Bytes, DecodeError, DecodeLimits, Decoder, KafkaDecode};

use crate::{observation::CallTimeline, request::RequestCompletion};

use super::{
    CompletionDisposition, RequestError,
    context_owner::{TypedPublicResponse, typed_response},
};

/// Semantic state retained from request preparation through typed completion.
pub(crate) struct PublicResponseContext {
    call_id: CallId,
    selected_version: ApiVersion,
    header_version: ApiVersion,
    expected_correlation: Option<CorrelationId>,
    decode_limits: DecodeLimits,
    retained_bytes: RetainedBytes,
    response: Box<dyn TypedPublicResponse>,
}

impl PublicResponseContext {
    pub(crate) fn new<T>(
        call_id: CallId,
        selected_version: ApiVersion,
        header_version: ApiVersion,
        decode_limits: DecodeLimits,
        completion: RequestCompletion<T>,
        timeline: Option<CallTimeline>,
    ) -> Self
    where
        T: KafkaDecode + Send + 'static,
    {
        let (response, retained_bytes) = typed_response(completion, timeline);
        Self {
            call_id,
            selected_version,
            header_version,
            expected_correlation: None,
            decode_limits,
            retained_bytes,
            response,
        }
    }

    pub(crate) const fn call_id(&self) -> CallId {
        self.call_id
    }

    #[cfg(test)]
    pub(crate) const fn selected_version(&self) -> ApiVersion {
        self.selected_version
    }

    #[cfg(test)]
    pub(crate) const fn header_version(&self) -> ApiVersion {
        self.header_version
    }

    #[cfg(test)]
    pub(crate) const fn expected_correlation(&self) -> Option<CorrelationId> {
        self.expected_correlation
    }

    pub(crate) const fn retained_bytes(&self) -> RetainedBytes {
        self.retained_bytes
    }

    pub(crate) fn bind_correlation(&mut self, correlation: CorrelationId) -> bool {
        if self.expected_correlation.is_some() {
            return false;
        }
        self.expected_correlation = Some(correlation);
        true
    }

    pub(crate) fn mark_prepared(&mut self, at: Instant) {
        self.response.mark_prepared(at);
    }

    pub(crate) fn mark_writer(&mut self, at: Instant) {
        self.response.mark_writer(at);
    }

    pub(crate) fn complete(
        self,
        frame: Bytes,
        observed_at: OutcomeStamp,
    ) -> Result<CompletionDisposition, PublicResponseCompletionError> {
        let Some(expected) = self.expected_correlation else {
            return Err(self.reject(
                PublicResponseFailure::UnboundCorrelation,
                RequestError::IdentityConflict,
                observed_at,
            ));
        };
        let mut decoder = match Decoder::new(frame, self.decode_limits) {
            Ok(decoder) => decoder,
            Err(error) => return Err(self.decode_rejection(error, observed_at)),
        };
        let header = match ResponseHeader::decode(&mut decoder, self.header_version) {
            Ok(header) => header,
            Err(error) => return Err(self.decode_rejection(error, observed_at)),
        };
        let received = CorrelationId::from_raw(header.correlation_id);
        if received != expected {
            let failure = RequestError::Rejected {
                failure: CallFailure::CorrelationMismatch { expected, received },
                delivery: Delivery::PossiblySent,
            };
            return Err(self.reject(
                PublicResponseFailure::CorrelationMismatch { expected, received },
                failure,
                observed_at,
            ));
        }
        self.response
            .decode(decoder, self.selected_version, observed_at)
    }

    pub(crate) fn fail(self, failure: RequestError) -> CompletionDisposition {
        self.response.fail(failure, Some(self.selected_version))
    }

    pub(crate) fn fail_observed(
        self,
        failure: RequestError,
        observed_at: OutcomeStamp,
    ) -> CompletionDisposition {
        self.response
            .fail_observed(failure, self.selected_version, observed_at)
    }

    fn decode_rejection(
        self,
        error: DecodeError,
        observed_at: OutcomeStamp,
    ) -> PublicResponseCompletionError {
        self.reject(
            PublicResponseFailure::HeaderDecode(error.clone()),
            RequestError::Decode(error),
            observed_at,
        )
    }

    fn reject(
        self,
        failure: PublicResponseFailure,
        request_error: RequestError,
        observed_at: OutcomeStamp,
    ) -> PublicResponseCompletionError {
        let completion = self.fail_observed(request_error, observed_at);
        PublicResponseCompletionError {
            failure,
            completion,
        }
    }
}

impl fmt::Debug for PublicResponseContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublicResponseContext")
            .field("call_id", &self.call_id)
            .field("selected_version", &self.selected_version)
            .field("header_version", &self.header_version)
            .field("expected_correlation", &self.expected_correlation)
            .field("retained_bytes", &self.retained_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicResponseFailure {
    UnboundCorrelation,
    HeaderDecode(DecodeError),
    CorrelationMismatch {
        expected: CorrelationId,
        received: CorrelationId,
    },
    BodyDecode(DecodeError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicResponseCompletionError {
    pub(crate) failure: PublicResponseFailure,
    pub(crate) completion: CompletionDisposition,
}
