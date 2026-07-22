//! Bounded FIFO registration, header inspection, and machine-approved dispatch.

use std::{collections::VecDeque, num::NonZeroUsize};

use kafka_driver_core::{CallId, CorrelationId};
use kafka_driver_transport::FrameBody;
use kafka_wire::{KafkaMessage, RequestResponsePair, ResponseHeader, response_header_version_for};
use kafka_wire_core::{ApiVersion, DecodeLimits, Decoder, KafkaDecode};

use crate::completion::CompletionSender;
#[cfg(test)]
use crate::{api::Call, completion::completion_pair};

use super::{
    CompletionDisposition, RequestError, ResponseAdmissionError, ResponseDispatch,
    ResponseDispatchError, ResponseEnvelope, ResponseFailError, ResponseFailure,
    ResponseInspectError,
    slot::{PendingResponse, TypedSlot},
};
#[cfg(test)]
use super::{FailedResponses, ResponseCloseReason};

/// Bounded typed response ownership in Kafka connection order.
pub(crate) struct ResponseRegistry {
    pub(super) slots: VecDeque<Box<dyn PendingResponse>>,
    max_pending: usize,
    decode_limits: DecodeLimits,
}

impl ResponseRegistry {
    /// Creates an empty registry with explicit slot and decode bounds.
    pub(crate) const fn new(max_pending: NonZeroUsize, decode_limits: DecodeLimits) -> Self {
        Self {
            slots: VecDeque::new(),
            max_pending: max_pending.get(),
            decode_limits,
        }
    }

    /// Atomically registers a generated request's typed response completion.
    #[cfg(test)]
    pub(crate) fn register<R>(
        &mut self,
        call_id: CallId,
        correlation_id: CorrelationId,
        version: ApiVersion,
    ) -> Result<Call<Result<R::Response, ResponseFailure>>, ResponseAdmissionError>
    where
        R: RequestResponsePair,
        R::Response: Send + 'static,
    {
        let header_version = self.validate_admission::<R>(call_id, correlation_id, version)?;
        let (receiver, completion) = completion_pair();
        self.insert_validated::<R>(call_id, correlation_id, version, header_version, completion);
        Ok(Call::new(receiver))
    }

    /// Inserts a caller-owned completion after single-owner validation.
    pub(crate) fn insert_validated<R>(
        &mut self,
        call_id: CallId,
        correlation_id: CorrelationId,
        version: ApiVersion,
        header_version: ApiVersion,
        completion: CompletionSender<Result<R::Response, ResponseFailure>>,
    ) where
        R: RequestResponsePair,
        R::Response: Send + 'static,
    {
        self.slots.push_back(Box::new(TypedSlot::<R::Response>::new(
            call_id,
            correlation_id,
            version,
            header_version,
            completion,
        )));
    }

    /// Decodes only the FIFO front's response header, retaining typed ownership.
    pub(crate) fn inspect_front(
        &self,
        frame: FrameBody,
    ) -> Result<ResponseEnvelope, ResponseInspectError> {
        let Some(slot) = self.slots.front() else {
            return Err(ResponseInspectError::NoPendingResponse { frame });
        };
        let bytes = frame.clone().into_bytes();
        let mut decoder = Decoder::new(bytes.clone(), self.decode_limits).map_err(|error| {
            ResponseInspectError::HeaderDecode {
                error,
                frame: frame.clone(),
            }
        })?;
        let header = ResponseHeader::decode(&mut decoder, slot.header_version())
            .map_err(|error| ResponseInspectError::HeaderDecode { error, frame })?;
        let body = bytes.slice(decoder.offset()..);
        Ok(ResponseEnvelope::new(
            CorrelationId::from_raw(header.correlation_id),
            body,
        ))
    }

    /// Completes the FIFO front only after the connection machine approves it.
    pub(crate) fn complete_verified(
        &mut self,
        call_id: CallId,
        correlation_id: CorrelationId,
        envelope: ResponseEnvelope,
    ) -> Result<ResponseDispatch, ResponseDispatchError> {
        let Some(front) = self.slots.front() else {
            return Err(ResponseDispatchError::NoPendingResponse { envelope });
        };
        if front.call_id() != call_id
            || front.correlation_id() != correlation_id
            || envelope.correlation_id() != correlation_id
        {
            return Err(ResponseDispatchError::VerificationMismatch {
                expected_call: front.call_id(),
                expected_correlation: front.correlation_id(),
                approved_call: call_id,
                approved_correlation: correlation_id,
                observed_correlation: envelope.correlation_id(),
                envelope,
            });
        }
        let Some(slot) = self.slots.pop_front() else {
            return Err(ResponseDispatchError::NoPendingResponse { envelope });
        };
        match slot.decode(envelope.into_body(), self.decode_limits) {
            Ok(completion) => Ok(ResponseDispatch {
                call_id,
                correlation_id,
                completion,
            }),
            Err(failure) => Err(ResponseDispatchError::BodyDecode {
                call_id,
                error: failure.error,
                completion: failure.completion,
            }),
        }
    }

    /// Fails the FIFO front only when the machine names that exact call.
    pub(crate) fn fail_verified(
        &mut self,
        call_id: CallId,
        failure: RequestError,
    ) -> Result<CompletionDisposition, ResponseFailError> {
        let Some(front) = self.slots.front() else {
            return Err(ResponseFailError::NoPendingResponse { call_id, failure });
        };
        if front.call_id() != call_id {
            return Err(ResponseFailError::VerificationMismatch {
                expected_call: front.call_id(),
                failed_call: call_id,
                failure,
            });
        }
        let Some(slot) = self.slots.pop_front() else {
            return Err(ResponseFailError::NoPendingResponse { call_id, failure });
        };
        Ok(slot.fail(failure))
    }

    /// Fails and removes every remaining slot when its connection epoch ends.
    #[cfg(test)]
    pub(crate) fn fail_all(&mut self, reason: ResponseCloseReason) -> FailedResponses {
        let mut failed = FailedResponses::default();
        while let Some(slot) = self.slots.pop_front() {
            failed.total += 1;
            if slot.fail(RequestError::ConnectionClosed(reason))
                == CompletionDisposition::ReceiverAbandoned
            {
                failed.abandoned += 1;
            }
        }
        failed
    }

    /// Returns typed response slots currently awaiting frames.
    #[cfg(test)]
    pub(crate) fn pending(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn validate_admission<R>(
        &self,
        call_id: CallId,
        correlation_id: CorrelationId,
        version: ApiVersion,
    ) -> Result<ApiVersion, ResponseAdmissionError>
    where
        R: RequestResponsePair,
    {
        if self.slots.len() >= self.max_pending {
            return Err(ResponseAdmissionError::CapacityReached {
                limit: self.max_pending,
            });
        }
        if self.slots.iter().any(|slot| slot.call_id() == call_id) {
            return Err(ResponseAdmissionError::CallInUse { call_id });
        }
        if self
            .slots
            .iter()
            .any(|slot| slot.correlation_id() == correlation_id)
        {
            return Err(ResponseAdmissionError::CorrelationInUse { correlation_id });
        }
        if !R::supports(version) {
            return Err(ResponseAdmissionError::UnsupportedVersion {
                message: R::NAME,
                version,
            });
        }
        if !R::Response::supports(version) {
            return Err(ResponseAdmissionError::UnsupportedVersion {
                message: R::Response::NAME,
                version,
            });
        }
        response_header_version_for::<R>(version)
            .map(ApiVersion::new)
            .map_err(|_| ResponseAdmissionError::UnsupportedVersion {
                message: R::NAME,
                version,
            })
    }
}

impl std::fmt::Debug for ResponseRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponseRegistry")
            .field("pending", &self.slots.len())
            .field("max_pending", &self.max_pending)
            .field("decode_limits", &self.decode_limits)
            .finish()
    }
}
