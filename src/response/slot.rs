//! Type-erased FIFO ownership with generated response decoding at completion.

use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire_core::{ApiVersion, Bytes, DecodeError, DecodeLimits, Decoder, KafkaDecode};

use crate::completion::CompletionSender;

use super::{CompletionDisposition, ResponseCloseReason, ResponseFailure};

pub(super) trait PendingResponse {
    fn call_id(&self) -> CallId;
    fn correlation_id(&self) -> CorrelationId;
    fn header_version(&self) -> ApiVersion;
    fn decode(
        self: Box<Self>,
        body: Bytes,
        limits: DecodeLimits,
    ) -> Result<CompletionDisposition, SlotDecodeError>;
    fn fail(self: Box<Self>, reason: ResponseCloseReason) -> CompletionDisposition;
}

pub(super) struct TypedSlot<T> {
    call_id: CallId,
    correlation_id: CorrelationId,
    version: ApiVersion,
    header_version: ApiVersion,
    completion: CompletionSender<Result<T, ResponseFailure>>,
}

impl<T> TypedSlot<T> {
    pub(super) const fn new(
        call_id: CallId,
        correlation_id: CorrelationId,
        version: ApiVersion,
        header_version: ApiVersion,
        completion: CompletionSender<Result<T, ResponseFailure>>,
    ) -> Self {
        Self {
            call_id,
            correlation_id,
            version,
            header_version,
            completion,
        }
    }
}

impl<T> PendingResponse for TypedSlot<T>
where
    T: KafkaDecode + 'static,
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

    fn decode(
        self: Box<Self>,
        body: Bytes,
        limits: DecodeLimits,
    ) -> Result<CompletionDisposition, SlotDecodeError> {
        let decoded = Decoder::new(body, limits).and_then(|mut decoder| {
            let response = T::decode(&mut decoder, self.version)?;
            decoder.finish()?;
            Ok(response)
        });
        match decoded {
            Ok(response) => Ok(disposition(self.completion.complete(Ok(response)).is_ok())),
            Err(error) => {
                let completion = disposition(
                    self.completion
                        .complete(Err(ResponseFailure::Decode(error.clone())))
                        .is_ok(),
                );
                Err(SlotDecodeError { error, completion })
            }
        }
    }

    fn fail(self: Box<Self>, reason: ResponseCloseReason) -> CompletionDisposition {
        disposition(
            self.completion
                .complete(Err(ResponseFailure::ConnectionClosed(reason)))
                .is_ok(),
        )
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
