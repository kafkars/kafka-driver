//! Measured request bytes separated from affine Bornera response ownership.

use std::{fmt, time::Instant};

use bornera::{OutboundFrame, OutboundFrameError};
use bytes::BytesMut;
use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire::{OutboundFrameLimits, RequestFrameMeasure, RequestResponsePair, encode_request};
use kafka_wire_core::{ApiVersion, EncodeError, StrBytes};

use crate::{RequestError, response::PublicResponseContext};

/// One measured request awaiting context reservation and a Bornera permit key.
pub(crate) struct BorneraRequestPreparation {
    measure: RequestFrameMeasure,
    encoder: BorneraFrameEncoder,
    context: PublicResponseContext,
}

impl BorneraRequestPreparation {
    pub(super) fn new<R>(
        call_id: CallId,
        request: R,
        version: ApiVersion,
        client_id: Option<StrBytes>,
        limits: OutboundFrameLimits,
        measure: RequestFrameMeasure,
        context: PublicResponseContext,
    ) -> Self
    where
        R: RequestResponsePair + Send + 'static,
    {
        Self {
            measure,
            encoder: BorneraFrameEncoder {
                call_id,
                measure,
                request: Box::new(TypedFrameEncoder {
                    request,
                    version,
                    client_id,
                    limits,
                }),
            },
            context,
        }
    }

    pub(crate) const fn measure(&self) -> RequestFrameMeasure {
        self.measure
    }

    pub(crate) const fn context_retained_bytes(&self) -> calandria::RetainedBytes {
        self.context.retained_bytes()
    }

    pub(crate) fn into_parts(self) -> (BorneraFrameEncoder, PublicResponseContext) {
        (self.encoder, self.context)
    }
}

impl fmt::Debug for BorneraRequestPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BorneraRequestPreparation")
            .field("measure", &self.measure)
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

/// Deferred generated encoder that cannot produce bytes before correlation binding.
pub(crate) struct BorneraFrameEncoder {
    call_id: CallId,
    measure: RequestFrameMeasure,
    request: Box<dyn PreparedFrame>,
}

impl BorneraFrameEncoder {
    pub(crate) fn bind_and_encode(
        self,
        correlation: CorrelationId,
        context: &mut PublicResponseContext,
    ) -> Result<OutboundFrame, RequestError> {
        if context.call_id() != self.call_id || !context.bind_correlation(correlation) {
            return Err(RequestError::IdentityConflict);
        }
        let encoded = self
            .request
            .encode(correlation, self.measure.wire_bytes)
            .map_err(RequestError::Encode)?;
        if encoded.len() != self.measure.wire_bytes {
            return Err(RequestError::Encode(EncodeError::SizeMismatch {
                predicted: self.measure.wire_bytes,
                actual: encoded.len(),
            }));
        }
        // BytesMut capacity is not a retention contract. This exact copy is.
        let frame = OutboundFrame::copy_from_slice(&encoded)
            .map_err(|error| frame_error(error, self.measure.wire_bytes))?;
        context.mark_prepared(Instant::now());
        Ok(frame)
    }
}

impl fmt::Debug for BorneraFrameEncoder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BorneraFrameEncoder")
            .field("call_id", &self.call_id)
            .field("measure", &self.measure)
            .finish_non_exhaustive()
    }
}

trait PreparedFrame: Send {
    fn encode(
        self: Box<Self>,
        correlation: CorrelationId,
        wire_bytes: usize,
    ) -> Result<BytesMut, EncodeError>;
}

struct TypedFrameEncoder<R> {
    request: R,
    version: ApiVersion,
    client_id: Option<StrBytes>,
    limits: OutboundFrameLimits,
}

impl<R> PreparedFrame for TypedFrameEncoder<R>
where
    R: RequestResponsePair + Send + 'static,
{
    fn encode(
        self: Box<Self>,
        correlation: CorrelationId,
        wire_bytes: usize,
    ) -> Result<BytesMut, EncodeError> {
        let mut encoded = BytesMut::with_capacity(wire_bytes);
        encode_request(
            &mut encoded,
            correlation.get(),
            self.client_id,
            &self.request,
            self.version,
            self.limits,
        )?;
        Ok(encoded)
    }
}

fn frame_error(error: OutboundFrameError, wire_bytes: usize) -> RequestError {
    let bytes = match error {
        OutboundFrameError::Underreported { visible, .. } => {
            usize::try_from(visible.get()).unwrap_or(usize::MAX)
        }
        _ => wire_bytes,
    };
    RequestError::Encode(EncodeError::FrameTooLarge { bytes })
}
