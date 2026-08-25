//! `ApiVersions` session operation and admission opening on the direct Bornera connection.

use bornera::{ConnectionCommitError, EngineCommitError, OutboundFrame, RegisteredTransport};
use bornera_core::{CloseReason, OperationOptions};
use calandria::{Deadline, RetainedBytes};
use kafka_driver_core::{
    EffectId, KafkaSessionCloseReason, KafkaSessionDeadline, KafkaSessionEffect, KafkaSessionInput,
    Moment, NegotiationFailure,
};
use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, measure_request};

use super::{
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, add, calandria_moment, message},
};
use crate::{
    negotiation::{NegotiationExchange, negotiate},
    reactor::bornera::{OperationContextKey, correlation_id},
};

const NEGOTIATION_EFFECT: EffectId = EffectId::from_raw(1);
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn transport_opened(&mut self, now: Moment) -> std::io::Result<()> {
        let deadline = add(now, self.negotiation_timeout)?;
        self.apply_session(
            KafkaSessionInput::TransportOpened {
                deadline: KafkaSessionDeadline::new(now, deadline),
            },
            now,
        )
    }

    pub(super) fn finish_negotiation(
        &mut self,
        exchange: NegotiationExchange,
        frame: crate::reactor::bornera::KafkaFrame,
        now: Moment,
    ) -> std::io::Result<()> {
        let capabilities = exchange
            .finish_bytes(frame.into_bytes())
            .map_err(|error| error.failure())
            .and_then(|response| {
                negotiate(response, self.negotiation_limits).map_err(|e| e.failure())
            });
        match capabilities {
            Ok(capabilities) => self.apply_session(
                KafkaSessionInput::ApiVersionsSucceeded { capabilities },
                now,
            ),
            Err(failure) => {
                self.apply_session(KafkaSessionInput::ApiVersionsFailed { failure }, now)
            }
        }
    }

    pub(super) fn negotiation_failed(
        &mut self,
        failure: NegotiationFailure,
        now: Moment,
    ) -> std::io::Result<()> {
        self.apply_session(KafkaSessionInput::ApiVersionsFailed { failure }, now)
    }

    pub(super) fn apply_session(
        &mut self,
        input: KafkaSessionInput,
        now: Moment,
    ) -> std::io::Result<()> {
        let effects = self.session.apply(input).into_effects();
        for effect in effects {
            match effect {
                KafkaSessionEffect::StartApiVersions { deadline } => {
                    self.start_negotiation(now, deadline)?;
                }
                KafkaSessionEffect::SessionReady => {
                    self.set.open_admission(self.connection).map_err(message)?;
                    self.mark_runnable();
                }
                KafkaSessionEffect::BeginDrain => {
                    self.admission_open = false;
                    let deadline = add(now, DRAIN_TIMEOUT)?;
                    self.set
                        .begin_drain(self.connection, Deadline::at(calandria_moment(deadline)))
                        .map_err(message)?;
                    self.mark_runnable();
                }
                KafkaSessionEffect::CloseSession { reason } => {
                    self.admission_open = false;
                    self.record_session_close(reason);
                    self.set
                        .finalize(self.connection, close_reason(reason))
                        .map_err(message)?;
                    self.mark_runnable();
                }
                KafkaSessionEffect::CancelDeadline
                | KafkaSessionEffect::RescheduleDeadline { .. } => {}
                KafkaSessionEffect::StartAuthenticationHandshake { .. }
                | KafkaSessionEffect::StartAuthenticationExchange { .. } => {
                    return Err(std::io::Error::other(
                        "plaintext direct session unexpectedly requested authentication",
                    ));
                }
            }
        }
        Ok(())
    }

    fn start_negotiation(&mut self, now: Moment, deadline: Moment) -> std::io::Result<()> {
        let version = API_VERSIONS_API_DESCRIPTOR.supported_versions.min();
        let request = ApiVersionsRequest::default();
        let client_id = self.client_id.as_ref().map(crate::config::ClientId::wire);
        let Ok(measure) = measure_request(&request, version, client_id, self.outbound_limits)
        else {
            return self.negotiation_failed(NegotiationFailure::Malformed, now);
        };
        let Ok(mut reservation) = self
            .contexts
            .reserve(DirectOperationContext::negotiation(), RetainedBytes::ZERO)
        else {
            return self.negotiation_failed(NegotiationFailure::Capacity, now);
        };
        let write_retained = RetainedBytes::try_from(measure.wire_bytes)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let options = OperationOptions::until(Deadline::at(calandria_moment(deadline)))
            .session()
            .write_retained_bytes(write_retained);
        let permit = match self
            .set
            .reserve(self.connection, calandria_moment(now), options)
        {
            Ok(permit) => permit,
            Err(error) => {
                self.observe_reserve_rejection(error, write_retained);
                drop(reservation.abort());
                return self.negotiation_failed(NegotiationFailure::Capacity, now);
            }
        };
        let correlation = correlation_id(permit.match_key()).map_err(message)?;
        let (exchange, bytes) = match NegotiationExchange::start(
            NEGOTIATION_EFFECT,
            correlation,
            client_id,
            self.outbound_limits,
            self.negotiation_limits.decode_limits(),
        ) {
            Ok(started) => started,
            Err(error) => {
                drop(permit);
                drop(reservation.abort());
                return self.negotiation_failed(error.failure(), now);
            }
        };
        if bytes.len() != measure.wire_bytes
            || !reservation.bind(|context| context.bind_negotiation(exchange))
        {
            drop(permit);
            drop(reservation.abort());
            return self.negotiation_failed(NegotiationFailure::Malformed, now);
        }
        let frame = OutboundFrame::copy_from_slice(&bytes).map_err(message)?;
        self.commit_negotiation(permit, frame, reservation, now)
    }

    fn commit_negotiation(
        &mut self,
        permit: bornera_core::OperationPermit,
        frame: OutboundFrame,
        reservation: crate::reactor::bornera::ContextReservation<DirectOperationContext>,
        now: Moment,
    ) -> std::io::Result<()> {
        let operation = match self.set.commit(self.connection, permit, frame) {
            Ok(operation) => operation,
            Err(ConnectionCommitError::Connection(EngineCommitError::AcceptedOwnerFailure {
                operation,
                ..
            })) => {
                self.publish_negotiation(operation, reservation)?;
                if self.pending_recovery.is_none() {
                    self.pending_recovery =
                        Some(self.set.try_recover(self.connection).map_err(message)?);
                }
                return Ok(());
            }
            Err(_) => {
                drop(reservation.abort());
                return self.negotiation_failed(NegotiationFailure::Capacity, now);
            }
        };
        self.publish_negotiation(operation, reservation)?;
        self.mark_runnable();
        Ok(())
    }

    fn publish_negotiation(
        &mut self,
        operation: bornera_core::OperationId,
        reservation: crate::reactor::bornera::ContextReservation<DirectOperationContext>,
    ) -> std::io::Result<()> {
        let key = OperationContextKey::new(self.connection.epoch(), operation);
        if reservation.publish(key).is_err() {
            self.pending_recovery = Some(
                self.set
                    .abandon(self.connection, bornera::OwnerFailure::OwnerInvariant)
                    .map_err(message)?,
            );
        }
        Ok(())
    }
}

const fn close_reason(reason: KafkaSessionCloseReason) -> CloseReason {
    match reason {
        KafkaSessionCloseReason::Drained => CloseReason::Drained,
        KafkaSessionCloseReason::ProtocolFailed(_) => CloseReason::MalformedReply,
        KafkaSessionCloseReason::Requested
        | KafkaSessionCloseReason::NegotiationFailed(_)
        | KafkaSessionCloseReason::AuthenticationFailed(_)
        | KafkaSessionCloseReason::TransportClosed => CloseReason::Requested,
    }
}
