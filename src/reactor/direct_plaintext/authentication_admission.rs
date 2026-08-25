//! Measure-first SASL session operations admitted through Bornera.

use bornera::{OutboundFrame, RegisteredTransport};
use bornera_core::{OperationOptions, OperationPermit};
use calandria::{Deadline, RetainedBytes};
use kafka_driver_core::{
    AuthenticationFailure, AuthenticationRound, CorrelationId, EffectId, Moment, SaslMechanism,
};
use kafka_wire::{SaslAuthenticateRequest, SaslHandshakeRequest, measure_request};
use kafka_wire_core::{ApiVersion, Bytes, StrBytes};

use crate::{
    authentication::{
        AuthenticateExchange, AuthenticationExchange, AuthenticationExchangeError,
        HandshakeExchange,
    },
    reactor::bornera::{ContextReservation, correlation_id},
};

use super::{
    authentication_reserve::{AuthenticationReserveDisposition, reserve_disposition},
    authentication_settlement::AuthenticationStageOwner,
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, calandria_moment, message},
};

const HANDSHAKE_EFFECT: EffectId = EffectId::from_raw(2);

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn start_authentication_handshake(
        &mut self,
        mechanism: SaslMechanism,
        version: ApiVersion,
        now: Moment,
        deadline: Moment,
    ) -> std::io::Result<()> {
        let stage = AuthenticationStageOwner::Handshake;
        let mut request = SaslHandshakeRequest::default();
        request.mechanism = StrBytes::from(mechanism.name());
        let client_id = self
            .client_id
            .as_ref()
            .map(crate::config::ClientId::wire)
            .cloned();
        let measure =
            match measure_request(&request, version, client_id.as_ref(), self.outbound_limits) {
                Ok(measure) => measure,
                Err(error) => {
                    return self.fail_authentication_stage(
                        stage,
                        AuthenticationExchangeError::from(error).failure(),
                        now,
                    );
                }
            };
        let Some((mut reservation, permit, correlation)) =
            self.reserve_authentication(measure.wire_bytes, now, deadline, stage)?
        else {
            return Ok(());
        };
        let started = HandshakeExchange::start(
            HANDSHAKE_EFFECT,
            correlation,
            mechanism,
            version,
            client_id.as_ref(),
            self.outbound_limits,
            self.negotiation_limits.decode_limits(),
        );
        let (exchange, bytes) = match started {
            Ok(started) => started,
            Err(error) => {
                drop(permit);
                drop(reservation.abort());
                return self.fail_authentication_stage(stage, error.failure(), now);
            }
        };
        if bytes.len() != measure.wire_bytes
            || !reservation.bind(|context| {
                context.bind_authentication(AuthenticationExchange::Handshake(exchange))
            })
        {
            drop(permit);
            drop(reservation.abort());
            return self.fail_authentication_stage(stage, AuthenticationFailure::Malformed, now);
        }
        let frame = OutboundFrame::copy_from_slice(&bytes).map_err(message)?;
        self.commit_authentication(permit, frame, reservation, stage, now)
    }

    pub(super) fn start_authentication_exchange(
        &mut self,
        round: AuthenticationRound,
        version: ApiVersion,
        now: Moment,
        deadline: Moment,
    ) -> std::io::Result<()> {
        let stage = AuthenticationStageOwner::Exchange(round);
        let authentication_message = match self.authentication_session.as_mut() {
            Some(session) => session.next_message(self.outbound_limits.max_frame_bytes()),
            None => {
                return self.fail_authentication_stage(stage, AuthenticationFailure::Protocol, now);
            }
        };
        let authentication_message = match authentication_message {
            Ok(message) => message,
            Err(failure) => return self.fail_authentication_stage(stage, failure, now),
        };
        let mut request = SaslAuthenticateRequest::default();
        request.auth_bytes = Bytes::copy_from_slice(authentication_message.as_bytes());
        let client_id = self
            .client_id
            .as_ref()
            .map(crate::config::ClientId::wire)
            .cloned();
        let measure =
            match measure_request(&request, version, client_id.as_ref(), self.outbound_limits) {
                Ok(measure) => measure,
                Err(error) => {
                    return self.fail_authentication_stage(
                        stage,
                        AuthenticationExchangeError::from(error).failure(),
                        now,
                    );
                }
            };
        let Some((mut reservation, permit, correlation)) =
            self.reserve_authentication(measure.wire_bytes, now, deadline, stage)?
        else {
            return Ok(());
        };
        let started = AuthenticateExchange::start_prepared(
            exchange_effect(round),
            round,
            correlation,
            version,
            &request,
            client_id.as_ref(),
            self.outbound_limits,
            self.negotiation_limits.decode_limits(),
        );
        let (exchange, bytes) = match started {
            Ok(started) => started,
            Err(error) => {
                drop(permit);
                drop(reservation.abort());
                return self.fail_authentication_stage(stage, error.failure(), now);
            }
        };
        if bytes.len() != measure.wire_bytes
            || !reservation.bind(|context| {
                context.bind_authentication(AuthenticationExchange::Authenticate(exchange))
            })
        {
            drop(permit);
            drop(reservation.abort());
            return self.fail_authentication_stage(stage, AuthenticationFailure::Malformed, now);
        }
        let frame = OutboundFrame::copy_from_slice(&bytes).map_err(message)?;
        self.commit_authentication(permit, frame, reservation, stage, now)
    }

    fn reserve_authentication(
        &mut self,
        wire_bytes: usize,
        now: Moment,
        deadline: Moment,
        stage: AuthenticationStageOwner,
    ) -> std::io::Result<Option<AuthenticationAdmission>> {
        let reservation = match self.contexts.reserve(
            DirectOperationContext::authentication(),
            RetainedBytes::ZERO,
        ) {
            Ok(reservation) => reservation,
            Err(error) => {
                drop(error.into_context());
                self.fail_authentication_stage(stage, AuthenticationFailure::LocalCapacity, now)?;
                return Ok(None);
            }
        };
        let Ok(write_retained) = RetainedBytes::try_from(wire_bytes) else {
            drop(reservation.abort());
            self.fail_authentication_stage(stage, AuthenticationFailure::LocalCapacity, now)?;
            return Ok(None);
        };
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
                match reserve_disposition(error) {
                    AuthenticationReserveDisposition::Fail(failure) => {
                        self.fail_authentication_stage(stage, failure, now)?;
                    }
                    AuthenticationReserveDisposition::Lifecycle => {}
                    AuthenticationReserveDisposition::Recover => {
                        if self.pending_recovery.is_none() {
                            self.pending_recovery =
                                Some(self.set.try_recover(self.connection).map_err(message)?);
                        }
                    }
                }
                return Ok(None);
            }
        };
        let Ok(correlation) = correlation_id(permit.match_key()) else {
            drop(permit);
            drop(reservation.abort());
            self.fail_authentication_stage(stage, AuthenticationFailure::Malformed, now)?;
            return Ok(None);
        };
        Ok(Some((reservation, permit, correlation)))
    }
}

type AuthenticationAdmission = (
    ContextReservation<DirectOperationContext>,
    OperationPermit,
    CorrelationId,
);

const fn exchange_effect(round: AuthenticationRound) -> EffectId {
    EffectId::from_raw(2 + round.get() as u64)
}
