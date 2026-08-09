//! Ordered interpretation of secret-free authentication child effects.

use kafka_driver_core::{
    AuthenticationEffect, AuthenticationFailure, AuthenticationRound, ConnectionEffect,
    ConnectionPhase, EffectId,
};

use crate::{
    authentication::{AuthenticateExchange, AuthenticationExchange, HandshakeExchange},
    reactor::{
        Poller,
        resource::ResourceIdentity,
        timer::{DeadlineTimer, TimerScheduleError},
    },
};

use super::super::{BrokerError, owner::SingleBroker};
use super::write::AuthenticationWriteOutcome;

impl SingleBroker {
    pub(super) fn interpret_authentication_effects(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effects: Vec<ConnectionEffect>,
    ) -> Result<(), BrokerError> {
        let mut terminal = Vec::new();
        for effect in effects {
            match effect {
                ConnectionEffect::CancelDeadline { timer_id } if terminal.is_empty() => {
                    self.timers.cancel(timer_id);
                }
                ConnectionEffect::Authentication { effect } if terminal.is_empty() => {
                    if !self.interpret_authentication_effect(poller, identity, effect)? {
                        return Ok(());
                    }
                }
                other => terminal.push(other),
            }
        }
        if !terminal.is_empty() {
            self.interpret_close(poller, terminal, None)?;
        }
        if self.connection.state().phase() == ConnectionPhase::Ready {
            self.finish_authentication(poller, identity)?;
        }
        Ok(())
    }

    fn interpret_authentication_effect(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effect: AuthenticationEffect,
    ) -> Result<bool, BrokerError> {
        match effect {
            AuthenticationEffect::ScheduleDeadline {
                epoch,
                timer_id,
                at,
            } => {
                let deadline = DeadlineTimer::for_authentication(timer_id, epoch, at);
                if let Err(error) = self.timers.schedule(deadline) {
                    if matches!(error, TimerScheduleError::CapacityReached { .. }) {
                        self.fail_active_authentication(
                            poller,
                            identity,
                            AuthenticationFailure::LocalCapacity,
                        )?;
                        return Ok(false);
                    }
                    return Err(error.into());
                }
            }
            AuthenticationEffect::SendHandshake {
                epoch,
                transport_id,
                effect_id,
                correlation_id,
                mechanism,
                version,
            } => {
                if epoch != identity.epoch() || transport_id != identity.transport_id() {
                    return Err(BrokerError::MissingEffect);
                }
                let exchange = HandshakeExchange::start(
                    effect_id,
                    correlation_id,
                    mechanism,
                    version,
                    self.client_id.as_ref().map(crate::config::ClientId::wire),
                    self.outbound_frame,
                    self.negotiation_limits.decode_limits(),
                );
                let (exchange, frame) = match exchange {
                    Ok(exchange) => exchange,
                    Err(error) => {
                        self.fail_handshake(poller, identity, effect_id, error.failure())?;
                        return Ok(false);
                    }
                };
                self.authentication_exchange = Some(AuthenticationExchange::Handshake(exchange));
                match self.admit_authentication_write(poller, identity, effect_id, frame)? {
                    AuthenticationWriteOutcome::Admitted => {}
                    AuthenticationWriteOutcome::CapacityReached => {
                        self.fail_handshake(
                            poller,
                            identity,
                            effect_id,
                            AuthenticationFailure::LocalCapacity,
                        )?;
                        return Ok(false);
                    }
                    AuthenticationWriteOutcome::ConnectionLost => return Ok(false),
                }
            }
            AuthenticationEffect::SendExchange {
                epoch,
                transport_id,
                effect_id,
                round,
                correlation_id,
                version,
            } => {
                if epoch != identity.epoch() || transport_id != identity.transport_id() {
                    return Err(BrokerError::MissingEffect);
                }
                if !self.start_exchange(
                    poller,
                    identity,
                    effect_id,
                    round,
                    correlation_id,
                    version,
                )? {
                    return Ok(false);
                }
            }
            AuthenticationEffect::CancelDeadline { timer_id } => {
                self.timers.cancel(timer_id);
            }
            AuthenticationEffect::Succeeded | AuthenticationEffect::Failed { .. } => {
                return Err(BrokerError::UnexpectedEffect(
                    ConnectionEffect::Authentication { effect },
                ));
            }
        }
        Ok(true)
    }

    fn start_exchange(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effect_id: EffectId,
        round: AuthenticationRound,
        correlation_id: kafka_driver_core::CorrelationId,
        version: kafka_wire_core::ApiVersion,
    ) -> Result<bool, BrokerError> {
        let message = self
            .authentication_session
            .as_mut()
            .ok_or(BrokerError::MissingEffect)?
            .next_message(self.outbound_frame.max_frame_bytes());
        let message = match message {
            Ok(message) => message,
            Err(failure) => {
                self.fail_exchange(poller, identity, effect_id, round, failure)?;
                return Ok(false);
            }
        };
        let exchange = AuthenticateExchange::start(
            effect_id,
            round,
            correlation_id,
            version,
            &message,
            self.client_id.as_ref().map(crate::config::ClientId::wire),
            self.outbound_frame,
            self.negotiation_limits.decode_limits(),
        );
        let (exchange, frame) = match exchange {
            Ok(exchange) => exchange,
            Err(error) => {
                self.fail_exchange(poller, identity, effect_id, round, error.failure())?;
                return Ok(false);
            }
        };
        self.authentication_exchange = Some(AuthenticationExchange::Authenticate(exchange));
        match self.admit_authentication_write(poller, identity, effect_id, frame)? {
            AuthenticationWriteOutcome::Admitted => {}
            AuthenticationWriteOutcome::CapacityReached => {
                self.fail_exchange(
                    poller,
                    identity,
                    effect_id,
                    round,
                    AuthenticationFailure::LocalCapacity,
                )?;
                return Ok(false);
            }
            AuthenticationWriteOutcome::ConnectionLost => return Ok(false),
        }
        Ok(true)
    }
}
