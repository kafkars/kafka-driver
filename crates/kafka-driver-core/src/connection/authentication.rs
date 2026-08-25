//! Composition of the secret-free SASL child into one connection epoch.

use std::mem;

use crate::{
    AuthenticationDisposition, AuthenticationEffect, AuthenticationFailure, AuthenticationInput,
    AuthenticationMachine, AuthenticationState, SaslProtocol,
};

use super::{
    ActiveConnection, ActiveMode, CloseReason, ConnectionEffect, ConnectionMachine, Decision,
    KafkaSessionDeadline, KafkaSessionDisposition, KafkaSessionInput, StateData,
};

impl ConnectionMachine {
    pub(super) fn begin_authentication(
        &mut self,
        epoch: crate::ConnectionEpoch,
        transport_id: crate::TransportId,
        effect_id: crate::EffectId,
        capabilities: crate::NegotiatedCapabilities,
        attempt: crate::AuthenticationAttempt,
    ) -> Decision {
        let Some(deadline_timer) = self.matching_negotiation(epoch, transport_id, effect_id) else {
            return Decision::stale();
        };
        let session =
            self.session
                .apply(KafkaSessionInput::ApiVersionsSucceededWithAuthentication {
                    capabilities: capabilities.clone(),
                    deadline: KafkaSessionDeadline::new(attempt.now(), attempt.deadline()),
                });
        if session.disposition() == KafkaSessionDisposition::IgnoredStale {
            return Decision::stale();
        }
        if capabilities.len() > self.limits.max_capabilities().get() {
            return self.finish_negotiation_failure(super::NegotiationFailure::Capacity);
        }
        let Some(policy) = self.authentication else {
            return self.finish_authentication_setup_failure(AuthenticationFailure::Protocol);
        };
        let Some(handshake_version) = capabilities.version(policy.handshake_api_key()) else {
            return self.finish_authentication_setup_failure(AuthenticationFailure::Protocol);
        };
        let Some(authenticate_version) = capabilities.version(policy.authenticate_api_key()) else {
            return self.finish_authentication_setup_failure(AuthenticationFailure::Protocol);
        };
        let protocol =
            SaslProtocol::new(policy.mechanism(), handshake_version, authenticate_version);
        let mut authentication =
            AuthenticationMachine::new(epoch, transport_id, protocol, policy.limits());
        let transition = authentication.apply(AuthenticationInput::Start { attempt });
        if let AuthenticationState::Failed { failure } = authentication.state() {
            return self.finish_authentication_setup_failure(failure);
        }
        self.state = StateData::Authenticating {
            epoch,
            transport_id,
            capabilities,
            authentication,
        };
        let mut effects = Vec::with_capacity(1 + transition.effects().len());
        effects.push(ConnectionEffect::CancelDeadline {
            timer_id: deadline_timer,
        });
        effects.extend(wrap_effects(transition.into_effects()));
        Decision::applied(effects)
    }

    pub(super) fn authentication_input(&mut self, input: AuthenticationInput) -> Decision {
        let StateData::Authenticating { authentication, .. } = &mut self.state else {
            return Decision::stale();
        };
        let transition = authentication.apply(input);
        if transition.disposition() == AuthenticationDisposition::IgnoredStale {
            return Decision::stale();
        }
        if let Some(input) = session_authentication_input(input) {
            let session = self.session.apply(input);
            debug_assert_ne!(
                session.disposition(),
                KafkaSessionDisposition::IgnoredStale,
                "legacy authentication accepted an input rejected by session policy"
            );
        }
        match authentication.state() {
            AuthenticationState::Succeeded => self.finish_authentication_success(transition),
            AuthenticationState::Failed { failure } => {
                self.finish_authentication_failure(failure, transition)
            }
            AuthenticationState::Dormant
            | AuthenticationState::Handshaking { .. }
            | AuthenticationState::Exchanging { .. } => {
                Decision::applied(wrap_effects(transition.into_effects()))
            }
        }
    }

    pub(super) fn finish_authentication_setup_failure(
        &mut self,
        failure: AuthenticationFailure,
    ) -> Decision {
        let StateData::Negotiating {
            epoch,
            transport_id,
            deadline_timer,
            ..
        } = self.state
        else {
            return Decision::stale();
        };
        let reason = CloseReason::AuthenticationFailed(failure);
        self.state = StateData::Closing {
            epoch,
            transport_id,
            reason,
        };
        Decision::applied(vec![
            ConnectionEffect::CloseTransport {
                epoch,
                transport_id,
                reason,
            },
            ConnectionEffect::CancelDeadline {
                timer_id: deadline_timer,
            },
        ])
    }

    fn matching_negotiation(
        &self,
        epoch: crate::ConnectionEpoch,
        transport_id: crate::TransportId,
        effect_id: crate::EffectId,
    ) -> Option<crate::TimerId> {
        let StateData::Negotiating {
            epoch: expected_epoch,
            transport_id: expected_transport,
            effect_id: expected_effect,
            deadline_timer,
            ..
        } = self.state
        else {
            return None;
        };
        (epoch == expected_epoch
            && transport_id == expected_transport
            && effect_id == expected_effect)
            .then_some(deadline_timer)
    }

    fn finish_authentication_success(
        &mut self,
        transition: crate::AuthenticationTransition,
    ) -> Decision {
        let epoch = self.state.epoch();
        let placeholder = StateData::Closed {
            epoch,
            reason: CloseReason::Requested,
        };
        let previous = mem::replace(&mut self.state, placeholder);
        let StateData::Authenticating {
            epoch,
            transport_id,
            capabilities,
            ..
        } = previous
        else {
            return Decision::stale();
        };
        self.state = StateData::Active {
            mode: ActiveMode::Ready,
            connection: ActiveConnection::new(epoch, transport_id, capabilities, self.limits),
        };
        Decision::applied(wrap_effects(transition.into_effects()))
    }

    fn finish_authentication_failure(
        &mut self,
        failure: AuthenticationFailure,
        transition: crate::AuthenticationTransition,
    ) -> Decision {
        let epoch = self.state.epoch();
        let placeholder = StateData::Closed {
            epoch,
            reason: CloseReason::Requested,
        };
        let previous = mem::replace(&mut self.state, placeholder);
        let StateData::Authenticating {
            epoch,
            transport_id,
            ..
        } = previous
        else {
            return Decision::stale();
        };
        let reason = CloseReason::AuthenticationFailed(failure);
        self.state = StateData::Closing {
            epoch,
            transport_id,
            reason,
        };
        let mut effects = vec![ConnectionEffect::CloseTransport {
            epoch,
            transport_id,
            reason,
        }];
        effects.extend(wrap_effects(transition.into_effects()));
        Decision::applied(effects)
    }
}

fn session_authentication_input(input: AuthenticationInput) -> Option<KafkaSessionInput> {
    match input {
        AuthenticationInput::Start { .. } => None,
        AuthenticationInput::HandshakeAccepted { .. } => {
            Some(KafkaSessionInput::AuthenticationHandshakeSucceeded)
        }
        AuthenticationInput::HandshakeFailed { failure, .. } => {
            Some(KafkaSessionInput::AuthenticationHandshakeFailed { failure })
        }
        AuthenticationInput::ExchangeCompleted { round, outcome, .. } => {
            Some(KafkaSessionInput::AuthenticationExchangeCompleted { round, outcome })
        }
        AuthenticationInput::DeadlineElapsed { now, .. } => {
            Some(KafkaSessionInput::DeadlineElapsed { now })
        }
    }
}

fn wrap_effects(effects: Vec<AuthenticationEffect>) -> Vec<ConnectionEffect> {
    effects
        .into_iter()
        .filter_map(|effect| match effect {
            AuthenticationEffect::Succeeded | AuthenticationEffect::Failed { .. } => None,
            effect => Some(ConnectionEffect::Authentication { effect }),
        })
        .collect()
}
