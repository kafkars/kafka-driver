//! Authentication owner, input dispatch, terminal outcomes, and shared identity checks.

use crate::{ConnectionEpoch, CorrelationId, EffectId, TimerId, TransportId};

use super::{
    AuthenticationDisposition, AuthenticationEffect, AuthenticationFailure, AuthenticationInput,
    AuthenticationLimits, AuthenticationRound, AuthenticationState, AuthenticationTransition,
    SaslProtocol, StateData,
};

/// Deterministic policy owner for one connection epoch's SASL exchange.
#[derive(Debug)]
pub struct AuthenticationMachine {
    pub(super) epoch: ConnectionEpoch,
    pub(super) transport_id: TransportId,
    pub(super) protocol: SaslProtocol,
    pub(super) limits: AuthenticationLimits,
    pub(super) state: StateData,
}

impl AuthenticationMachine {
    /// Creates a dormant machine with negotiated, non-secret protocol choices.
    pub const fn new(
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        protocol: SaslProtocol,
        limits: AuthenticationLimits,
    ) -> Self {
        Self {
            epoch,
            transport_id,
            protocol,
            limits,
            state: StateData::Dormant,
        }
    }

    /// Applies one input through exclusive ownership.
    pub fn apply(&mut self, input: AuthenticationInput) -> AuthenticationTransition {
        let decision = match input {
            AuthenticationInput::Start { attempt } => self.start(attempt),
            AuthenticationInput::HandshakeAccepted {
                epoch,
                transport_id,
                effect_id,
            } => self.handshake_accepted(epoch, transport_id, effect_id),
            AuthenticationInput::HandshakeFailed {
                epoch,
                transport_id,
                effect_id,
                failure,
            } => self.handshake_failed(epoch, transport_id, effect_id, failure),
            AuthenticationInput::ExchangeCompleted {
                epoch,
                transport_id,
                effect_id,
                round,
                outcome,
            } => self.exchange_completed(epoch, transport_id, effect_id, round, outcome),
            AuthenticationInput::DeadlineElapsed {
                epoch,
                timer_id,
                now,
            } => self.deadline_elapsed(epoch, timer_id, now),
        };
        AuthenticationTransition::new(decision.effects, decision.disposition)
    }

    /// Returns a secret-free snapshot of the current stage.
    pub const fn state(&self) -> AuthenticationState {
        self.state
    }

    pub(super) fn matches(
        &self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
        expected_effect: EffectId,
    ) -> bool {
        epoch == self.epoch && transport_id == self.transport_id && effect_id == expected_effect
    }

    pub(super) fn exchange_effect(
        &self,
        effect_id: EffectId,
        round: AuthenticationRound,
    ) -> AuthenticationEffect {
        AuthenticationEffect::SendExchange {
            epoch: self.epoch,
            transport_id: self.transport_id,
            effect_id,
            round,
            correlation_id: CorrelationId::from_raw(i32::from(round.get()) + 1),
            version: self.protocol.authenticate_version(),
        }
    }

    pub(super) fn succeed(&mut self, deadline_timer: TimerId) -> Decision {
        self.state = StateData::Succeeded;
        Decision::applied(vec![
            AuthenticationEffect::CancelDeadline {
                timer_id: deadline_timer,
            },
            AuthenticationEffect::Succeeded,
        ])
    }

    pub(super) fn fail(
        &mut self,
        failure: AuthenticationFailure,
        deadline_timer: Option<TimerId>,
    ) -> Decision {
        self.state = StateData::Failed { failure };
        let mut effects = Vec::with_capacity(2);
        if let Some(timer_id) = deadline_timer {
            effects.push(AuthenticationEffect::CancelDeadline { timer_id });
        }
        effects.push(AuthenticationEffect::Failed { failure });
        Decision::applied(effects)
    }
}

pub(super) struct Decision {
    pub(super) effects: Vec<AuthenticationEffect>,
    pub(super) disposition: AuthenticationDisposition,
}

impl Decision {
    pub(super) const fn applied(effects: Vec<AuthenticationEffect>) -> Self {
        Self {
            effects,
            disposition: AuthenticationDisposition::Applied,
        }
    }

    pub(super) const fn stale() -> Self {
        Self {
            effects: Vec::new(),
            disposition: AuthenticationDisposition::IgnoredStale,
        }
    }
}
