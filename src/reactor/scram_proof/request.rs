//! Secret-owning SCRAM proof work fenced to one authentication exchange.

use std::fmt;

use kafka_driver_core::{AuthenticationRound, EffectId, ExchangeOutcome};
use kafka_wire_core::Bytes;

use crate::{
    authentication::AuthenticationSession,
    reactor::resource::{ResourceIdentity, ResourceToken},
};

pub(in crate::reactor) struct ScramProofRequest {
    token: ResourceToken,
    identity: ResourceIdentity,
    effect_id: EffectId,
    round: AuthenticationRound,
    session: AuthenticationSession,
    response: Bytes,
}

impl ScramProofRequest {
    pub(in crate::reactor) fn new(
        token: ResourceToken,
        identity: ResourceIdentity,
        effect_id: EffectId,
        round: AuthenticationRound,
        session: AuthenticationSession,
        response: Bytes,
    ) -> Self {
        Self {
            token,
            identity,
            effect_id,
            round,
            session,
            response,
        }
    }

    pub(super) fn finish(mut self) -> ScramProofOutcome {
        let outcome = self.session.receive(&self.response);
        ScramProofOutcome {
            token: self.token,
            identity: self.identity,
            effect_id: self.effect_id,
            round: self.round,
            session: self.session,
            outcome,
        }
    }

    pub(in crate::reactor) fn into_session(self) -> AuthenticationSession {
        self.session
    }
}

impl fmt::Debug for ScramProofRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramProofRequest")
            .field("token", &self.token)
            .field("identity", &self.identity)
            .field("effect_id", &self.effect_id)
            .field("round", &self.round)
            .finish_non_exhaustive()
    }
}

pub(in crate::reactor) struct ScramProofOutcome {
    token: ResourceToken,
    identity: ResourceIdentity,
    effect_id: EffectId,
    round: AuthenticationRound,
    session: AuthenticationSession,
    outcome: ExchangeOutcome,
}

impl ScramProofOutcome {
    pub(in crate::reactor) const fn token(&self) -> ResourceToken {
        self.token
    }

    pub(in crate::reactor) const fn identity(&self) -> ResourceIdentity {
        self.identity
    }

    pub(in crate::reactor) const fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    pub(in crate::reactor) const fn round(&self) -> AuthenticationRound {
        self.round
    }

    pub(in crate::reactor) fn into_parts(self) -> (AuthenticationSession, ExchangeOutcome) {
        (self.session, self.outcome)
    }
}

impl fmt::Debug for ScramProofOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramProofOutcome")
            .field("token", &self.token)
            .field("identity", &self.identity)
            .field("effect_id", &self.effect_id)
            .field("round", &self.round)
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}
