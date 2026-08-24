//! Secret-owning SCRAM proof work fenced to one authentication exchange.

use std::fmt;

use kafka_driver_core::{AuthenticationRound, EffectId};
use sasl_scram::{AwaitingServerFinal, Error, OutboundMessage, PendingDerivation};

use crate::reactor::resource::{ResourceIdentity, ResourceToken};

pub(in crate::reactor) struct ScramProofRequest {
    token: ResourceToken,
    identity: ResourceIdentity,
    effect_id: EffectId,
    round: AuthenticationRound,
    pending: PendingDerivation,
}

impl ScramProofRequest {
    pub(in crate::reactor) fn new(
        token: ResourceToken,
        identity: ResourceIdentity,
        effect_id: EffectId,
        round: AuthenticationRound,
        pending: PendingDerivation,
    ) -> Self {
        Self {
            token,
            identity,
            effect_id,
            round,
            pending,
        }
    }

    pub(super) fn finish(self) -> ScramProofOutcome {
        ScramProofOutcome {
            token: self.token,
            identity: self.identity,
            effect_id: self.effect_id,
            round: self.round,
            result: self.pending.derive(),
        }
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
    result: Result<(AwaitingServerFinal, OutboundMessage), Error>,
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

    pub(in crate::reactor) fn into_result(
        self,
    ) -> Result<(AwaitingServerFinal, OutboundMessage), Error> {
        self.result
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
            .field("succeeded", &self.result.is_ok())
            .finish_non_exhaustive()
    }
}
