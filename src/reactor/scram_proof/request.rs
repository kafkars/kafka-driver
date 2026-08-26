//! Secret-owning SCRAM proof work fenced to one authentication exchange.

use std::fmt;

use bornera::ConnectionToken;
use kafka_driver_core::{AuthenticationRound, EffectId};
use sasl_scram::{AwaitingServerFinal, Error, OutboundMessage, PendingDerivation};

#[cfg(test)]
use crate::reactor::resource::{ResourceIdentity, ResourceToken};

/// Backend-neutral destination for one proof completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ScramProofTarget {
    #[cfg(test)]
    Legacy {
        token: ResourceToken,
        identity: ResourceIdentity,
    },
    Direct {
        connection: ConnectionToken,
    },
}

impl ScramProofTarget {
    #[cfg(test)]
    pub(in crate::reactor) const fn legacy(
        token: ResourceToken,
        identity: ResourceIdentity,
    ) -> Self {
        Self::Legacy { token, identity }
    }

    pub(in crate::reactor) const fn direct(connection: ConnectionToken) -> Self {
        Self::Direct { connection }
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn legacy_identity(
        self,
    ) -> Option<(ResourceToken, ResourceIdentity)> {
        match self {
            #[cfg(test)]
            Self::Legacy { token, identity } => Some((token, identity)),
            Self::Direct { .. } => None,
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn direct_connection(self) -> Option<ConnectionToken> {
        match self {
            Self::Direct { connection } => Some(connection),
            #[cfg(test)]
            Self::Legacy { .. } => None,
        }
    }

    #[cfg(not(test))]
    pub(in crate::reactor) const fn direct_connection(self) -> ConnectionToken {
        match self {
            Self::Direct { connection } => connection,
        }
    }
}

/// Exact backend generation and authentication exchange owning one proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ScramProofFence {
    target: ScramProofTarget,
    #[cfg(test)]
    effect_id: EffectId,
    round: AuthenticationRound,
}

impl ScramProofFence {
    #[cfg(test)]
    pub(in crate::reactor) const fn legacy(
        token: ResourceToken,
        identity: ResourceIdentity,
        effect_id: EffectId,
        round: AuthenticationRound,
    ) -> Self {
        Self {
            target: ScramProofTarget::legacy(token, identity),
            effect_id,
            round,
        }
    }

    pub(in crate::reactor) const fn direct(
        connection: ConnectionToken,
        effect_id: EffectId,
        round: AuthenticationRound,
    ) -> Self {
        #[cfg(not(test))]
        let _ = effect_id;
        Self {
            target: ScramProofTarget::direct(connection),
            #[cfg(test)]
            effect_id,
            round,
        }
    }

    pub(in crate::reactor) const fn target(self) -> ScramProofTarget {
        self.target
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn effect_id(self) -> EffectId {
        self.effect_id
    }

    pub(in crate::reactor) const fn round(self) -> AuthenticationRound {
        self.round
    }
}

pub(in crate::reactor) struct ScramProofRequest {
    fence: ScramProofFence,
    pending: PendingDerivation,
}

impl ScramProofRequest {
    #[cfg(test)]
    pub(in crate::reactor) fn legacy(
        token: ResourceToken,
        identity: ResourceIdentity,
        effect_id: EffectId,
        round: AuthenticationRound,
        pending: PendingDerivation,
    ) -> Self {
        Self {
            fence: ScramProofFence::legacy(token, identity, effect_id, round),
            pending,
        }
    }

    pub(in crate::reactor) fn direct(
        connection: ConnectionToken,
        effect_id: EffectId,
        round: AuthenticationRound,
        pending: PendingDerivation,
    ) -> Self {
        Self {
            fence: ScramProofFence::direct(connection, effect_id, round),
            pending,
        }
    }

    pub(in crate::reactor) const fn fence(&self) -> ScramProofFence {
        self.fence
    }

    pub(in crate::reactor) fn finish(self) -> ScramProofOutcome {
        ScramProofOutcome {
            fence: self.fence,
            result: self.pending.derive(),
        }
    }
}

impl fmt::Debug for ScramProofRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramProofRequest")
            .field("fence", &self.fence)
            .finish_non_exhaustive()
    }
}

pub(in crate::reactor) struct ScramProofOutcome {
    fence: ScramProofFence,
    result: Result<(AwaitingServerFinal, OutboundMessage), Error>,
}

impl ScramProofOutcome {
    pub(in crate::reactor) const fn fence(&self) -> ScramProofFence {
        self.fence
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
            .field("fence", &self.fence)
            .field("succeeded", &self.result.is_ok())
            .finish_non_exhaustive()
    }
}
