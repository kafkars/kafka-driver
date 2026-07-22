//! Route demand and identity-echoing DNS outcomes accepted by broker resolution.

use crate::{BrokerEndpoint, BrokerRoute, ConnectionEpoch, DnsOutcome, EffectId};

/// One route activation or external resolver outcome for a broker identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerResolutionInput {
    /// Starts or supersedes advertised endpoint resolution.
    Start {
        /// Generation-fenced route requesting this broker.
        route: BrokerRoute,
        /// Endpoint advertised by the same immutable generation.
        endpoint: BrokerEndpoint,
        /// Fresh connection generation that will own a successful result.
        epoch: ConnectionEpoch,
        /// Fresh external DNS identity.
        effect_id: EffectId,
    },
    /// Reports one resolver result carrying the identities supplied at start.
    ResolutionCompleted {
        /// Identity-fenced DNS outcome.
        outcome: DnsOutcome,
    },
}
