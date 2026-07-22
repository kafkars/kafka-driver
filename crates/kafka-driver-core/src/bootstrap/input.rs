//! Commands and identity-fenced resolver outcomes accepted by bootstrap policy.

use crate::{ConnectionEpoch, DnsOutcome, EffectId};

/// One owner command or external DNS outcome applied to bootstrap policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapInput {
    /// Starts a new bootstrap connection generation.
    Start {
        /// Fresh connection generation that will own the selected address.
        epoch: ConnectionEpoch,
        /// Identity reserved for the first DNS effect.
        effect_id: EffectId,
    },
    /// Reports one DNS outcome and reserves an identity if another endpoint is needed.
    ResolutionCompleted {
        /// Outcome echoing the completed generation and effect identity.
        outcome: DnsOutcome,
        /// Identity reserved for a possible next endpoint attempt.
        retry_effect_id: EffectId,
    },
}
