//! One identity-fenced broker DNS request.

use crate::{BrokerEndpoint, ConnectionEpoch, EffectId};

/// External resolver work requested for one connection generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsRequest {
    epoch: ConnectionEpoch,
    effect_id: EffectId,
    endpoint: BrokerEndpoint,
}

impl DnsRequest {
    /// Creates resolver work with identities that its outcome must echo.
    pub const fn new(
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        endpoint: BrokerEndpoint,
    ) -> Self {
        Self {
            epoch,
            effect_id,
            endpoint,
        }
    }

    /// Returns the connection generation requesting resolution.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the external effect identity.
    pub const fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    /// Returns the broker endpoint being resolved.
    pub const fn endpoint(&self) -> &BrokerEndpoint {
        &self.endpoint
    }
}
