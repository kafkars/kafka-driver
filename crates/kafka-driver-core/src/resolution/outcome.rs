//! Sanitized identity-fenced outcomes returned by a DNS interpreter.

use crate::{ConnectionEpoch, EffectId};

use super::ResolvedAddressSet;

/// Resolver failure without operating-system error or host detail leakage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsFailure {
    /// No records exist for the requested name.
    NameNotFound,
    /// Resolution failed transiently and may succeed later.
    Temporary,
    /// Resolution succeeded without a usable bounded address set.
    NoUsableAddress,
}

/// Resolver result whose echoed identity permits stale-work rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsOutcome {
    epoch: ConnectionEpoch,
    effect_id: EffectId,
    result: Result<ResolvedAddressSet, DnsFailure>,
}

impl DnsOutcome {
    /// Creates an outcome carrying the identities supplied to external work.
    pub const fn new(
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        result: Result<ResolvedAddressSet, DnsFailure>,
    ) -> Self {
        Self {
            epoch,
            effect_id,
            result,
        }
    }

    /// Returns the connection generation carried by this outcome.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the external effect identity carried by this outcome.
    pub const fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    /// Borrows the usable addresses or sanitized failure.
    pub const fn result(&self) -> &Result<ResolvedAddressSet, DnsFailure> {
        &self.result
    }

    /// Consumes the outcome and returns its addresses or sanitized failure.
    pub fn into_result(self) -> Result<ResolvedAddressSet, DnsFailure> {
        self.result
    }
}
