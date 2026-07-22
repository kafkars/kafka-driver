//! Expected resolver calls and independently identified delayed outcomes.

use std::time::Duration;

use kafka_driver_core::{ConnectionEpoch, EffectId};

use crate::{BrokerEndpoint, Planned, ResolvedAddress};

/// One resolver call expected by a deterministic DNS script.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsRequest {
    epoch: ConnectionEpoch,
    effect_id: EffectId,
    endpoint: BrokerEndpoint,
}

impl DnsRequest {
    /// Creates an expected resolver request.
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

    /// Returns the connection epoch that requested resolution.
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

/// Scriptable resolver failure without operating-system error leakage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsFailure {
    /// No records exist for the configured name.
    NameNotFound,
    /// Resolution failed transiently and may succeed later.
    Temporary,
    /// Resolution succeeded without a usable address.
    NoUsableAddress,
}

/// Delayed resolver result whose identity may intentionally be stale.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsOutcome {
    epoch: ConnectionEpoch,
    effect_id: EffectId,
    result: Result<Vec<ResolvedAddress>, DnsFailure>,
}

impl DnsOutcome {
    /// Creates a resolved or failed result with explicit returned identities.
    pub const fn new(
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        result: Result<Vec<ResolvedAddress>, DnsFailure>,
    ) -> Self {
        Self {
            epoch,
            effect_id,
            result,
        }
    }

    /// Returns the epoch carried by the resolver result.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the effect identity carried by the resolver result.
    pub const fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    /// Borrows the resolved addresses or scripted failure.
    pub const fn result(&self) -> &Result<Vec<ResolvedAddress>, DnsFailure> {
        &self.result
    }

    /// Consumes the outcome and returns the resolved addresses or failure.
    pub fn into_result(self) -> Result<Vec<ResolvedAddress>, DnsFailure> {
        self.result
    }
}

/// One exact resolver expectation and its delayed deterministic outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsStep {
    expected: DnsRequest,
    planned: Planned<DnsOutcome>,
}

impl DnsStep {
    /// Creates one resolver script step.
    pub const fn new(expected: DnsRequest, delay: Duration, outcome: DnsOutcome) -> Self {
        Self {
            expected,
            planned: Planned::new(delay, outcome),
        }
    }

    /// Returns the exact request required to consume this step.
    pub const fn expected(&self) -> &DnsRequest {
        &self.expected
    }

    pub(super) fn into_parts(self) -> (DnsRequest, Planned<DnsOutcome>) {
        (self.expected, self.planned)
    }
}
