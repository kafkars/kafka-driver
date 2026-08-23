//! Expected resolver calls and independently identified delayed outcomes.

use std::time::Duration;

use calandria::Span;
use calandria_sim::Planned;
use kafka_driver_core::{DnsOutcome, DnsRequest};

/// One exact resolver expectation and its delayed deterministic outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsStep {
    expected: DnsRequest,
    planned: Planned<DnsOutcome>,
}

impl DnsStep {
    /// Creates one resolver script step.
    pub fn new(expected: DnsRequest, delay: Duration, outcome: DnsOutcome) -> Self {
        Self {
            expected,
            planned: Planned::new(
                Span::try_from(delay).unwrap_or(Span::from_nanos(u64::MAX)),
                outcome,
            ),
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
