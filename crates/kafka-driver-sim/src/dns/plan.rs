//! Expected resolver calls and independently identified delayed outcomes.

use std::time::Duration;

use criticality::{
    plan::{Plan, Planned},
    script::ScriptStep,
    time::Span,
};
use kafka_driver_core::{DnsOutcome, DnsRequest};

/// One exact resolver expectation and its delayed deterministic outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DnsStep {
    expected: DnsRequest,
    response: Plan<DnsOutcome>,
}

impl DnsStep {
    /// Creates one resolver script step.
    pub fn new(expected: DnsRequest, delay: Duration, outcome: DnsOutcome) -> Self {
        Self::with_plan(
            expected,
            Plan::single(Planned::new(
                Span::from_ticks(u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX)),
                outcome,
            )),
        )
    }

    /// Creates one resolver script step with zero or more delayed outcomes.
    pub const fn with_plan(expected: DnsRequest, response: Plan<DnsOutcome>) -> Self {
        Self { expected, response }
    }

    /// Returns the exact request required to consume this step.
    pub const fn expected(&self) -> &DnsRequest {
        &self.expected
    }

    pub(super) fn into_script_step(self) -> ScriptStep<DnsRequest, DnsOutcome> {
        ScriptStep::new(self.expected, self.response)
    }
}
