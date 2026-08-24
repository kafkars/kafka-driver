//! FIFO DNS expectation matching with non-consuming mismatch failures.

use std::{error::Error, fmt};

use criticality::{
    plan::Plan,
    retained::RetainedBytes,
    script::{ExactScript, ScriptFailure, ScriptLimits, ScriptStep},
};

use crate::{DnsOutcome, DnsRequest, DnsStep};

/// Finite ordered resolver script.
#[derive(Clone, Debug)]
pub struct ScriptedDns {
    script: ExactScript<DnsRequest, DnsOutcome>,
}

impl ScriptedDns {
    /// Owns a finite sequence of resolver expectations.
    pub fn new(steps: impl IntoIterator<Item = DnsStep>) -> Self {
        let steps = steps
            .into_iter()
            .map(DnsStep::into_script_step)
            .collect::<Vec<_>>();
        let limits = script_limits(&steps);
        // Compatibility contract: these pre-existing fixtures had no retained-byte
        // budget. Count admission is exact; variable bytes remain unmeasured.
        let script = ExactScript::try_with_measure(
            limits,
            steps,
            |_| RetainedBytes::ZERO,
            |_| RetainedBytes::ZERO,
        )
        .unwrap_or_else(|error| panic!("exact DNS script must fit derived limits: {error}"));
        Self { script }
    }

    /// Matches and consumes exactly the next resolver expectation.
    pub fn resolve(&mut self, request: DnsRequest) -> Result<Plan<DnsOutcome>, DnsScriptError> {
        match self.script.respond(&request) {
            Ok(response) => Ok(response),
            Err(ScriptFailure::Exhausted { .. }) => {
                Err(DnsScriptError::PlanExhausted { received: request })
            }
            Err(ScriptFailure::Mismatch { .. }) => match self.script.expected().cloned() {
                Some(expected) => Err(DnsScriptError::UnexpectedRequest {
                    expected,
                    received: request,
                }),
                None => Err(DnsScriptError::PlanExhausted { received: request }),
            },
        }
    }

    /// Returns resolver expectations not yet consumed.
    pub fn remaining_steps(&self) -> usize {
        self.script.len()
    }

    /// Returns whether every resolver expectation was consumed.
    pub fn is_complete(&self) -> bool {
        self.script.is_empty()
    }
}

impl Default for ScriptedDns {
    fn default() -> Self {
        Self::new([])
    }
}

fn script_limits(steps: &[ScriptStep<DnsRequest, DnsOutcome>]) -> ScriptLimits {
    let outcomes = steps.iter().fold(0_usize, |total, step| {
        total.saturating_add(step.response().len())
    });
    ScriptLimits::new(steps.len(), outcomes, RetainedBytes::ZERO)
}

/// Why a resolver call could not consume the next deterministic step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DnsScriptError {
    /// A resolver call arrived after the finite script ended.
    PlanExhausted {
        /// Unscripted resolver request.
        received: DnsRequest,
    },
    /// A resolver call differed from the next exact expectation.
    UnexpectedRequest {
        /// Next request required by the script.
        expected: DnsRequest,
        /// Request actually made.
        received: DnsRequest,
    },
}

impl fmt::Display for DnsScriptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlanExhausted { .. } => {
                formatter.write_str("DNS script exhausted before resolver call")
            }
            Self::UnexpectedRequest { .. } => {
                formatter.write_str("resolver call did not match next DNS script step")
            }
        }
    }
}

impl Error for DnsScriptError {}
