//! FIFO DNS expectation matching with non-consuming mismatch failures.

use std::{collections::VecDeque, error::Error, fmt};

use crate::{DnsOutcome, DnsRequest, DnsStep, Planned};

/// Finite ordered resolver script.
#[derive(Clone, Debug, Default)]
pub struct ScriptedDns {
    steps: VecDeque<DnsStep>,
}

impl ScriptedDns {
    /// Owns a finite sequence of resolver expectations.
    pub fn new(steps: impl IntoIterator<Item = DnsStep>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
        }
    }

    /// Matches and consumes exactly the next resolver expectation.
    pub fn resolve(&mut self, request: DnsRequest) -> Result<Planned<DnsOutcome>, DnsScriptError> {
        let Some(next) = self.steps.front() else {
            return Err(DnsScriptError::PlanExhausted { received: request });
        };
        if next.expected() != &request {
            return Err(DnsScriptError::UnexpectedRequest {
                expected: next.expected().clone(),
                received: request,
            });
        }
        let Some(step) = self.steps.pop_front() else {
            return Err(DnsScriptError::PlanExhausted { received: request });
        };
        let (_, planned) = step.into_parts();
        Ok(planned)
    }

    /// Returns resolver expectations not yet consumed.
    pub fn remaining_steps(&self) -> usize {
        self.steps.len()
    }

    /// Returns whether every resolver expectation was consumed.
    pub fn is_complete(&self) -> bool {
        self.steps.is_empty()
    }
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
