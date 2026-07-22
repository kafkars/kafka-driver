//! FIFO poller expectation matching with non-consuming mismatch failures.

use std::{collections::VecDeque, error::Error, fmt};

use crate::{Planned, PollRequest, PollStep, ReadinessEvent};

/// Finite ordered readiness script.
#[derive(Clone, Debug, Default)]
pub struct ScriptedPoller {
    steps: VecDeque<PollStep>,
}

impl ScriptedPoller {
    /// Owns a finite sequence of interest expectations.
    pub fn new(steps: impl IntoIterator<Item = PollStep>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
        }
    }

    /// Matches and consumes exactly the next interest arm.
    pub fn arm(
        &mut self,
        request: PollRequest,
    ) -> Result<Planned<ReadinessEvent>, PollScriptError> {
        let Some(next) = self.steps.front() else {
            return Err(PollScriptError::PlanExhausted { received: request });
        };
        if next.expected() != request {
            return Err(PollScriptError::UnexpectedRequest {
                expected: next.expected(),
                received: request,
            });
        }
        let Some(step) = self.steps.pop_front() else {
            return Err(PollScriptError::PlanExhausted { received: request });
        };
        Ok(step.into_planned())
    }

    /// Returns interest expectations not yet consumed.
    pub fn remaining_steps(&self) -> usize {
        self.steps.len()
    }

    /// Returns whether every interest expectation was consumed.
    pub fn is_complete(&self) -> bool {
        self.steps.is_empty()
    }
}

/// Why an interest arm could not consume the next deterministic step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PollScriptError {
    /// An interest arm arrived after the finite script ended.
    PlanExhausted {
        /// Unscripted interest arm.
        received: PollRequest,
    },
    /// An interest arm differed from the next exact expectation.
    UnexpectedRequest {
        /// Next request required by the script.
        expected: PollRequest,
        /// Request actually made.
        received: PollRequest,
    },
}

impl fmt::Display for PollScriptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlanExhausted { .. } => {
                formatter.write_str("poller script exhausted before interest arm")
            }
            Self::UnexpectedRequest { .. } => {
                formatter.write_str("interest arm did not match next poller script step")
            }
        }
    }
}

impl Error for PollScriptError {}
