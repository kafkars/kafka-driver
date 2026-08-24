//! FIFO poller expectation matching with non-consuming mismatch failures.

use std::{error::Error, fmt};

use criticality::{
    plan::Plan,
    retained::RetainedBytes,
    script::{ExactScript, ScriptFailure, ScriptLimits, ScriptStep},
};

use crate::{PollRequest, PollStep, ReadinessEvent};

/// Finite ordered readiness script.
#[derive(Clone, Debug)]
pub struct ScriptedPoller {
    script: ExactScript<PollRequest, ReadinessEvent>,
}

impl ScriptedPoller {
    /// Owns a finite sequence of interest expectations.
    pub fn new(steps: impl IntoIterator<Item = PollStep>) -> Self {
        let steps = steps
            .into_iter()
            .map(PollStep::into_script_step)
            .collect::<Vec<_>>();
        let limits = script_limits(&steps);
        let script = ExactScript::try_with_measure(
            limits,
            steps,
            |_| RetainedBytes::ZERO,
            |_| RetainedBytes::ZERO,
        )
        .unwrap_or_else(|error| panic!("exact poller script must fit derived limits: {error}"));
        Self { script }
    }

    /// Matches and consumes exactly the next interest arm.
    pub fn arm(&mut self, request: PollRequest) -> Result<Plan<ReadinessEvent>, PollScriptError> {
        let expected = self.script.expected().copied();
        match self.script.respond(&request) {
            Ok(response) => Ok(response),
            Err(ScriptFailure::Exhausted { .. }) => {
                Err(PollScriptError::PlanExhausted { received: request })
            }
            Err(ScriptFailure::Mismatch { .. }) => match expected {
                Some(expected) => Err(PollScriptError::UnexpectedRequest {
                    expected,
                    received: request,
                }),
                None => Err(PollScriptError::PlanExhausted { received: request }),
            },
        }
    }

    /// Returns interest expectations not yet consumed.
    pub fn remaining_steps(&self) -> usize {
        self.script.len()
    }

    /// Returns whether every interest expectation was consumed.
    pub fn is_complete(&self) -> bool {
        self.script.is_empty()
    }
}

impl Default for ScriptedPoller {
    fn default() -> Self {
        Self::new([])
    }
}

fn script_limits(steps: &[ScriptStep<PollRequest, ReadinessEvent>]) -> ScriptLimits {
    let outcomes = steps.iter().fold(0_usize, |total, step| {
        total.saturating_add(step.response().len())
    });
    ScriptLimits::new(steps.len(), outcomes, RetainedBytes::ZERO)
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
