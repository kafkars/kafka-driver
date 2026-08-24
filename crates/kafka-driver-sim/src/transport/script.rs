//! Exact FIFO transport matching with mismatch-safe fault-plan ownership.

use std::{error::Error, fmt};

use criticality::{
    plan::Plan,
    retained::RetainedBytes,
    script::{ExactScript, ScriptFailure, ScriptLimits, ScriptStep},
};

use super::plan::{TransportRequest, TransportResponse};
use crate::{FaultPlan, ReadRequest, ReadResult, TransportOutcome, WriteRequest, WriteResult};

/// Operation category at the front of a transport fault plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportOperationKind {
    /// Bounded read call.
    Read,
    /// Exact write call.
    Write,
}

/// Finite exact transport simulation driven by a validated fault plan.
#[derive(Clone, Debug)]
pub struct ScriptedTransport {
    script: ExactScript<TransportRequest, TransportResponse>,
}

impl ScriptedTransport {
    /// Creates a transport that owns the supplied finite plan.
    pub fn new(plan: FaultPlan) -> Self {
        let steps = plan.into_script_steps();
        let limits = script_limits(&steps);
        let script = ExactScript::try_with_measure(
            limits,
            steps,
            |_| RetainedBytes::ZERO,
            |_| RetainedBytes::ZERO,
        )
        .unwrap_or_else(|error| panic!("exact transport script must fit derived limits: {error}"));
        Self { script }
    }

    /// Matches and consumes exactly the next bounded read.
    pub fn read(
        &mut self,
        request: ReadRequest,
    ) -> Result<Plan<TransportOutcome<ReadResult>>, TransportScriptError> {
        let expected = self.script.expected().cloned();
        let received = TransportRequest::Read(request);
        let response = self.script.respond(&received);
        match (response, expected, received) {
            (Ok(response), _, _) => Ok(map_read_plan(response)),
            (Err(ScriptFailure::Exhausted { .. }), _, TransportRequest::Read(received))
            | (Err(ScriptFailure::Mismatch { .. }), None, TransportRequest::Read(received)) => {
                Err(TransportScriptError::ReadPlanExhausted { received })
            }
            (
                Err(ScriptFailure::Mismatch { .. }),
                Some(TransportRequest::Read(expected)),
                TransportRequest::Read(received),
            ) => Err(TransportScriptError::UnexpectedRead { expected, received }),
            (
                Err(ScriptFailure::Mismatch { .. }),
                Some(TransportRequest::Write(_)),
                TransportRequest::Read(_),
            ) => Err(TransportScriptError::UnexpectedOperation {
                expected: TransportOperationKind::Write,
                received: TransportOperationKind::Read,
            }),
            (_, _, TransportRequest::Write(_)) => {
                panic!("read call changed its transport request kind")
            }
        }
    }

    /// Matches and consumes exactly the next offered write bytes.
    pub fn write(
        &mut self,
        request: WriteRequest,
    ) -> Result<Plan<TransportOutcome<WriteResult>>, TransportScriptError> {
        let expected = self.script.expected().cloned();
        let received = TransportRequest::Write(request);
        let response = self.script.respond(&received);
        match (response, expected, received) {
            (Ok(response), _, _) => Ok(map_write_plan(response)),
            (Err(ScriptFailure::Exhausted { .. }), _, TransportRequest::Write(received))
            | (Err(ScriptFailure::Mismatch { .. }), None, TransportRequest::Write(received)) => {
                Err(TransportScriptError::WritePlanExhausted { received })
            }
            (
                Err(ScriptFailure::Mismatch { .. }),
                Some(TransportRequest::Read(_)),
                TransportRequest::Write(_),
            ) => Err(TransportScriptError::UnexpectedOperation {
                expected: TransportOperationKind::Read,
                received: TransportOperationKind::Write,
            }),
            (
                Err(ScriptFailure::Mismatch { .. }),
                Some(TransportRequest::Write(expected)),
                TransportRequest::Write(received),
            ) => Err(TransportScriptError::UnexpectedWrite { expected, received }),
            (_, _, TransportRequest::Read(_)) => {
                panic!("write call changed its transport request kind")
            }
        }
    }

    /// Returns transport operations not yet consumed.
    pub fn remaining_steps(&self) -> usize {
        self.script.len()
    }

    /// Returns whether every transport operation was consumed.
    pub fn is_complete(&self) -> bool {
        self.script.is_empty()
    }
}

fn script_limits(steps: &[ScriptStep<TransportRequest, TransportResponse>]) -> ScriptLimits {
    let outcomes = steps.iter().fold(0_usize, |total, step| {
        total.saturating_add(step.response().len())
    });
    ScriptLimits::new(steps.len(), outcomes, RetainedBytes::ZERO)
}

fn map_read_plan(response: Plan<TransportResponse>) -> Plan<TransportOutcome<ReadResult>> {
    response
        .into_outcomes()
        .into_iter()
        .map(|planned| planned.map(read_outcome))
        .collect()
}

fn map_write_plan(response: Plan<TransportResponse>) -> Plan<TransportOutcome<WriteResult>> {
    response
        .into_outcomes()
        .into_iter()
        .map(|planned| planned.map(write_outcome))
        .collect()
}

fn read_outcome(response: TransportResponse) -> TransportOutcome<ReadResult> {
    match response {
        TransportResponse::Read(outcome) => outcome,
        TransportResponse::Write(_) => panic!("read script returned a write outcome"),
    }
}

fn write_outcome(response: TransportResponse) -> TransportOutcome<WriteResult> {
    match response {
        TransportResponse::Write(outcome) => outcome,
        TransportResponse::Read(_) => panic!("write script returned a read outcome"),
    }
}

/// Why an I/O call could not consume the next exact transport step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportScriptError {
    /// A read arrived after the finite plan ended.
    ReadPlanExhausted {
        /// Unscripted read call.
        received: ReadRequest,
    },
    /// A write arrived after the finite plan ended.
    WritePlanExhausted {
        /// Unscripted write call.
        received: WriteRequest,
    },
    /// The caller attempted a different operation category than planned.
    UnexpectedOperation {
        /// Operation category at the plan front.
        expected: TransportOperationKind,
        /// Operation category actually called.
        received: TransportOperationKind,
    },
    /// A read differed from the next exact expectation.
    UnexpectedRead {
        /// Next read required by the plan.
        expected: ReadRequest,
        /// Read actually called.
        received: ReadRequest,
    },
    /// A write differed from the next exact expectation.
    UnexpectedWrite {
        /// Next write required by the plan.
        expected: WriteRequest,
        /// Write actually called.
        received: WriteRequest,
    },
}

impl fmt::Display for TransportScriptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadPlanExhausted { .. } => {
                formatter.write_str("transport fault plan exhausted before read")
            }
            Self::WritePlanExhausted { .. } => {
                formatter.write_str("transport fault plan exhausted before write")
            }
            Self::UnexpectedOperation { expected, received } => write!(
                formatter,
                "transport plan expects {expected:?} before {received:?}"
            ),
            Self::UnexpectedRead { .. } => {
                formatter.write_str("read call did not match next transport step")
            }
            Self::UnexpectedWrite { .. } => {
                formatter.write_str("write call did not match next transport step")
            }
        }
    }
}

impl Error for TransportScriptError {}
