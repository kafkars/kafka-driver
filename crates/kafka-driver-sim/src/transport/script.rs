//! Exact FIFO transport matching with mismatch-safe fault-plan ownership.

use std::{collections::VecDeque, error::Error, fmt};

use crate::{
    FaultPlan, ReadRequest, ReadResult, TransportOutcome, TransportStep, WriteRequest, WriteResult,
};

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
    steps: VecDeque<TransportStep>,
}

impl ScriptedTransport {
    /// Creates a transport that owns the supplied finite plan.
    pub fn new(plan: FaultPlan) -> Self {
        Self {
            steps: plan.into_steps().into(),
        }
    }

    /// Matches and consumes exactly the next bounded read.
    pub fn read(
        &mut self,
        request: ReadRequest,
    ) -> Result<TransportOutcome<ReadResult>, TransportScriptError> {
        let Some(step) = self.steps.front() else {
            return Err(TransportScriptError::ReadPlanExhausted { received: request });
        };
        let TransportStep::Read(step) = step else {
            return Err(TransportScriptError::UnexpectedOperation {
                expected: TransportOperationKind::Write,
                received: TransportOperationKind::Read,
            });
        };
        if step.expected() != request {
            return Err(TransportScriptError::UnexpectedRead {
                expected: step.expected(),
                received: request,
            });
        }
        let Some(TransportStep::Read(step)) = self.steps.pop_front() else {
            return Err(TransportScriptError::ReadPlanExhausted { received: request });
        };
        Ok(step.into_outcome())
    }

    /// Matches and consumes exactly the next offered write bytes.
    pub fn write(
        &mut self,
        request: WriteRequest,
    ) -> Result<TransportOutcome<WriteResult>, TransportScriptError> {
        let Some(step) = self.steps.front() else {
            return Err(TransportScriptError::WritePlanExhausted { received: request });
        };
        let TransportStep::Write(step) = step else {
            return Err(TransportScriptError::UnexpectedOperation {
                expected: TransportOperationKind::Read,
                received: TransportOperationKind::Write,
            });
        };
        if step.expected() != &request {
            return Err(TransportScriptError::UnexpectedWrite {
                expected: step.expected().clone(),
                received: request,
            });
        }
        let Some(TransportStep::Write(step)) = self.steps.pop_front() else {
            return Err(TransportScriptError::WritePlanExhausted { received: request });
        };
        Ok(step.into_outcome())
    }

    /// Returns transport operations not yet consumed.
    pub fn remaining_steps(&self) -> usize {
        self.steps.len()
    }

    /// Returns whether every transport operation was consumed.
    pub fn is_complete(&self) -> bool {
        self.steps.is_empty()
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
