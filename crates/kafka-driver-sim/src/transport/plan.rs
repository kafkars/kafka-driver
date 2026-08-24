//! Validated finite I/O and fault steps for one scripted transport.

use std::{error::Error, fmt};

use criticality::{
    plan::{Plan, Planned},
    script::ScriptStep,
    time::Span,
};

use crate::{ReadRequest, ReadResult, TransportOutcome, WriteRequest, WriteResult};

/// One validated expected read and its independently identified result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadStep {
    expected: ReadRequest,
    response: Plan<TransportOutcome<ReadResult>>,
}

impl ReadStep {
    /// Validates that scripted bytes fit the expected read bound.
    pub fn new(
        expected: ReadRequest,
        outcome: TransportOutcome<ReadResult>,
    ) -> Result<Self, TransportPlanError> {
        Self::with_plan(expected, Plan::single(Planned::new(Span::ZERO, outcome)))
    }

    /// Validates every delayed outcome against the expected read bound.
    pub fn with_plan(
        expected: ReadRequest,
        response: Plan<TransportOutcome<ReadResult>>,
    ) -> Result<Self, TransportPlanError> {
        for planned in response.iter() {
            if let ReadResult::Bytes(bytes) = planned.outcome().result()
                && bytes.len() > expected.max_bytes()
            {
                return Err(TransportPlanError::ReadExceedsRequest {
                    returned: bytes.len(),
                    maximum: expected.max_bytes(),
                });
            }
        }
        Ok(Self { expected, response })
    }

    /// Returns the exact read required to consume this step.
    pub const fn expected(&self) -> ReadRequest {
        self.expected
    }
}

/// One validated expected write and its independently identified result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteStep {
    expected: WriteRequest,
    response: Plan<TransportOutcome<WriteResult>>,
}

impl WriteStep {
    /// Validates that scripted progress does not exceed offered bytes.
    pub fn new(
        expected: WriteRequest,
        outcome: TransportOutcome<WriteResult>,
    ) -> Result<Self, TransportPlanError> {
        Self::with_plan(expected, Plan::single(Planned::new(Span::ZERO, outcome)))
    }

    /// Validates every delayed outcome against the offered write bytes.
    pub fn with_plan(
        expected: WriteRequest,
        response: Plan<TransportOutcome<WriteResult>>,
    ) -> Result<Self, TransportPlanError> {
        for planned in response.iter() {
            if let WriteResult::Written(written) = *planned.outcome().result()
                && written > expected.bytes().len()
            {
                return Err(TransportPlanError::WriteExceedsRequest {
                    written,
                    offered: expected.bytes().len(),
                });
            }
        }
        Ok(Self { expected, response })
    }

    /// Returns the exact write required to consume this step.
    pub const fn expected(&self) -> &WriteRequest {
        &self.expected
    }
}

/// One exact operation in an ordered transport fault plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportStep {
    /// Expected bounded read.
    Read(ReadStep),
    /// Expected exact write.
    Write(WriteStep),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TransportRequest {
    Read(ReadRequest),
    Write(WriteRequest),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TransportResponse {
    Read(TransportOutcome<ReadResult>),
    Write(TransportOutcome<WriteResult>),
}

impl TransportStep {
    fn into_script_step(self) -> ScriptStep<TransportRequest, TransportResponse> {
        match self {
            Self::Read(step) => {
                let response = step
                    .response
                    .into_outcomes()
                    .into_iter()
                    .map(|planned| planned.map(TransportResponse::Read))
                    .collect();
                ScriptStep::new(TransportRequest::Read(step.expected), response)
            }
            Self::Write(step) => {
                let response = step
                    .response
                    .into_outcomes()
                    .into_iter()
                    .map(|planned| planned.map(TransportResponse::Write))
                    .collect();
                ScriptStep::new(TransportRequest::Write(step.expected), response)
            }
        }
    }
}

/// Finite ordered plan containing progress, blocking, closure, or faults.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    steps: Vec<TransportStep>,
}

impl FaultPlan {
    /// Owns a finite validated sequence of transport operations.
    pub fn new(steps: impl IntoIterator<Item = TransportStep>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
        }
    }

    pub(super) fn into_script_steps(self) -> Vec<ScriptStep<TransportRequest, TransportResponse>> {
        self.steps
            .into_iter()
            .map(TransportStep::into_script_step)
            .collect()
    }
}

/// Why an impossible transport result was rejected from a fault plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportPlanError {
    /// Scripted read bytes exceed the expected caller buffer.
    ReadExceedsRequest {
        /// Bytes scripted as returned.
        returned: usize,
        /// Maximum bytes accepted by the read.
        maximum: usize,
    },
    /// Scripted written bytes exceed the expected offered slice.
    WriteExceedsRequest {
        /// Bytes scripted as written.
        written: usize,
        /// Bytes offered by the expected call.
        offered: usize,
    },
}

impl fmt::Display for TransportPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadExceedsRequest { returned, maximum } => write!(
                formatter,
                "scripted read returns {returned} bytes into a {maximum}-byte request"
            ),
            Self::WriteExceedsRequest { written, offered } => write!(
                formatter,
                "scripted write reports {written} bytes from only {offered} offered"
            ),
        }
    }
}

impl Error for TransportPlanError {}
