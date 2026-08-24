//! Expected interest arms and independently identified readiness outcomes.

use std::time::Duration;

use criticality::{
    plan::{Plan, Planned},
    script::ScriptStep,
    time::Span,
};
use kafka_driver_core::{ConnectionEpoch, TransportId};

use crate::{PollInterest, Readiness};

/// One transport interest arm expected by a deterministic poller script.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PollRequest {
    epoch: ConnectionEpoch,
    transport_id: TransportId,
    interest: PollInterest,
}

impl PollRequest {
    /// Creates an expected interest arm.
    pub const fn new(
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        interest: PollInterest,
    ) -> Self {
        Self {
            epoch,
            transport_id,
            interest,
        }
    }

    /// Returns the connection epoch arming the interest.
    pub const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the transport being armed.
    pub const fn transport_id(self) -> TransportId {
        self.transport_id
    }

    /// Returns useful readiness for the transport's current state.
    pub const fn interest(self) -> PollInterest {
        self.interest
    }
}

/// Scripted readiness whose identities may intentionally be stale.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessEvent {
    epoch: ConnectionEpoch,
    transport_id: TransportId,
    readiness: Readiness,
}

impl ReadinessEvent {
    /// Creates an explicitly identified readiness observation.
    pub const fn new(
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        readiness: Readiness,
    ) -> Self {
        Self {
            epoch,
            transport_id,
            readiness,
        }
    }

    /// Returns the epoch carried by the observation.
    pub const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the transport carried by the observation.
    pub const fn transport_id(self) -> TransportId {
        self.transport_id
    }

    /// Returns the observed readiness flags.
    pub const fn readiness(self) -> Readiness {
        self.readiness
    }
}

/// One exact interest expectation and delayed readiness result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PollStep {
    expected: PollRequest,
    response: Plan<ReadinessEvent>,
}

impl PollStep {
    /// Creates one deterministic poller step.
    pub fn new(expected: PollRequest, delay: Duration, event: ReadinessEvent) -> Self {
        Self::with_plan(
            expected,
            Plan::single(Planned::new(
                Span::from_ticks(u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX)),
                event,
            )),
        )
    }

    /// Creates one poller script step with zero or more delayed outcomes.
    pub const fn with_plan(expected: PollRequest, response: Plan<ReadinessEvent>) -> Self {
        Self { expected, response }
    }

    /// Returns the exact interest arm required to consume this step.
    pub const fn expected(&self) -> PollRequest {
        self.expected
    }

    pub(super) fn into_script_step(self) -> ScriptStep<PollRequest, ReadinessEvent> {
        ScriptStep::new(self.expected, self.response)
    }
}
