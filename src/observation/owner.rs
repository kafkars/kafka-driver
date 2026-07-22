//! Shared atomic ownership for cumulative public-call observation.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{CallCounters, CallLatencySnapshot, FailureCounters};

use super::{
    latency::{LatencyCounter, increment},
    timeline::{CallDurations, CallOutcome},
};

#[derive(Debug, Default)]
pub(crate) struct Observation {
    pub(super) admitted: AtomicU64,
    pub(super) succeeded: AtomicU64,
    pub(super) failed: AtomicU64,
    pub(super) receiver_abandoned: AtomicU64,
    pub(super) not_sent: AtomicU64,
    pub(super) possibly_sent: AtomicU64,
    pub(super) dns: AtomicU64,
    pub(super) connect: AtomicU64,
    pub(super) transport: AtomicU64,
    pub(super) negotiation: AtomicU64,
    pub(super) authentication: AtomicU64,
    pub(super) deadline: AtomicU64,
    pub(super) local_rejection: AtomicU64,
    pub(super) response_capacity: AtomicU64,
    pub(super) route_capacity: AtomicU64,
    mailbox: LatencyCounter,
    routing: LatencyCounter,
    preparation: LatencyCounter,
    writer_admission: LatencyCounter,
    in_flight: LatencyCounter,
    end_to_end: LatencyCounter,
    deadline_lateness: LatencyCounter,
}

impl Observation {
    pub(super) fn admit(&self) {
        increment(&self.admitted);
    }

    pub(super) fn finish(
        &self,
        outcome: CallOutcome<'_>,
        delivered: bool,
        durations: CallDurations,
    ) {
        match outcome {
            CallOutcome::Succeeded => increment(&self.succeeded),
            CallOutcome::Failed(failure) => {
                increment(&self.failed);
                self.classify_failure(failure);
            }
        }
        if !delivered {
            increment(&self.receiver_abandoned);
        }
        durations.record(self);
    }

    pub(crate) fn snapshot(&self) -> ObservationSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        ObservationSnapshot {
            calls: CallCounters::new([
                load(&self.admitted),
                load(&self.succeeded),
                load(&self.failed),
                load(&self.receiver_abandoned),
                load(&self.not_sent),
                load(&self.possibly_sent),
            ]),
            failures: FailureCounters::new([
                load(&self.dns),
                load(&self.connect),
                load(&self.transport),
                load(&self.negotiation),
                load(&self.authentication),
                load(&self.deadline),
                load(&self.local_rejection),
                load(&self.response_capacity),
                load(&self.route_capacity),
            ]),
            latency: CallLatencySnapshot::new([
                self.mailbox.snapshot(),
                self.routing.snapshot(),
                self.preparation.snapshot(),
                self.writer_admission.snapshot(),
                self.in_flight.snapshot(),
                self.end_to_end.snapshot(),
                self.deadline_lateness.snapshot(),
            ]),
        }
    }
}

pub(crate) struct ObservationSnapshot {
    pub(crate) calls: CallCounters,
    pub(crate) failures: FailureCounters,
    pub(crate) latency: CallLatencySnapshot,
}

impl CallDurations {
    fn record(self, observation: &Observation) {
        record(self.mailbox, &observation.mailbox);
        record(self.routing, &observation.routing);
        record(self.preparation, &observation.preparation);
        record(self.writer_admission, &observation.writer_admission);
        record(self.in_flight, &observation.in_flight);
        record(Some(self.end_to_end), &observation.end_to_end);
        record(self.deadline_lateness, &observation.deadline_lateness);
    }
}

fn record(duration: Option<std::time::Duration>, counter: &LatencyCounter) {
    if let Some(duration) = duration {
        counter.record(duration);
    }
}
