//! Exclusive lifecycle timestamps transferred with one public call.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::RequestError;

use super::Observation;

pub(crate) struct CallTimeline {
    observation: Arc<Observation>,
    submitted: Instant,
    deadline: Option<Instant>,
    reactor: Option<Instant>,
    routed: Option<Instant>,
    prepared: Option<Instant>,
    writer: Option<Instant>,
}

impl CallTimeline {
    pub(crate) fn new(
        observation: Arc<Observation>,
        submitted: Instant,
        timeout: Duration,
    ) -> Self {
        Self {
            observation,
            submitted,
            deadline: submitted.checked_add(timeout),
            reactor: None,
            routed: None,
            prepared: None,
            writer: None,
        }
    }

    pub(crate) fn until(
        observation: Arc<Observation>,
        submitted: Instant,
        deadline: Instant,
    ) -> Self {
        Self {
            observation,
            submitted,
            deadline: Some(deadline),
            reactor: None,
            routed: None,
            prepared: None,
            writer: None,
        }
    }

    pub(crate) fn mark_reactor(&mut self, at: Instant) {
        if self.reactor.is_none() {
            self.observation.admit();
            self.reactor = Some(at);
        }
    }

    pub(crate) fn mark_routed(&mut self, at: Instant) {
        self.routed.get_or_insert(at);
    }

    pub(crate) fn mark_prepared(&mut self, at: Instant) {
        self.prepared.get_or_insert(at);
    }

    pub(crate) fn mark_writer(&mut self, at: Instant) {
        self.writer.get_or_insert(at);
    }

    pub(crate) fn finish(self, outcome: CallOutcome<'_>, delivered: bool) {
        let now = Instant::now();
        let durations = CallDurations {
            mailbox: between(self.submitted, self.reactor),
            routing: between_option(self.reactor, self.routed),
            preparation: between_option(self.routed, self.prepared),
            writer_admission: between_option(self.prepared, self.writer),
            in_flight: between_option(self.writer, Some(now)),
            end_to_end: now.saturating_duration_since(self.submitted),
            deadline_lateness: deadline_lateness(outcome, self.deadline, now),
        };
        self.observation.finish(outcome, delivered, durations);
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CallOutcome<'a> {
    Succeeded,
    Failed(&'a RequestError),
}

pub(super) struct CallDurations {
    pub(super) mailbox: Option<Duration>,
    pub(super) routing: Option<Duration>,
    pub(super) preparation: Option<Duration>,
    pub(super) writer_admission: Option<Duration>,
    pub(super) in_flight: Option<Duration>,
    pub(super) end_to_end: Duration,
    pub(super) deadline_lateness: Option<Duration>,
}

fn between(start: Instant, end: Option<Instant>) -> Option<Duration> {
    end.map(|end| end.saturating_duration_since(start))
}

fn between_option(start: Option<Instant>, end: Option<Instant>) -> Option<Duration> {
    start
        .zip(end)
        .map(|(start, end)| end.saturating_duration_since(start))
}

fn deadline_lateness(
    outcome: CallOutcome<'_>,
    deadline: Option<Instant>,
    completed: Instant,
) -> Option<Duration> {
    let CallOutcome::Failed(RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::DeadlineExceeded,
        ..
    }) = outcome
    else {
        return None;
    };
    deadline.map(|deadline| completed.saturating_duration_since(deadline))
}
