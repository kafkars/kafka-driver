//! Test-only compatibility over Calandria's canonical event timeline.

use calandria::{Moment as CalandriaMoment, RetainedBytes};
use calandria_sim::{Planned, Timeline, TimelineId, TimelineLimits};
use kafka_driver_core::Moment;

#[derive(Debug)]
pub(crate) struct Scenario<E> {
    timeline: Timeline<E>,
}

impl<E> Scenario<E> {
    pub(crate) fn new() -> Self {
        Self {
            timeline: Timeline::with_measure(TimelineId::new(1), TimelineLimits::default(), |_| {
                RetainedBytes::ZERO
            }),
        }
    }

    pub(crate) fn now(&self) -> Moment {
        Moment::from_nanos(self.timeline.now().as_nanos())
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.timeline.is_empty()
    }

    pub(crate) fn schedule_at(&mut self, at: Moment, event: E) -> Result<(), E> {
        self.timeline
            .schedule_at(CalandriaMoment::from_nanos(at.as_nanos()), event)
            .map(|_| ())
            .map_err(calandria_sim::ScheduleError::into_event)
    }

    pub(crate) fn schedule_planned(&mut self, planned: Planned<E>) -> Result<(), E> {
        self.timeline
            .schedule_planned(planned)
            .map(|_| ())
            .map_err(calandria_sim::ScheduleError::into_event)
    }

    pub(crate) fn next_event(&mut self) -> Option<(Moment, E)> {
        self.timeline.pop_next().map(|delivery| {
            (
                Moment::from_nanos(delivery.at().as_nanos()),
                delivery.into_event(),
            )
        })
    }
}
