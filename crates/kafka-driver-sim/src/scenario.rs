//! Test-only compatibility over Criticality's canonical event timeline.

use criticality::{
    plan::Planned,
    retained::RetainedBytes,
    time::Moment as CriticalityMoment,
    timeline::{EventToken, Timeline, TimelineId, TimelineLimits},
};
use kafka_driver_core::Moment;

const MAX_PENDING_EVENTS: usize = 1_024;
const MAX_RETAINED_BYTES: u64 = 16 * 1_024 * 1_024;

#[derive(Debug)]
pub(crate) struct Scenario<E> {
    timeline: Timeline<E>,
}

impl<E> Scenario<E> {
    pub(crate) fn new() -> Self {
        Self {
            timeline: Timeline::with_measure(
                TimelineId::new(1),
                TimelineLimits::new(MAX_PENDING_EVENTS, RetainedBytes::new(MAX_RETAINED_BYTES)),
                |_| RetainedBytes::ZERO,
            ),
        }
    }

    pub(crate) fn now(&self) -> Moment {
        Moment::from_nanos(self.timeline.now().tick())
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.timeline.is_empty()
    }

    pub(crate) fn schedule_at(&mut self, at: Moment, event: E) -> Result<EventToken, E> {
        self.timeline
            .schedule_at(CriticalityMoment::from_tick(at.as_nanos()), event)
            .map_err(criticality::timeline::ScheduleError::into_event)
    }

    pub(crate) fn schedule_planned(&mut self, planned: Planned<E>) -> Result<EventToken, E> {
        self.timeline
            .schedule_planned(planned)
            .map_err(criticality::timeline::ScheduleError::into_event)
    }

    pub(crate) fn cancel(&mut self, token: EventToken) -> Option<E> {
        self.timeline.cancel(token)
    }

    pub(crate) fn next_event(&mut self) -> Option<(Moment, E)> {
        self.timeline.pop_next().map(|delivery| {
            (
                Moment::from_nanos(delivery.at().tick()),
                delivery.into_event(),
            )
        })
    }
}
