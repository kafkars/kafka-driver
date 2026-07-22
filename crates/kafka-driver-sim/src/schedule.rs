//! Ordered storage for scripted events with stable same-time delivery.

use std::collections::BTreeMap;

use kafka_driver_core::Moment;

use crate::{Scheduled, SimEventId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScheduleError {
    CapacityReached { limit: usize },
    EventIdsExhausted,
}

#[derive(Clone, Debug)]
pub(crate) struct EventSchedule<E> {
    next_id: Option<u64>,
    max_pending_events: usize,
    events: BTreeMap<(Moment, SimEventId), E>,
}

impl<E> EventSchedule<E> {
    pub(crate) const fn new(max_pending_events: usize) -> Self {
        Self {
            next_id: Some(0),
            max_pending_events,
            events: BTreeMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub(crate) fn schedule(&mut self, at: Moment, event: E) -> Result<SimEventId, ScheduleError> {
        if self.events.len() >= self.max_pending_events {
            return Err(ScheduleError::CapacityReached {
                limit: self.max_pending_events,
            });
        }
        let Some(raw_id) = self.next_id else {
            return Err(ScheduleError::EventIdsExhausted);
        };
        let id = SimEventId::from_raw(raw_id);
        self.next_id = raw_id.checked_add(1);
        self.events.insert((at, id), event);
        Ok(id)
    }

    pub(crate) fn next_at(&self) -> Option<Moment> {
        self.events.keys().next().map(|(at, _)| *at)
    }

    pub(crate) fn pop_next(&mut self) -> Option<Scheduled<E>> {
        let ((at, id), event) = self.events.pop_first()?;
        Some(Scheduled::new(id, at, event))
    }
}
