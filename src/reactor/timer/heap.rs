//! Bounded min-heap with stable ordering and eager identity cancellation.

use std::{cmp::Ordering, collections::BinaryHeap, num::NonZeroUsize};

use kafka_driver_core::{Moment, TimerId};

use super::{DeadlineTimer, TimerDrain, TimerScheduleError};

/// Single-owner storage for connection deadlines awaiting reactor delivery.
#[derive(Debug)]
pub(in crate::reactor) struct TimerHeap {
    capacity: NonZeroUsize,
    deadlines: BinaryHeap<ScheduledDeadline>,
    next_sequence: Option<u64>,
}

impl TimerHeap {
    pub(in crate::reactor) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            deadlines: BinaryHeap::with_capacity(capacity.get()),
            next_sequence: Some(0),
        }
    }

    pub(in crate::reactor) fn schedule(
        &mut self,
        deadline: DeadlineTimer,
    ) -> Result<(), TimerScheduleError> {
        if self.contains(deadline.timer_id()) {
            return Err(TimerScheduleError::IdentityInUse {
                timer_id: deadline.timer_id(),
            });
        }
        if self.deadlines.len() >= self.capacity.get() {
            return Err(TimerScheduleError::CapacityReached {
                limit: self.capacity.get(),
            });
        }
        let Some(sequence) = self.next_sequence else {
            return Err(TimerScheduleError::SequenceExhausted);
        };

        self.next_sequence = sequence.checked_add(1);
        self.deadlines
            .push(ScheduledDeadline { deadline, sequence });
        Ok(())
    }

    pub(in crate::reactor) fn cancel(&mut self, timer_id: TimerId) -> bool {
        let retained_before = self.deadlines.len();
        self.deadlines
            .retain(|scheduled| scheduled.deadline.timer_id() != timer_id);
        self.deadlines.len() != retained_before
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.deadlines
            .peek()
            .map(|scheduled| scheduled.deadline.at())
    }

    pub(in crate::reactor) fn drain_due_into(
        &mut self,
        now: Moment,
        destination: &mut Vec<DeadlineTimer>,
        budget: NonZeroUsize,
    ) -> TimerDrain {
        let mut fired = 0;
        while fired < budget.get() && self.next_deadline().is_some_and(|at| at <= now) {
            let Some(scheduled) = self.deadlines.pop() else {
                break;
            };
            destination.push(scheduled.deadline);
            fired += 1;
        }
        TimerDrain::new(fired, self.next_deadline().is_some_and(|at| at <= now))
    }

    pub(in crate::reactor) fn len(&self) -> usize {
        self.deadlines.len()
    }

    fn contains(&self, timer_id: TimerId) -> bool {
        self.deadlines
            .iter()
            .any(|scheduled| scheduled.deadline.timer_id() == timer_id)
    }

    #[cfg(test)]
    pub(super) fn with_next_sequence(capacity: NonZeroUsize, next_sequence: u64) -> Self {
        Self {
            capacity,
            deadlines: BinaryHeap::with_capacity(capacity.get()),
            next_sequence: Some(next_sequence),
        }
    }
}

#[derive(Debug)]
struct ScheduledDeadline {
    deadline: DeadlineTimer,
    sequence: u64,
}

impl PartialEq for ScheduledDeadline {
    fn eq(&self, other: &Self) -> bool {
        self.deadline.at() == other.deadline.at() && self.sequence == other.sequence
    }
}

impl Eq for ScheduledDeadline {}

impl PartialOrd for ScheduledDeadline {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledDeadline {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .at()
            .cmp(&self.deadline.at())
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}
