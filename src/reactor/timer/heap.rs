//! Kafka timer identities adapted onto Calandria's bounded timer queue.

use std::num::NonZeroUsize;

use calandria::{
    Deadline, RetainedBytes, Timer, TimerId as CalandriaTimerId, TimerLimits, TimerOwnerId,
    TimerQueue, TimerScheduleFailure, TimerToken,
};
use kafka_driver_core::{Moment, TimerId};

use super::{DeadlineTimer, TimerDrain, TimerScheduleError};

/// Single-owner storage for connection deadlines awaiting reactor delivery.
#[derive(Debug)]
pub(in crate::reactor) struct TimerHeap {
    queue: TimerQueue<DeadlineTimer>,
}

impl TimerHeap {
    pub(in crate::reactor) fn new(capacity: NonZeroUsize) -> Self {
        Self::starting_at(capacity, CalandriaTimerId::ZERO)
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
        self.queue
            .schedule(calandria_deadline(deadline.at()), deadline)
            .map_err(map_schedule_error)?;
        Ok(())
    }

    pub(in crate::reactor) fn cancel(&mut self, timer_id: TimerId) -> bool {
        let Some(token) = self.token_for(timer_id) else {
            return false;
        };
        self.queue.cancel(token).is_some()
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.queue
            .next_deadline()
            .map(|deadline| Moment::from_nanos(deadline.moment().as_nanos()))
    }

    pub(in crate::reactor) fn drain_due_into(
        &mut self,
        now: Moment,
        destination: &mut Vec<Timer<DeadlineTimer>>,
        budget: NonZeroUsize,
    ) -> TimerDrain {
        self.queue.drain_due_into(
            calandria::Moment::from_nanos(now.as_nanos()),
            destination,
            budget,
        )
    }

    #[cfg(test)]
    pub(in crate::reactor) fn len(&self) -> usize {
        self.queue.len()
    }

    fn contains(&self, timer_id: TimerId) -> bool {
        self.token_for(timer_id).is_some()
    }

    fn token_for(&self, timer_id: TimerId) -> Option<TimerToken> {
        self.queue
            .iter()
            .find(|timer| timer.value().timer_id() == timer_id)
            .map(Timer::token)
    }

    fn starting_at(capacity: NonZeroUsize, first_id: CalandriaTimerId) -> Self {
        let limits = TimerLimits::new(capacity, RetainedBytes::ZERO);
        Self {
            queue: TimerQueue::starting_at_with_measure(
                TimerOwnerId::new(0),
                limits,
                first_id,
                |_| RetainedBytes::ZERO,
            ),
        }
    }

    #[cfg(test)]
    pub(super) fn with_next_sequence(capacity: NonZeroUsize, next_sequence: u64) -> Self {
        Self::starting_at(capacity, CalandriaTimerId::new(next_sequence))
    }
}

fn calandria_deadline(moment: Moment) -> Deadline {
    Deadline::at(calandria::Moment::from_nanos(moment.as_nanos()))
}

fn map_schedule_error(error: calandria::TimerScheduleError<DeadlineTimer>) -> TimerScheduleError {
    let (_, _, failure) = error.into_parts();
    match failure {
        TimerScheduleFailure::TimerCapacity { limit } => {
            TimerScheduleError::CapacityReached { limit: limit.get() }
        }
        TimerScheduleFailure::TimerIdsExhausted => TimerScheduleError::SequenceExhausted,
        TimerScheduleFailure::RetainedByteOverflow { .. }
        | TimerScheduleFailure::RetainedByteCapacity { .. } => {
            panic!("zero-sized driver timer exceeded Calandria's zero-byte limit")
        }
    }
}
