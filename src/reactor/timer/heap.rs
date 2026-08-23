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
    tokens: Vec<(TimerId, TimerToken)>,
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
        let timer_id = deadline.timer_id();
        let token = self
            .queue
            .schedule(calandria_deadline(deadline.at()), deadline)
            .map_err(map_schedule_error)?;
        self.tokens.push((timer_id, token));
        Ok(())
    }

    pub(in crate::reactor) fn cancel(&mut self, timer_id: TimerId) -> bool {
        let Some(position) = self
            .tokens
            .iter()
            .position(|(candidate, _)| *candidate == timer_id)
        else {
            return false;
        };
        let (_, token) = self.tokens.swap_remove(position);
        self.queue
            .cancel(token)
            .unwrap_or_else(|| panic!("driver timer token diverged from Calandria queue"));
        true
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
        let retained = destination.len();
        let drain = self.queue.drain_due_into(
            calandria::Moment::from_nanos(now.as_nanos()),
            destination,
            budget,
        );
        for timer in &destination[retained..] {
            self.forget_token(timer.token());
        }
        drain
    }

    #[cfg(test)]
    pub(in crate::reactor) fn len(&self) -> usize {
        self.queue.len()
    }

    fn contains(&self, timer_id: TimerId) -> bool {
        self.tokens
            .iter()
            .any(|(candidate, _)| *candidate == timer_id)
    }

    fn forget_token(&mut self, token: TimerToken) {
        let Some(position) = self
            .tokens
            .iter()
            .position(|(_, candidate)| *candidate == token)
        else {
            panic!("delivered Calandria timer has no driver identity");
        };
        self.tokens.swap_remove(position);
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
            tokens: Vec::with_capacity(capacity.get()),
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
