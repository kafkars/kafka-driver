//! Fairness-bounded delivery of driver-relative deadline events to policy.

use kafka_driver_core::{ConnectionEffect, ConnectionInput, Moment};

use crate::reactor::{
    Poller,
    timer::{DeadlineSubject, DeadlineTimer},
};

use super::{BrokerError, owner::SingleBroker};

/// Progress from one bounded due-deadline delivery phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct DeadlineProgress {
    fired: usize,
    more_due: bool,
}

impl DeadlineProgress {
    pub(in crate::reactor) const fn idle() -> Self {
        Self {
            fired: 0,
            more_due: false,
        }
    }

    pub(in crate::reactor) const fn made_progress(self) -> bool {
        self.fired != 0
    }

    pub(in crate::reactor) const fn more_due(self) -> bool {
        self.more_due
    }
}

impl SingleBroker {
    pub(in crate::reactor) fn fire_due(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<DeadlineProgress, BrokerError> {
        let drain = self
            .timers
            .drain_due_into(now, &mut self.due_timers, self.timer_budget);
        let mut due = std::mem::take(&mut self.due_timers);
        for deadline in due.drain(..) {
            self.deliver_deadline(poller, now, deadline)?;
            self.reconcile_connection(poller, now)?;
        }
        self.due_timers = due;
        Ok(DeadlineProgress {
            fired: drain.fired(),
            more_due: drain.more_due(),
        })
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.timers.next_deadline()
    }

    fn deliver_deadline(
        &mut self,
        poller: &Poller,
        now: Moment,
        deadline: DeadlineTimer,
    ) -> Result<(), BrokerError> {
        if deadline.subject() == DeadlineSubject::Reconnect {
            return self.deliver_reconnect(poller, deadline.epoch(), deadline.timer_id(), now);
        }
        let transition = self.connection.apply(ConnectionInput::DeadlineElapsed {
            epoch: deadline.epoch(),
            timer_id: deadline.timer_id(),
            now,
        })?;
        let effects = transition.into_effects();
        if matches!(deadline.subject(), DeadlineSubject::Call(_))
            && let [
                ConnectionEffect::ScheduleDeadline {
                    epoch,
                    call_id,
                    timer_id,
                    at,
                },
            ] = effects.as_slice()
        {
            return self
                .timers
                .schedule(DeadlineTimer::for_call(*timer_id, *epoch, *call_id, *at))
                .map_err(Into::into);
        }
        if deadline.subject() == DeadlineSubject::Negotiation
            && let [
                ConnectionEffect::ScheduleNegotiationDeadline {
                    epoch,
                    timer_id,
                    at,
                },
            ] = effects.as_slice()
        {
            return self
                .timers
                .schedule(DeadlineTimer::for_negotiation(*timer_id, *epoch, *at))
                .map_err(Into::into);
        }
        if deadline.subject() == DeadlineSubject::Authentication
            && let [
                ConnectionEffect::Authentication {
                    effect:
                        kafka_driver_core::AuthenticationEffect::ScheduleDeadline {
                            epoch,
                            timer_id,
                            at,
                        },
                },
            ] = effects.as_slice()
        {
            return self
                .timers
                .schedule(DeadlineTimer::for_authentication(*timer_id, *epoch, *at))
                .map_err(Into::into);
        }
        self.interpret_close(poller, effects, None)
    }
}
