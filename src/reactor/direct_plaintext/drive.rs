//! Fair host turn over Bornera mechanics, publications, and semantic admission.

use bornera::RegisteredTransport;
use kafka_driver_core::Moment;

use crate::reactor::causality::CausalSequence;

use super::owner::{DirectOwner, calandria_moment, message};

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(in crate::reactor) fn drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<bool> {
        let expiration = self.pending.expire_due(now, self.submission_budget.get());
        let mut progress = expiration.settled() != 0;
        let more_due = expiration.more_due();
        progress |= self.settle_pending_recovery(causality)?;
        if self.terminal {
            return Ok(progress);
        }
        progress |= self.drive_engine(now, causality)?;
        progress |= self.settle_pending_recovery(causality)?;
        if self.terminal {
            return Ok(progress);
        }
        let remaining = self
            .submission_budget
            .get()
            .saturating_sub(expiration.settled());
        let admitted = self.admit_pending(now, causality, remaining)?;
        progress |= admitted != 0;
        progress |= self.settle_pending_recovery(causality)?;
        if more_due && !self.terminal {
            self.mark_runnable();
        }
        Ok(progress)
    }

    fn drive_engine(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<bool> {
        let Ok(turn) = self.set.turn_component(calandria_moment(now)) else {
            self.pending_recovery = Some(self.set.try_recover(self.connection).map_err(message)?);
            return Ok(true);
        };
        self.last_turn = turn;
        let outcomes: Vec<_> = self
            .set
            .drain_outcomes(self.connection)
            .map_err(message)?
            .collect();
        let events: Vec<_> = self
            .set
            .drain_events(self.connection)
            .map_err(message)?
            .collect();
        let progress =
            self.last_turn.work().get() != 0 || !outcomes.is_empty() || !events.is_empty();
        for outcome in outcomes {
            self.settle_outcome(outcome, now, causality, true)?;
        }
        if self.pending_recovery.is_none() {
            for event in events {
                self.settle_event(event, now, causality)?;
            }
        }
        Ok(progress)
    }

    fn settle_pending_recovery(&mut self, causality: &mut CausalSequence) -> std::io::Result<bool> {
        let Some(report) = self.pending_recovery.take() else {
            return Ok(false);
        };
        self.settle_recovery(report, causality)?;
        Ok(true)
    }
}
