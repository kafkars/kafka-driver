//! Fair host turn over Bornera mechanics, publications, and semantic admission.

use bornera::{
    ConnectionAccessError, ConnectionEvent, ConnectionToken, EngineOutcome, RegisteredTransport,
};
use kafka_driver_core::Moment;

use crate::reactor::{bornera::KafkaFrame, causality::CausalSequence};

use super::owner::DirectLaneAccess;

#[derive(Clone, Copy)]
pub(super) struct DirectDrivePreparation {
    pub(super) progress: bool,
    pub(super) remaining: usize,
    pub(super) more_due: bool,
}

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn prepare_drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<DirectDrivePreparation> {
        self.mark_waiting();
        let mut progress = self.fire_due_reconnect(now, causality)?;
        progress |= self.fire_due_session_deadline(now)?;
        let submission_budget = self.submission_budget.get();
        let expiration = self.pending.expire_due(now, submission_budget);
        progress |= expiration.settled() != 0;
        let more_due = expiration.more_due();
        progress |= self.settle_pending_recovery(now, causality)?;
        Ok(DirectDrivePreparation {
            progress,
            remaining: submission_budget.saturating_sub(expiration.settled()),
            more_due,
        })
    }

    pub(super) fn drain_after_turn(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<bool> {
        if self.connection.is_none() {
            return Ok(false);
        }
        self.drain_engine(now, causality)
    }

    pub(super) fn finish_drive(
        &mut self,
        preparation: &DirectDrivePreparation,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<bool> {
        let DirectDrivePreparation {
            mut progress,
            remaining,
            more_due,
        } = *preparation;
        progress |= self.settle_pending_recovery(now, causality)?;
        if self.is_terminal() {
            return Ok(progress);
        }
        let admitted = self.admit_pending(now, causality, remaining)?;
        progress |= admitted != 0;
        progress |= self.settle_pending_recovery(now, causality)?;
        if more_due && self.lifecycle.has_live_generation() && !self.is_terminal() {
            self.mark_runnable();
        }
        Ok(progress)
    }

    pub(super) fn capture_turn_failure(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        let connection = self.live_connection()?;
        let report = self.recover_failed_generation(connection, now, Some(causality))?;
        self.capture_recovery(report);
        Ok(())
    }

    fn drain_engine(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<bool> {
        let connection = self.live_connection()?;
        let drained_outcomes = self.set.drain_outcomes(connection).map(collect_drain);
        let outcomes = match drained_outcomes {
            Ok(outcomes) => outcomes,
            Err(ConnectionAccessError::StaleConnection) => {
                self.stale_generation_fatal(now, Some(causality))?;
                return Ok(true);
            }
            Err(_) => {
                let report = self.recover_failed_generation(connection, now, Some(causality))?;
                self.capture_recovery(report);
                return Ok(true);
            }
        };
        let drained_events = self.set.drain_events(connection).map(collect_drain);
        let events = match drained_events {
            Ok(events) => events,
            Err(ConnectionAccessError::StaleConnection) => {
                self.stale_generation_fatal(now, Some(causality))?;
                return Ok(true);
            }
            Err(_) => {
                let report = self.recover_failed_generation(connection, now, Some(causality))?;
                self.capture_recovery(report);
                if let Some(recovery) = self.pending_recovery.as_mut() {
                    prepend(&mut recovery.report.outcomes, outcomes.into_iter());
                }
                return Ok(true);
            }
        };
        let progress = !outcomes.is_empty() || !events.is_empty();
        let mut outcomes = outcomes.into_iter();
        let mut events = events.into_iter();
        while let Some(outcome) = outcomes.next() {
            if let Err(error) = self.settle_outcome(outcome, now, causality, true, None) {
                return Err(self
                    .totalize_drive_failure(error, connection, now, causality, outcomes, events));
            }
            if let Some(recovery) = self.pending_recovery.as_mut() {
                prepend(&mut recovery.report.outcomes, outcomes);
                prepend(&mut recovery.report.events, events);
                return Ok(true);
            }
        }
        while let Some(event) = events.next() {
            if let Err(error) = self.settle_event(event, now, causality) {
                return Err(self
                    .totalize_drive_failure(error, connection, now, causality, outcomes, events));
            }
            if let Some(recovery) = self.pending_recovery.as_mut() {
                prepend(&mut recovery.report.events, events);
                return Ok(true);
            }
        }
        Ok(progress)
    }

    fn totalize_drive_failure(
        &mut self,
        error: std::io::Error,
        connection: ConnectionToken,
        now: Moment,
        causality: &mut CausalSequence,
        outcomes: impl Iterator<Item = EngineOutcome<KafkaFrame>>,
        events: impl Iterator<Item = ConnectionEvent>,
    ) -> std::io::Error {
        if self.pending_recovery.is_none() && self.connection == Some(connection) {
            let Ok(report) = self.abandon_generation(
                connection,
                bornera::OwnerFailure::OwnerInvariant,
                now,
                Some(causality),
            ) else {
                return error;
            };
            self.capture_diverged_recovery(report);
        }
        let Some(recovery) = self.pending_recovery.as_mut() else {
            return self.host_fatal(error);
        };
        recovery.semantic_diverged = true;
        prepend(&mut recovery.report.outcomes, outcomes);
        prepend(&mut recovery.report.events, events);
        let _ = self.settle_pending_recovery(now, causality);
        error
    }

    pub(super) fn settle_pending_recovery(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<bool> {
        let Some(report) = self.pending_recovery.take() else {
            return Ok(false);
        };
        self.settle_recovery(report, now, causality)?;
        Ok(true)
    }
}

pub(super) fn prepend<T>(target: &mut Vec<T>, prefix: impl Iterator<Item = T>) {
    let suffix = std::mem::take(target);
    target.extend(prefix);
    target.extend(suffix);
}

fn collect_drain<T>(items: impl Iterator<Item = T>) -> Vec<T> {
    items.collect()
}
