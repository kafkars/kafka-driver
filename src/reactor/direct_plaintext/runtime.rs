//! Capacity-one hosting facade over one shared set and one reusable Kafka lane.

use std::{
    io,
    ops::{Deref, DerefMut},
};

use bornera::{ConnectionSet, ConnectionSetConfig, RegisteredTransport};
use calandria::{Next, ResourceOwnerId, Span, Turn, WaitOutcome};
use kafka_driver_core::Moment;

use crate::{
    SeedSnapshot,
    config::DriverLimits,
    reactor::{
        causality::CausalSequence,
        scram_proof::{ScramProofOutcome, ScramProofSender},
    },
    request::ErasedRequest,
};

use super::{
    limits::set_limits,
    owner::{
        DirectLane, DirectLaneAccess, DirectLaneView, DirectSet, ID, calandria_moment, message,
    },
};

pub(super) fn new_set<T: RegisteredTransport>(driver: &DriverLimits) -> io::Result<DirectSet<T>> {
    ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(ID)),
        set_limits(driver),
    )
    .map_err(message)
}

pub(in crate::reactor) struct DirectRuntime<T: RegisteredTransport> {
    pub(super) set: DirectSet<T>,
    pub(super) lane: DirectLane<T>,
    pub(super) last_turn: Turn,
}

impl<T: RegisteredTransport> DirectRuntime<T> {
    pub(super) fn access(&mut self) -> DirectLaneAccess<'_, T> {
        DirectLaneAccess {
            lane: &mut self.lane,
            set: &mut self.set,
        }
    }

    pub(super) fn view(&self) -> DirectLaneView<'_, T> {
        DirectLaneView {
            lane: &self.lane,
            set: &self.set,
        }
    }

    pub(in crate::reactor) fn submit(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        self.access().submit_request(request, now, causality)
    }

    pub(in crate::reactor) fn drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let preparation = self.access().prepare_drive(now, causality)?;
        if !preparation.should_turn {
            return Ok(preparation.progress);
        }
        let (turn_succeeded, set_progress) =
            if let Ok(turn) = self.set.turn_component(calandria_moment(now)) {
                let progress = turn.work().get() != 0;
                self.last_turn = turn;
                (true, progress)
            } else {
                self.last_turn = Turn::waiting();
                self.access().capture_turn_failure(now, causality)?;
                (false, true)
            };
        let lane_progress =
            self.access()
                .finish_drive(&preparation, turn_succeeded, now, causality)?;
        Ok(set_progress || lane_progress)
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        match self.set.poll_io(maximum) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let primary = message(error);
                let Some(connection) = self.lane.connection else {
                    let _ = self.access().generation_invariant_fatal(
                        Moment::ORIGIN,
                        None,
                        "Bornera readiness failed without a live direct generation",
                    );
                    return Err(primary);
                };
                match self
                    .access()
                    .recover_failed_generation(connection, Moment::ORIGIN, None)
                {
                    Ok(report) => {
                        self.access().capture_recovery(report);
                        Ok(WaitOutcome::Notified)
                    }
                    Err(_) => Err(primary),
                }
            }
        }
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        self.set.wake_handle()
    }

    pub(in crate::reactor) fn pulse_handle(&self) -> bornera::ConnectionPulseHandle {
        self.set.pulse_handle()
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        if self.lane.is_terminal() {
            return None;
        }
        let engine = self
            .lane
            .lifecycle
            .has_live_generation()
            .then(|| match self.last_turn.next() {
                Next::Now => Some(Moment::from_nanos(0)),
                Next::WakeOr(deadline) => Some(Moment::from_nanos(deadline.moment().as_nanos())),
                Next::Wake | Next::Stop => None,
            })
            .flatten();
        engine
            .into_iter()
            .chain(self.lane.lifecycle.next_deadline())
            .chain(
                self.lane
                    .lifecycle
                    .has_live_generation()
                    .then_some(self.lane.session_deadline)
                    .flatten(),
            )
            .chain(self.lane.pending.next_deadline())
            .min()
    }

    pub(in crate::reactor) fn has_local_work(&self) -> bool {
        !self.lane.is_terminal()
            && (self.lane.pending_recovery.is_some()
                || self.lane.runnable
                || (self.lane.lifecycle.has_live_generation()
                    && matches!(self.last_turn.next(), Next::Now))
                || (self.lane.admission_open && !self.lane.pending.is_empty()))
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.lane.is_terminal()
    }

    pub(in crate::reactor) fn begin_session_drain(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        self.access().begin_session_drain(now, causality)
    }

    pub(in crate::reactor) fn fire_due_session_deadline(
        &mut self,
        now: Moment,
    ) -> io::Result<bool> {
        self.access().fire_due_session_deadline(now)
    }

    pub(in crate::reactor) fn complete_scram_proof(
        &mut self,
        outcome: ScramProofOutcome,
        now: Moment,
    ) -> io::Result<bool> {
        self.access().complete_scram_proof(outcome, now)
    }

    pub(in crate::reactor) fn install_scram_proof_sender(&mut self, sender: ScramProofSender) {
        self.access().install_scram_proof_sender(sender);
    }

    pub(in crate::reactor) fn release_scram_proof_sender(&mut self) {
        self.access().release_scram_proof_sender();
    }

    pub(in crate::reactor) fn seed_snapshot(&self) -> Option<SeedSnapshot> {
        self.view().seed_snapshot()
    }

    #[cfg(test)]
    pub(in crate::reactor) fn selector_registrations(&self) -> usize {
        self.set.snapshot().poller.registrations()
    }
}

impl<T: RegisteredTransport> Deref for DirectRuntime<T> {
    type Target = DirectLane<T>;

    fn deref(&self) -> &Self::Target {
        &self.lane
    }
}

impl<T: RegisteredTransport> DerefMut for DirectRuntime<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.lane
    }
}
