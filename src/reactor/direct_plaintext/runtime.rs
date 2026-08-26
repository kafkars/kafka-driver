//! Capacity-one public facade over the reusable shared-set lane coordinator.

use std::io;

use bornera::RegisteredTransport;
use calandria::{Span, WaitOutcome};
use kafka_driver_core::Moment;

use crate::{
    SeedSnapshot,
    reactor::{
        causality::CausalSequence,
        scram_proof::{ScramProofOutcome, ScramProofSender},
    },
    request::ErasedRequest,
};

use super::{
    DirectBrokerRpc,
    owner::{DirectLane, DirectLaneAccess, DirectLaneView},
    set_owner::DirectSetOwner,
};

pub(in crate::reactor) struct DirectRuntime<T: RegisteredTransport> {
    pub(super) connections: DirectSetOwner<T>,
    pub(super) lane: DirectLane<T>,
}

impl<T: RegisteredTransport> DirectRuntime<T> {
    pub(super) fn access(&mut self) -> DirectLaneAccess<'_, T> {
        self.connections.access(&mut self.lane)
    }

    #[allow(
        dead_code,
        reason = "the selector-neutral RPC facade is activated by the pending cluster cutover"
    )]
    pub(in crate::reactor) fn rpc<'lane, 'cause>(
        &'lane mut self,
        causality: &'cause mut CausalSequence,
    ) -> DirectBrokerRpc<'lane, 'cause, T> {
        DirectBrokerRpc::new(self.access(), causality)
    }

    pub(super) fn view(&self) -> DirectLaneView<'_, T> {
        self.connections.view(&self.lane)
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
        self.connections
            .drive(std::slice::from_mut(&mut self.lane), now, causality)
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        self.connections
            .wait(std::slice::from_mut(&mut self.lane), maximum)
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        self.connections.wake_handle()
    }

    pub(in crate::reactor) fn pulse_handle(&self) -> bornera::ConnectionPulseHandle {
        self.connections.pulse_handle()
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.connections
            .next_deadline(std::slice::from_ref(&self.lane))
    }

    pub(in crate::reactor) fn has_local_work(&self) -> bool {
        self.connections
            .has_local_work(std::slice::from_ref(&self.lane))
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
        self.connections.snapshot().poller.registrations()
    }
}
