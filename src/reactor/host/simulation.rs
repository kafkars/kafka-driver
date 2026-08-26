//! Modeled capability boundary for running the production duty deterministically.

use std::{io, sync::Arc, time::Instant};

use crate::{
    api::CallIds, completion::ShutdownRequester, config::DriverLimits, observation::Observation,
    reactor::MailboxSender,
};

use super::{Command, Reactor};

impl Reactor {
    pub(super) fn new_simulated(
        limits: &DriverLimits,
        address: std::net::SocketAddr,
        origin: Instant,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
    ) -> io::Result<(MailboxSender<Command>, ShutdownRequester, Self)> {
        Self::new_test_at(limits, address, origin, call_ids, observation)
    }

    pub(super) fn simulate_connect(&mut self) -> bool {
        self.backend
            .direct_mut()
            .is_some_and(super::super::direct_plaintext::DirectBackend::simulate_connect)
    }

    pub(super) fn simulate_receive(&mut self, bytes: &[u8]) -> bool {
        self.backend.direct_mut().is_some_and(|backend| {
            super::super::direct_plaintext::DirectBackend::simulate_receive(backend, bytes)
        })
    }

    pub(super) fn take_simulated_frames(&mut self) -> Vec<Vec<u8>> {
        self.backend.direct_mut().map_or_else(Vec::new, |backend| {
            super::super::direct_plaintext::DirectBackend::take_simulated_frames(backend)
        })
    }
}
