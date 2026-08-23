//! Modeled capability boundary for running the production duty deterministically.

use std::{io, sync::Arc, time::Instant};

use kafka_driver_core::Moment;

use crate::{
    api::CallIds,
    config::{BrokerConfig, DriverLimits},
    observation::Observation,
    reactor::{MailboxSender, PollEvent, Readiness},
};

use super::{Command, Reactor, ShutdownRequester};

impl Reactor {
    pub(super) fn new_simulated(
        limits: &DriverLimits,
        config: BrokerConfig,
        origin: Instant,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
    ) -> io::Result<(MailboxSender<Command>, ShutdownRequester, Self)> {
        let (commands, shutdown, mut reactor) = Self::new(limits, None, call_ids, observation)?;
        reactor.clock = super::super::clock::ReactorClock::from_origin(origin);
        reactor
            .brokers
            .install_simulated_seed(config, &reactor.poller, Moment::ORIGIN)
            .map_err(io::Error::other)?;
        Ok((commands, shutdown, reactor))
    }

    pub(super) fn simulate_connect(&mut self) -> bool {
        let Some(token) = self
            .brokers
            .seed_mut()
            .and_then(super::super::broker::SingleBroker::simulate_connect)
        else {
            return false;
        };
        self.poll_events.push(PollEvent::Resource {
            token,
            readiness: Readiness::WRITABLE,
        });
        true
    }

    pub(super) fn simulate_receive(&mut self, bytes: Vec<u8>) -> bool {
        let Some(token) = self
            .brokers
            .seed_mut()
            .and_then(|seed| seed.simulate_receive(bytes))
        else {
            return false;
        };
        self.poll_events.push(PollEvent::Resource {
            token,
            readiness: Readiness::READABLE,
        });
        true
    }

    pub(super) fn take_simulated_frames(&mut self) -> Vec<Vec<u8>> {
        self.brokers.seed_mut().map_or_else(
            Vec::new,
            super::super::broker::SingleBroker::take_simulated_frames,
        )
    }
}
