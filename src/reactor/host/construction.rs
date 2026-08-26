//! Exclusive Bornera backend construction before mailbox publication.

use std::sync::Arc;

use crate::{
    api::CallIds,
    completion::{ShutdownRequester, shutdown_barrier},
    config::{DirectBrokerConfig, DriverLimits, DriverTarget},
    observation::Observation,
};

use super::{CoordinatorOwner, HostState, MetadataOwner, NameResolution, Reactor};
use crate::reactor::{
    Command, MailboxSender, ReactorBackend, ReactorClock, WakeHandle,
    causality::CausalSequence,
    direct_plaintext::{ClusterBackend, DirectBackend},
    mailbox,
    scram_proof::ScramProofWorker,
};

#[cfg(test)]
use crate::reactor::{LegacyBackend, Poller, broker::BrokerLimits, broker_set::BrokerSet};

impl Reactor {
    pub(crate) fn new(
        limits: &DriverLimits,
        target: DriverTarget,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
    ) -> std::io::Result<(MailboxSender<Command>, ShutdownRequester, Self)> {
        let clock = ReactorClock::new();
        let now = clock.now().map_err(std::io::Error::other)?;
        let construction = match target {
            DriverTarget::Direct(config) => Construction::direct(limits, config, now)?,
            DriverTarget::Bootstrap(config) => Construction::cluster(limits, config)?,
        };
        Ok(Self::from_construction(
            limits,
            construction,
            call_ids,
            observation,
            clock,
        ))
    }

    fn from_construction(
        limits: &DriverLimits,
        construction: Construction,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
        clock: ReactorClock,
    ) -> (MailboxSender<Command>, ShutdownRequester, Self) {
        let (sender, commands) = mailbox(
            limits.mailbox_capacity(),
            limits.mailbox_byte_capacity(),
            Command::retained_bytes,
            construction.backend.wake_handle(),
        );
        let (shutdown_requester, shutdown) = shutdown_barrier(limits.mailbox_capacity());
        let reactor = Self {
            commands,
            limits: *limits,
            command_batch: Vec::with_capacity(limits.command_budget().get()),
            backend: construction.backend,
            resolution: construction.resolution,
            resolver_shutdown: None,
            broker_dns_outcomes: Vec::with_capacity(limits.resolver().outcome_budget().get()),
            direct_dns_outcomes: Vec::with_capacity(limits.resolver().outcome_budget().get()),
            scram_proof: construction.scram_proof,
            scram_proof_shutdown: None,
            scram_proof_outcomes: Vec::with_capacity(limits.scram_proof().outcome_budget().get()),
            metadata: construction.metadata,
            coordinator: construction.coordinator,
            call_ids,
            observation,
            causality: CausalSequence::new(),
            clock,
            state: HostState::Running,
            shutdown,
        };
        (sender, shutdown_requester, reactor)
    }

    #[cfg(test)]
    pub(crate) fn new_legacy_test(
        limits: &DriverLimits,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
    ) -> std::io::Result<(MailboxSender<Command>, ShutdownRequester, Self)> {
        let clock = ReactorClock::new();
        let construction = Construction::legacy_test(limits)?;
        Ok(Self::from_construction(
            limits,
            construction,
            call_ids,
            observation,
            clock,
        ))
    }
}

struct Construction {
    backend: ReactorBackend,
    resolution: Option<NameResolution>,
    scram_proof: Option<ScramProofWorker>,
    metadata: Option<MetadataOwner>,
    coordinator: Option<CoordinatorOwner>,
}

impl Construction {
    fn direct(
        limits: &DriverLimits,
        config: DirectBrokerConfig,
        now: kafka_driver_core::Moment,
    ) -> std::io::Result<Self> {
        let requires_proof_worker = config.requires_proof_worker();
        let mut backend = DirectBackend::new(limits, config, now)?;
        let scram_proof = requires_proof_worker
            .then(|| {
                ScramProofWorker::spawn(
                    limits.scram_proof(),
                    WakeHandle::bornera(backend.pulse_handle()),
                )
            })
            .transpose()?;
        if let Some(worker) = &scram_proof {
            backend.install_scram_proof_sender(worker.sender());
        }
        Ok(Self {
            backend: ReactorBackend::Direct(Box::new(backend)),
            resolution: None,
            scram_proof,
            metadata: None,
            coordinator: None,
        })
    }

    fn cluster(
        limits: &DriverLimits,
        config: crate::config::BootstrapConfig,
    ) -> std::io::Result<Self> {
        let requires_proof_worker = config.requires_proof_worker();
        let mut backend = ClusterBackend::new(limits, config.broker_template().clone())?;
        let wake = WakeHandle::bornera(backend.pulse_handle());
        let scram_proof = requires_proof_worker
            .then(|| ScramProofWorker::spawn(limits.scram_proof(), wake.clone()))
            .transpose()?;
        if let Some(worker) = &scram_proof {
            backend.install_scram_proof_sender(worker.sender());
        }
        let resolution = NameResolution::start(config, limits.resolver(), wake)?;
        Ok(Self {
            backend: ReactorBackend::Cluster(Box::new(backend)),
            resolution: Some(resolution),
            scram_proof,
            metadata: Some(MetadataOwner::new(limits.metadata())),
            coordinator: Some(CoordinatorOwner::new(limits.coordinator())),
        })
    }

    #[cfg(test)]
    fn legacy_test(limits: &DriverLimits) -> std::io::Result<Self> {
        let broker_limits = BrokerLimits::default();
        let capacity = BrokerSet::poll_registration_capacity(broker_limits, limits.metadata())
            .map_err(std::io::Error::other)?;
        let poller = Poller::with_registration_capacity(limits.poll_event_budget(), capacity)?;
        let brokers = BrokerSet::with_scram_proof(broker_limits, limits.metadata(), None, None)
            .map_err(std::io::Error::other)?;
        Ok(Self {
            backend: ReactorBackend::Legacy(Box::new(LegacyBackend::new(
                poller,
                Vec::with_capacity(limits.poll_event_budget().get()),
                brokers,
            ))),
            resolution: None,
            scram_proof: None,
            metadata: None,
            coordinator: None,
        })
    }
}

#[cfg(test)]
#[path = "construction_test.rs"]
mod test;
