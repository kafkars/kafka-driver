//! Exclusive legacy-or-Bornera backend construction before mailbox publication.

use std::sync::Arc;

use crate::{
    api::CallIds,
    completion::{ShutdownRequester, shutdown_barrier},
    config::{DirectBrokerConfig, DirectTargetSelection, DriverLimits, DriverTarget},
    observation::Observation,
};

use super::{CoordinatorOwner, HostState, MetadataOwner, NameResolution, Reactor};
use crate::reactor::{
    Command, LegacyBackend, MailboxSender, Poller, ReactorBackend, ReactorClock, WakeHandle,
    broker::BrokerLimits, broker_set::BrokerSet, causality::CausalSequence,
    direct_plaintext::DirectBackend, mailbox, scram_proof::ScramProofWorker,
};

impl Reactor {
    pub(crate) fn new(
        limits: &DriverLimits,
        target: Option<DriverTarget>,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
    ) -> std::io::Result<(MailboxSender<Command>, ShutdownRequester, Self)> {
        let clock = ReactorClock::new();
        let now = clock.now().map_err(std::io::Error::other)?;
        let construction = match target {
            Some(target) => match target.select_direct() {
                DirectTargetSelection::Direct(config) => Construction::direct(limits, config, now)?,
                DirectTargetSelection::Legacy(target) => {
                    Construction::legacy(limits, Some(target), now)?
                }
            },
            None => Construction::legacy(limits, None, now)?,
        };
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
        Ok((sender, shutdown_requester, reactor))
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

    fn legacy(
        limits: &DriverLimits,
        target: Option<DriverTarget>,
        now: kafka_driver_core::Moment,
    ) -> std::io::Result<Self> {
        let broker_limits = BrokerLimits::default();
        let capacity = BrokerSet::poll_registration_capacity(broker_limits, limits.metadata())
            .map_err(std::io::Error::other)?;
        let poller = Poller::with_registration_capacity(limits.poll_event_budget(), capacity)?;
        let broker_template = match &target {
            Some(DriverTarget::Bootstrap(config)) => Some(config.broker_template().clone()),
            Some(DriverTarget::Direct(_)) | None => None,
        };
        let scram_proof = target
            .as_ref()
            .filter(|target| target.requires_proof_worker())
            .map(|_| {
                ScramProofWorker::spawn(
                    limits.scram_proof(),
                    WakeHandle::new(poller.pulse_handle()),
                )
            })
            .transpose()?;
        let proof_sender = scram_proof.as_ref().map(ScramProofWorker::sender);
        let mut brokers = BrokerSet::with_scram_proof(
            broker_limits,
            limits.metadata(),
            broker_template,
            proof_sender,
        )
        .map_err(std::io::Error::other)?;
        let (resolution, metadata, coordinator) = match target {
            Some(DriverTarget::Direct(config)) => {
                brokers
                    .install_seed(config, &poller, now)
                    .map_err(std::io::Error::other)?;
                (None, None, None)
            }
            Some(DriverTarget::Bootstrap(config)) => (
                Some(NameResolution::start(
                    config,
                    limits.resolver(),
                    WakeHandle::new(poller.pulse_handle()),
                )?),
                Some(MetadataOwner::new(limits.metadata())),
                Some(CoordinatorOwner::new(limits.coordinator())),
            ),
            None => (None, None, None),
        };
        Ok(Self {
            backend: ReactorBackend::Legacy(Box::new(LegacyBackend::new(
                poller,
                Vec::with_capacity(limits.poll_event_budget().get()),
                brokers,
            ))),
            resolution,
            scram_proof,
            metadata,
            coordinator,
        })
    }
}
