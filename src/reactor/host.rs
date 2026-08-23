//! Embedded reactor host for bounded administrative command progress.

mod address_refresh;
mod broker;
mod broker_route;
mod commands;
mod coordinator;
mod debug;
mod invalidation;
mod metadata;
mod observation;
mod outcome;
mod resolution;
mod resolution_error;
mod resolution_progress;
mod routing;
mod scram_proof;
mod state;
mod submission;
mod topic_view;
mod turn;

#[cfg(test)]
mod resolution_test;
#[cfg(test)]
mod shutdown_test;
#[cfg(test)]
mod submission_test;

use std::{sync::Arc, time::Duration};

use calandria::{Span, WaitOutcome};

use crate::{
    api::CallIds,
    completion::{ShutdownCompleter, ShutdownRequester, shutdown_barrier},
    config::{DriverLimits, DriverTarget},
    observation::Observation,
};

use super::{
    Command, MailboxSender, PollEvent, Poller, ReactorError, WakeHandle,
    broker::BrokerLimits,
    broker_set::BrokerSet,
    causality::CausalSequence,
    clock::ReactorClock,
    coordinator::CoordinatorOwner,
    mailbox,
    mailbox::MailboxReceiver,
    metadata::MetadataOwner,
    resolver::ResolverShutdown,
    scram_proof::{ScramProofOutcome, ScramProofWorker},
};

use resolution::NameResolution;
use resolution_progress::BrokerDnsOutcome;
use state::HostState;

pub use outcome::TurnOutcome;

/// Single-owner embedded host for driver state and external resources.
pub struct Reactor {
    commands: MailboxReceiver<Command>,
    limits: DriverLimits,
    command_batch: Vec<Command>,
    poller: Poller,
    poll_events: Vec<PollEvent>,
    brokers: BrokerSet,
    resolution: Option<NameResolution>,
    resolver_shutdown: Option<ResolverShutdown>,
    broker_dns_outcomes: Vec<BrokerDnsOutcome>,
    scram_proof: Option<ScramProofWorker>,
    scram_proof_shutdown: Option<super::scram_proof::ScramProofShutdown>,
    scram_proof_outcomes: Vec<ScramProofOutcome>,
    metadata: Option<MetadataOwner>,
    coordinator: Option<CoordinatorOwner>,
    call_ids: Arc<CallIds>,
    observation: Arc<Observation>,
    causality: CausalSequence,
    clock: ReactorClock,
    state: HostState,
    shutdown: ShutdownCompleter,
}

impl Reactor {
    pub(crate) fn new(
        limits: &DriverLimits,
        target: Option<DriverTarget>,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
    ) -> std::io::Result<(MailboxSender<Command>, ShutdownRequester, Self)> {
        let poller = Poller::new(limits.poll_event_budget())?;
        let wake = WakeHandle::new(poller.wake_handle());
        let (sender, commands) = mailbox(
            limits.mailbox_capacity(),
            limits.mailbox_byte_capacity(),
            Command::retained_bytes,
            wake.clone(),
        );
        let clock = ReactorClock::new();
        let (shutdown_requester, shutdown) = shutdown_barrier(limits.mailbox_capacity());
        let now = clock.now().map_err(std::io::Error::other)?;
        let broker_template = match &target {
            Some(DriverTarget::Bootstrap(config)) => Some(config.broker_template().clone()),
            Some(DriverTarget::Direct(_)) | None => None,
        };
        let scram_proof = target
            .as_ref()
            .filter(|target| target.requires_proof_worker())
            .map(|_| ScramProofWorker::spawn(limits.scram_proof(), wake.clone()))
            .transpose()?;
        let proof_sender = scram_proof.as_ref().map(ScramProofWorker::sender);
        let mut brokers = BrokerSet::with_scram_proof(
            BrokerLimits::default(),
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
                Some(NameResolution::start(config, limits.resolver(), wake)?),
                Some(MetadataOwner::new(limits.metadata())),
                Some(CoordinatorOwner::new(limits.coordinator())),
            ),
            None => (None, None, None),
        };
        let reactor = Self {
            command_batch: Vec::with_capacity(limits.command_budget().get()),
            poll_events: Vec::with_capacity(limits.poll_event_budget().get()),
            commands,
            limits: *limits,
            poller,
            brokers,
            resolution,
            resolver_shutdown: None,
            broker_dns_outcomes: Vec::with_capacity(limits.resolver().outcome_budget().get()),
            scram_proof,
            scram_proof_shutdown: None,
            scram_proof_outcomes: Vec::with_capacity(limits.scram_proof().outcome_budget().get()),
            metadata,
            coordinator,
            call_ids,
            observation,
            causality: CausalSequence::new(),
            clock,
            state: HostState::Running,
            shutdown,
        };
        Ok((sender, shutdown_requester, reactor))
    }

    /// Drives at most one fairness-bounded turn, waiting up to `max_wait`.
    pub fn turn(&mut self, max_wait: Duration) -> Result<TurnOutcome, ReactorError> {
        if self.state == HostState::Shutdown {
            return Ok(TurnOutcome::Shutdown { commands: 0 });
        }
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let outcome = self.drive_at(now)?;
        if !matches!(outcome, TurnOutcome::Idle) {
            return Ok(outcome);
        }

        let maximum = Span::try_from(max_wait).unwrap_or(Span::from_nanos(u64::MAX));
        let wait = self
            .next(now)
            .bounded_wait(calandria::Moment::from_nanos(now.as_nanos()), maximum);
        let _ = self.wait_for_events(wait)?;
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.drive_at(now)
    }

    pub(in crate::reactor) fn wait_for_events(
        &mut self,
        maximum: Span,
    ) -> Result<WaitOutcome, ReactorError> {
        self.poll_events.clear();
        let observed = self
            .poller
            .poll_into(Some(maximum.as_duration()), &mut self.poll_events)
            .map_err(ReactorError::poll)?;
        Ok(if observed == 0 {
            WaitOutcome::Idle
        } else {
            WaitOutcome::Notified
        })
    }

    pub(crate) fn clock(&self) -> ReactorClock {
        self.clock.clone()
    }

    /// Returns a cloneable notification handle for embedded-host integration.
    pub fn wake_handle(&self) -> WakeHandle {
        self.commands.wake_handle()
    }

    /// Returns whether shutdown has reached its terminal state.
    pub const fn is_shutdown(&self) -> bool {
        matches!(self.state, HostState::Shutdown)
    }
}
