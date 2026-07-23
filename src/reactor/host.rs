//! Embedded reactor host for bounded administrative command progress.

mod address_refresh;
mod broker;
mod commands;
mod coordinator;
mod debug;
mod invalidation;
mod metadata;
mod observation;
mod resolution;
mod resolution_error;
mod resolution_progress;
mod routing;
mod scram_proof;
mod state;
mod submission;

#[cfg(test)]
mod submission_test;

use std::{sync::Arc, time::Duration};

use crate::{
    api::CallIds,
    config::{DriverLimits, DriverTarget},
    observation::Observation,
};

use super::{
    Command, MailboxSender, PollEvent, Poller, ReactorError, WakeHandle,
    broker::BrokerLimits,
    broker_set::BrokerSet,
    clock::ReactorClock,
    coordinator::CoordinatorOwner,
    mailbox,
    mailbox::{DrainStatus, MailboxReceiver},
    metadata::MetadataOwner,
    scram_proof::{ScramProofOutcome, ScramProofWorker},
};

use resolution::NameResolution;
use resolution_progress::BrokerDnsOutcome;
use state::{HostState, ShutdownWaiters};

/// Result of one bounded reactor turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnOutcome {
    /// No command was available before the host's wait limit.
    Idle,
    /// Bounded command, timer, or I/O work made progress.
    Progress {
        /// Number of commands processed during this turn.
        commands: usize,
        /// Whether bounded command, timer, or retained I/O work remains.
        more_work: bool,
    },
    /// Shutdown reached its terminal state.
    Shutdown {
        /// Number of shutdown commands completed during this turn.
        commands: usize,
    },
}

/// Single-owner embedded host for driver state and external resources.
pub struct Reactor {
    commands: MailboxReceiver<Command>,
    limits: DriverLimits,
    command_batch: Vec<Command>,
    poller: Poller,
    poll_events: Vec<PollEvent>,
    brokers: BrokerSet,
    resolution: Option<NameResolution>,
    broker_dns_outcomes: Vec<BrokerDnsOutcome>,
    scram_proof: Option<ScramProofWorker>,
    scram_proof_outcomes: Vec<ScramProofOutcome>,
    metadata: Option<MetadataOwner>,
    coordinator: Option<CoordinatorOwner>,
    call_ids: Arc<CallIds>,
    observation: Arc<Observation>,
    clock: ReactorClock,
    state: HostState,
    shutdown_waiters: ShutdownWaiters,
}

impl Reactor {
    pub(crate) fn new(
        limits: DriverLimits,
        target: Option<DriverTarget>,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
    ) -> std::io::Result<(MailboxSender<Command>, Self)> {
        let poller = Poller::new(limits.poll_event_budget())?;
        let wake = WakeHandle::new(poller.wake_handle());
        let (sender, commands) = mailbox(
            limits.mailbox_capacity(),
            limits.mailbox_byte_capacity(),
            Command::retained_bytes,
            wake.clone(),
        );
        let clock = ReactorClock::new();
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
            limits,
            poller,
            brokers,
            resolution,
            broker_dns_outcomes: Vec::with_capacity(limits.resolver().outcome_budget().get()),
            scram_proof,
            scram_proof_outcomes: Vec::with_capacity(limits.scram_proof().outcome_budget().get()),
            metadata,
            coordinator,
            call_ids,
            observation,
            clock,
            state: HostState::Running,
            shutdown_waiters: ShutdownWaiters::new(limits.mailbox_capacity()),
        };
        Ok((sender, reactor))
    }

    /// Drives at most one fairness-bounded turn, waiting up to `max_wait`.
    pub fn turn(&mut self, max_wait: Duration) -> Result<TurnOutcome, ReactorError> {
        if self.state == HostState::Shutdown {
            return Ok(TurnOutcome::Shutdown { commands: 0 });
        }
        let mut status = self
            .commands
            .drain_into(&mut self.command_batch, self.limits.command_budget());
        let mut processed = self.process_commands()?;
        if status == DrainStatus::Closed && self.state == HostState::Running {
            self.begin_implicit_shutdown()?;
        }
        if let Some(outcome) = self.finish_shutdown_if_terminal(processed)? {
            return Ok(outcome);
        }
        let deadlines = self.fire_due_deadlines()?;
        let mut progress = deadlines.made_progress();
        let mut more_due = deadlines.more_due();
        let resolution = self.continue_resolution()?;
        let mut more_resolution = resolution.more_work();
        progress |= resolution.made_progress();
        let proofs = self.continue_scram_proofs()?;
        let mut more_proofs = proofs.more_work();
        progress |= proofs.made_progress();
        progress |= processed != 0;
        progress |= self.continue_broker_io()?;
        progress |= self.continue_metadata()?;
        progress |= self.continue_coordinator()?;

        if !progress && status == DrainStatus::Idle {
            self.poll_events.clear();
            let wait = self.poll_wait(max_wait)?;
            self.poller
                .poll_into(Some(wait), &mut self.poll_events)
                .map_err(ReactorError::poll)?;
            status = self
                .commands
                .drain_into(&mut self.command_batch, self.limits.command_budget());
            processed += self.process_commands()?;
            if status == DrainStatus::Closed && self.state == HostState::Running {
                self.begin_implicit_shutdown()?;
            }
            let deadlines = self.fire_due_deadlines()?;
            progress |= processed != 0 || deadlines.made_progress();
            more_due |= deadlines.more_due();
            let resolution = self.continue_resolution()?;
            progress |= resolution.made_progress();
            more_resolution |= resolution.more_work();
            progress |= self.observe_poll_events()?;
            let proofs = self.continue_scram_proofs()?;
            progress |= proofs.made_progress();
            more_proofs |= proofs.more_work();
            progress |= self.continue_metadata()?;
            progress |= self.continue_coordinator()?;
        }
        if let Some(outcome) = self.finish_shutdown_if_terminal(processed)? {
            return Ok(outcome);
        }
        if progress {
            return Ok(TurnOutcome::Progress {
                commands: processed,
                more_work: status == DrainStatus::MorePending
                    || more_due
                    || more_resolution
                    || more_proofs
                    || self.broker_has_local_io()
                    || self.metadata_has_local_work()
                    || self.coordinator_has_local_work(),
            });
        }
        Ok(TurnOutcome::Idle)
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
