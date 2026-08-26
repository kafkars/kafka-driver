//! Embedded reactor host for bounded administrative command progress.

mod address_refresh;
mod broker;
mod broker_route;
mod cluster_address_refresh;
mod commands;
mod construction;
mod coordinator;
mod coordinator_routing;
mod debug;
mod direct_address_refresh;
mod invalidation;
mod metadata;
mod observation;
mod outcome;
mod resolution;
mod resolution_error;
mod resolution_progress;
mod resolution_turn;
mod routing;
mod scram_proof;
#[cfg(test)]
mod simulation;
mod state;
mod submission;
mod topic_view;
mod turn;

#[cfg(test)]
mod cluster_seed_rotation_test;
#[cfg(test)]
mod direct_address_refresh_test;
#[cfg(test)]
mod resolution_test;
#[cfg(test)]
mod scram_proof_test;
#[cfg(test)]
mod shutdown_abandonment_test;
#[cfg(test)]
mod shutdown_test;
#[cfg(test)]
mod simulation_model_test;
#[cfg(test)]
mod simulation_protocol_test;
#[cfg(test)]
mod simulation_test;
#[cfg(test)]
mod submission_test;

use std::{sync::Arc, time::Duration};

use calandria::{Span, WaitOutcome};

use crate::{
    api::CallIds, completion::ShutdownCompleter, config::DriverLimits, observation::Observation,
};

use super::{
    Command, ReactorBackend, ReactorError, WakeHandle,
    causality::CausalSequence,
    clock::ReactorClock,
    coordinator::CoordinatorOwner,
    mailbox::MailboxReceiver,
    metadata::MetadataOwner,
    resolver::ResolverShutdown,
    scram_proof::{ScramProofOutcome, ScramProofWorker},
};

use resolution::NameResolution;
use resolution_progress::{BrokerDnsOutcome, DirectDnsOutcome};
use state::HostState;

pub use outcome::TurnOutcome;

/// Single-owner embedded host for driver state and external resources.
pub struct Reactor {
    commands: MailboxReceiver<Command>,
    limits: DriverLimits,
    command_batch: Vec<Command>,
    backend: ReactorBackend,
    resolution: Option<NameResolution>,
    resolver_shutdown: Option<ResolverShutdown>,
    broker_dns_outcomes: Vec<BrokerDnsOutcome>,
    direct_dns_outcomes: Vec<DirectDnsOutcome>,
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
        self.backend.wait(maximum).map_err(ReactorError::poll)
    }

    pub(crate) fn clock(&self) -> ReactorClock {
        self.clock.clone()
    }

    pub(crate) fn termination_wake(&self) -> calandria::WakeHandle {
        self.backend.wake_handle()
    }

    /// Returns a cloneable notification handle for embedded-host integration.
    pub fn wake_handle(&self) -> WakeHandle {
        self.backend.public_wake()
    }

    /// Returns whether shutdown has reached its terminal state.
    pub const fn is_shutdown(&self) -> bool {
        matches!(self.state, HostState::Shutdown)
    }
}
