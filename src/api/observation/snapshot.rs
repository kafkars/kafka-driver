//! One bounded point-in-time projection built by the reactor owner.

use kafka_driver_core::MetadataGeneration;

use super::{
    BootstrapSnapshot, BrokerLaneSnapshot, CallCounters, CallLatencySnapshot, FailureCounters,
    MailboxSnapshot, SeedSnapshot,
};

/// One point-in-time view built by the single reactor owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriverSnapshot {
    mailbox: MailboxSnapshot,
    metadata_generation: Option<MetadataGeneration>,
    bootstrap: BootstrapSnapshot,
    lanes: Vec<BrokerLaneSnapshot>,
    calls: CallCounters,
    failures: FailureCounters,
    latency: CallLatencySnapshot,
}

impl DriverSnapshot {
    pub(crate) const fn new(
        mailbox: MailboxSnapshot,
        metadata_generation: Option<MetadataGeneration>,
        bootstrap: BootstrapSnapshot,
        lanes: Vec<BrokerLaneSnapshot>,
        calls: CallCounters,
        failures: FailureCounters,
        latency: CallLatencySnapshot,
    ) -> Self {
        Self {
            mailbox,
            metadata_generation,
            bootstrap,
            lanes,
            calls,
            failures,
            latency,
        }
    }

    /// Borrows current bounded mailbox pressure and cumulative rejection counts.
    pub const fn mailbox(&self) -> &MailboxSnapshot {
        &self.mailbox
    }

    /// Returns the installed immutable metadata generation, if cluster mode is ready.
    pub const fn metadata_generation(&self) -> Option<MetadataGeneration> {
        self.metadata_generation
    }

    /// Borrows the bootstrap or direct seed connection state, when configured.
    pub const fn seed(&self) -> Option<&SeedSnapshot> {
        self.bootstrap.seed()
    }

    /// Borrows bootstrap DNS diagnostics and installed seed ownership.
    pub const fn bootstrap(&self) -> &BootstrapSnapshot {
        &self.bootstrap
    }

    /// Borrows one deterministic entry per live sparse discovered-broker lane.
    pub fn lanes(&self) -> &[BrokerLaneSnapshot] {
        &self.lanes
    }

    /// Returns cumulative public-call admission and terminal outcome counts.
    pub const fn calls(&self) -> CallCounters {
        self.calls
    }

    /// Returns cumulative classified public-call failures.
    pub const fn failures(&self) -> FailureCounters {
        self.failures
    }

    /// Returns cumulative public-call stage and end-to-end duration summaries.
    pub const fn latency(&self) -> CallLatencySnapshot {
        self.latency
    }
}
