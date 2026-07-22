//! One bounded point-in-time projection built by the reactor owner.

use kafka_driver_core::MetadataGeneration;

use super::{BrokerLaneSnapshot, MailboxSnapshot, SeedSnapshot};

/// One point-in-time view built by the single reactor owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriverSnapshot {
    mailbox: MailboxSnapshot,
    metadata_generation: Option<MetadataGeneration>,
    seed: Option<SeedSnapshot>,
    lanes: Vec<BrokerLaneSnapshot>,
}

impl DriverSnapshot {
    pub(crate) const fn new(
        mailbox: MailboxSnapshot,
        metadata_generation: Option<MetadataGeneration>,
        seed: Option<SeedSnapshot>,
        lanes: Vec<BrokerLaneSnapshot>,
    ) -> Self {
        Self {
            mailbox,
            metadata_generation,
            seed,
            lanes,
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
        self.seed.as_ref()
    }

    /// Borrows one deterministic entry per live sparse discovered-broker lane.
    pub fn lanes(&self) -> &[BrokerLaneSnapshot] {
        &self.lanes
    }
}
