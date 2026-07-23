//! Public host-admission and selector-fairness limits.

use std::num::NonZeroUsize;

use super::{CoordinatorLimits, MetadataLimits, ResolverLimits, ScramProofLimits};

const DEFAULT_MAILBOX_CAPACITY: NonZeroUsize = nonzero(1_024);
const DEFAULT_MAILBOX_BYTE_CAPACITY: NonZeroUsize = nonzero(256 * 1_024 * 1_024);
const DEFAULT_COMMAND_BUDGET: NonZeroUsize = nonzero(256);
const DEFAULT_POLL_EVENT_BUDGET: NonZeroUsize = nonzero(256);

/// Resource bounds applied to one driver reactor.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverLimits {
    mailbox_capacity: NonZeroUsize,
    mailbox_byte_capacity: NonZeroUsize,
    command_budget: NonZeroUsize,
    poll_event_budget: NonZeroUsize,
    resolver: ResolverLimits,
    metadata: MetadataLimits,
    coordinator: CoordinatorLimits,
    scram_proof: ScramProofLimits,
}

impl DriverLimits {
    /// Creates limits with explicit mailbox and per-turn command bounds.
    pub const fn new(mailbox_capacity: NonZeroUsize, command_budget: NonZeroUsize) -> Self {
        Self {
            mailbox_capacity,
            mailbox_byte_capacity: DEFAULT_MAILBOX_BYTE_CAPACITY,
            command_budget,
            poll_event_budget: DEFAULT_POLL_EVENT_BUDGET,
            resolver: ResolverLimits::defaults(),
            metadata: MetadataLimits::default_limits(),
            coordinator: CoordinatorLimits::defaults(),
            scram_proof: ScramProofLimits::defaults(),
        }
    }

    /// Replaces the maximum readiness events returned by one OS poll.
    pub const fn with_poll_event_budget(mut self, poll_event_budget: NonZeroUsize) -> Self {
        self.poll_event_budget = poll_event_budget;
        self
    }

    /// Replaces each mailbox lane's retained-command byte bound.
    pub const fn with_mailbox_byte_capacity(mut self, mailbox_byte_capacity: NonZeroUsize) -> Self {
        self.mailbox_byte_capacity = mailbox_byte_capacity;
        self
    }

    /// Replaces bounded resolver admission, retention, and fairness policy.
    pub const fn with_resolver_limits(mut self, resolver: ResolverLimits) -> Self {
        self.resolver = resolver;
        self
    }

    /// Replaces broker membership and Metadata RPC bounds.
    pub const fn with_metadata_limits(mut self, metadata: MetadataLimits) -> Self {
        self.metadata = metadata;
        self
    }

    /// Replaces coordinator discovery, retention, and fairness policy.
    pub const fn with_coordinator_limits(mut self, coordinator: CoordinatorLimits) -> Self {
        self.coordinator = coordinator;
        self
    }

    /// Replaces bounded SCRAM proof queue and outcome fairness policy.
    pub const fn with_scram_proof_limits(mut self, scram_proof: ScramProofLimits) -> Self {
        self.scram_proof = scram_proof;
        self
    }

    /// Returns each independent request and shutdown-control command bound.
    pub const fn mailbox_capacity(self) -> NonZeroUsize {
        self.mailbox_capacity
    }

    /// Returns each independent request and control lane's retained-byte bound.
    pub const fn mailbox_byte_capacity(self) -> NonZeroUsize {
        self.mailbox_byte_capacity
    }

    /// Returns the maximum commands processed by one reactor turn.
    pub const fn command_budget(self) -> NonZeroUsize {
        self.command_budget
    }

    /// Returns the maximum readiness events returned by one OS poll.
    pub const fn poll_event_budget(self) -> NonZeroUsize {
        self.poll_event_budget
    }

    /// Returns bounded resolver admission, retention, and fairness policy.
    pub const fn resolver(self) -> ResolverLimits {
        self.resolver
    }

    /// Returns bounded cluster metadata retention and request policy.
    pub const fn metadata(self) -> MetadataLimits {
        self.metadata
    }

    /// Returns bounded coordinator discovery, retention, and fairness policy.
    pub const fn coordinator(self) -> CoordinatorLimits {
        self.coordinator
    }

    /// Returns bounded SCRAM proof queue and outcome fairness policy.
    pub const fn scram_proof(self) -> ScramProofLimits {
        self.scram_proof
    }
}

impl Default for DriverLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAILBOX_CAPACITY, DEFAULT_COMMAND_BUDGET)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("driver defaults must be nonzero");
    };
    value
}
