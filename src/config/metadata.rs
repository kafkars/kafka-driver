//! Public bounds for retained broker membership and internal Metadata RPC waits.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{BrokerDirectoryLimits, PartitionLeaderLimits};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_WAITING_CALLS: NonZeroUsize = nonzero(256);
const DEFAULT_WAITING_BYTES: NonZeroUsize = nonzero(8 * 1024 * 1024);
const DEFAULT_ADMISSION_BUDGET: NonZeroUsize = nonzero(64);

/// Resource and wait bounds applied to cluster metadata refreshes.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataLimits {
    broker_directory: BrokerDirectoryLimits,
    partition_leaders: PartitionLeaderLimits,
    request_timeout: Duration,
    waiting_calls: NonZeroUsize,
    waiting_bytes: NonZeroUsize,
    admission_budget: NonZeroUsize,
}

impl MetadataLimits {
    /// Creates explicit broker-retention and Metadata RPC bounds.
    pub const fn new(broker_directory: BrokerDirectoryLimits, request_timeout: Duration) -> Self {
        Self {
            broker_directory,
            partition_leaders: PartitionLeaderLimits::defaults(),
            request_timeout,
            waiting_calls: DEFAULT_WAITING_CALLS,
            waiting_bytes: DEFAULT_WAITING_BYTES,
            admission_budget: DEFAULT_ADMISSION_BUDGET,
        }
    }

    /// Replaces retained topic and partition-leader bounds.
    pub const fn with_partition_leader_limits(
        mut self,
        partition_leaders: PartitionLeaderLimits,
    ) -> Self {
        self.partition_leaders = partition_leaders;
        self
    }

    /// Replaces per-broker waiting count, encoded bytes, and turn admission bounds.
    pub const fn with_waiting_limits(
        mut self,
        waiting_calls: NonZeroUsize,
        waiting_bytes: NonZeroUsize,
        admission_budget: NonZeroUsize,
    ) -> Self {
        self.waiting_calls = waiting_calls;
        self.waiting_bytes = waiting_bytes;
        self.admission_budget = admission_budget;
        self
    }

    /// Returns maximum broker membership retained in one generation.
    pub const fn broker_directory(self) -> BrokerDirectoryLimits {
        self.broker_directory
    }

    /// Returns maximum topic and known partition-leader retention per generation.
    pub const fn partition_leaders(self) -> PartitionLeaderLimits {
        self.partition_leaders
    }

    /// Returns the maximum wait assigned to one generated Metadata RPC.
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Returns maximum calls waiting for one broker connection.
    pub const fn waiting_calls(self) -> NonZeroUsize {
        self.waiting_calls
    }

    /// Returns maximum encoded request bytes waiting for one broker connection.
    pub const fn waiting_bytes(self) -> NonZeroUsize {
        self.waiting_bytes
    }

    /// Returns maximum waiting calls admitted to a ready broker in one turn.
    pub const fn admission_budget(self) -> NonZeroUsize {
        self.admission_budget
    }

    pub(super) const fn default_limits() -> Self {
        Self::new(BrokerDirectoryLimits::defaults(), DEFAULT_REQUEST_TIMEOUT)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("metadata defaults must be nonzero");
    };
    value
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self::default_limits()
    }
}
