//! Public bounds for retained broker membership and internal Metadata RPC waits.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{BrokerDirectoryLimits, MetadataQueryLimits, PartitionLeaderLimits};

use super::ControllerWaitingLimits;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_WAITING_CALLS: NonZeroUsize = nonzero(256);
const DEFAULT_WAITING_BYTES: NonZeroUsize = nonzero(8 * 1024 * 1024);
const DEFAULT_ADMISSION_BUDGET: NonZeroUsize = nonzero(64);
const DEFAULT_LANE_TURN_BUDGET: NonZeroUsize = nonzero(256);
const DEFAULT_PARTITION_WAITING_CALLS: NonZeroUsize = nonzero(256);
const DEFAULT_PARTITION_WAITING_BYTES: NonZeroUsize = nonzero(8 * 1024 * 1024);
const DEFAULT_PARTITION_ADMISSION_BUDGET: NonZeroUsize = nonzero(64);
const DEFAULT_INVALIDATION_WAITERS: NonZeroUsize = nonzero(256);
const DEFAULT_TOPIC_VIEW_WAITERS: NonZeroUsize = nonzero(256);
const DEFAULT_TOPIC_VIEW_BYTES: NonZeroUsize = nonzero(2 * 1024 * 1024);
const DEFAULT_TOPIC_VIEW_ADMISSION_BUDGET: NonZeroUsize = nonzero(64);

/// Resource and wait bounds applied to cluster metadata refreshes.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataLimits {
    broker_directory: BrokerDirectoryLimits,
    partition_leaders: PartitionLeaderLimits,
    queries: MetadataQueryLimits,
    request_timeout: Duration,
    waiting_calls: NonZeroUsize,
    waiting_bytes: NonZeroUsize,
    admission_budget: NonZeroUsize,
    lane_turn_budget: NonZeroUsize,
    partition_waiting_calls: NonZeroUsize,
    partition_waiting_bytes: NonZeroUsize,
    partition_admission_budget: NonZeroUsize,
    invalidation_waiters: NonZeroUsize,
    controller_waiting: ControllerWaitingLimits,
    topic_view_waiters: NonZeroUsize,
    topic_view_bytes: NonZeroUsize,
    topic_view_admission_budget: NonZeroUsize,
}

impl MetadataLimits {
    /// Creates explicit broker-retention and Metadata RPC bounds.
    pub const fn new(broker_directory: BrokerDirectoryLimits, request_timeout: Duration) -> Self {
        Self {
            broker_directory,
            partition_leaders: PartitionLeaderLimits::defaults(),
            queries: MetadataQueryLimits::defaults(),
            request_timeout,
            waiting_calls: DEFAULT_WAITING_CALLS,
            waiting_bytes: DEFAULT_WAITING_BYTES,
            admission_budget: DEFAULT_ADMISSION_BUDGET,
            lane_turn_budget: DEFAULT_LANE_TURN_BUDGET,
            partition_waiting_calls: DEFAULT_PARTITION_WAITING_CALLS,
            partition_waiting_bytes: DEFAULT_PARTITION_WAITING_BYTES,
            partition_admission_budget: DEFAULT_PARTITION_ADMISSION_BUDGET,
            invalidation_waiters: DEFAULT_INVALIDATION_WAITERS,
            controller_waiting: ControllerWaitingLimits::default_limits(),
            topic_view_waiters: DEFAULT_TOPIC_VIEW_WAITERS,
            topic_view_bytes: DEFAULT_TOPIC_VIEW_BYTES,
            topic_view_admission_budget: DEFAULT_TOPIC_VIEW_ADMISSION_BUDGET,
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

    /// Replaces the bound on distinct Metadata queries waiting behind one RPC.
    pub const fn with_query_limits(mut self, queries: MetadataQueryLimits) -> Self {
        self.queries = queries;
        self
    }

    /// Replaces per-lane waiting count, encoded bytes, and turn processing bounds.
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

    /// Replaces the global broker-lane work bound for one reactor phase.
    pub const fn with_lane_turn_budget(mut self, lane_turn_budget: NonZeroUsize) -> Self {
        self.lane_turn_budget = lane_turn_budget;
        self
    }

    /// Replaces topic-route waiting count, encoded bytes, and turn admission bounds.
    pub const fn with_partition_waiting_limits(
        mut self,
        waiting_calls: NonZeroUsize,
        waiting_bytes: NonZeroUsize,
        admission_budget: NonZeroUsize,
    ) -> Self {
        self.partition_waiting_calls = waiting_calls;
        self.partition_waiting_bytes = waiting_bytes;
        self.partition_admission_budget = admission_budget;
        self
    }

    /// Replaces the bound on public invalidations awaiting newer evidence.
    pub const fn with_invalidation_waiters(mut self, invalidation_waiters: NonZeroUsize) -> Self {
        self.invalidation_waiters = invalidation_waiters;
        self
    }

    /// Replaces controller-route waiting count, byte, and turn-processing bounds.
    pub const fn with_controller_waiting_limits(mut self, limits: ControllerWaitingLimits) -> Self {
        self.controller_waiting = limits;
        self
    }

    /// Replaces exact-topic view waiter count, byte, and turn-processing bounds.
    pub const fn with_topic_view_limits(
        mut self,
        waiters: NonZeroUsize,
        bytes: NonZeroUsize,
        admission_budget: NonZeroUsize,
    ) -> Self {
        self.topic_view_waiters = waiters;
        self.topic_view_bytes = bytes;
        self.topic_view_admission_budget = admission_budget;
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

    /// Returns the distinct Metadata follow-up query bound.
    pub const fn queries(self) -> MetadataQueryLimits {
        self.queries
    }

    /// Returns the maximum wait assigned to one generated Metadata RPC.
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Returns maximum calls waiting for one broker traffic lane.
    pub const fn waiting_calls(self) -> NonZeroUsize {
        self.waiting_calls
    }

    /// Returns maximum encoded request bytes waiting for one broker traffic lane.
    pub const fn waiting_bytes(self) -> NonZeroUsize {
        self.waiting_bytes
    }

    /// Returns maximum waiting calls admitted or expired for one lane in one turn.
    pub const fn admission_budget(self) -> NonZeroUsize {
        self.admission_budget
    }

    /// Returns maximum broker lanes progressed in one reactor phase.
    pub const fn lane_turn_budget(self) -> NonZeroUsize {
        self.lane_turn_budget
    }

    /// Returns maximum calls waiting for a topic-partition route.
    pub const fn partition_waiting_calls(self) -> NonZeroUsize {
        self.partition_waiting_calls
    }

    /// Returns maximum encoded request bytes waiting for topic-partition routes.
    pub const fn partition_waiting_bytes(self) -> NonZeroUsize {
        self.partition_waiting_bytes
    }

    /// Returns maximum topic-route waiters examined in one reactor turn.
    pub const fn partition_admission_budget(self) -> NonZeroUsize {
        self.partition_admission_budget
    }

    /// Returns maximum public invalidations awaiting newer metadata evidence.
    pub const fn invalidation_waiters(self) -> NonZeroUsize {
        self.invalidation_waiters
    }

    /// Returns bounds for controller-routed calls awaiting metadata.
    pub const fn controller_waiting(self) -> ControllerWaitingLimits {
        self.controller_waiting
    }

    /// Returns maximum public exact-topic views awaiting installed facts.
    pub const fn topic_view_waiters(self) -> NonZeroUsize {
        self.topic_view_waiters
    }

    /// Returns maximum bytes retained by public exact-topic view waiters.
    pub const fn topic_view_bytes(self) -> NonZeroUsize {
        self.topic_view_bytes
    }

    /// Returns maximum exact-topic view waiters examined in one reactor turn.
    pub const fn topic_view_admission_budget(self) -> NonZeroUsize {
        self.topic_view_admission_budget
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
