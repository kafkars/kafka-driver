//! Fairness and ownership results from one bounded name-resolution phase.

use kafka_driver_core::DnsOutcome;

use crate::reactor::{
    BrokerLane, bootstrap::ResolvedSeed, direct_plaintext::endpoint_refresh::DirectRefreshOwner,
};

pub(super) struct BrokerDnsOutcome {
    pub(super) lane: BrokerLane,
    pub(super) outcome: DnsOutcome,
}

pub(super) struct DirectDnsOutcome {
    pub(super) owner: DirectRefreshOwner,
    pub(super) outcome: DnsOutcome,
}

pub(super) struct ResolutionProgress {
    pub(super) outcomes: usize,
    pub(super) submissions: usize,
    pub(super) more_work: bool,
    pub(super) broker: Option<ResolvedSeed>,
    pub(super) restarted: bool,
}

impl ResolutionProgress {
    pub(super) const fn made_progress(&self) -> bool {
        self.outcomes != 0 || self.submissions != 0 || self.broker.is_some() || self.restarted
    }
}

pub(super) struct ResolutionTurn {
    pub(super) made_progress: bool,
    pub(super) more_work: bool,
}

impl ResolutionTurn {
    pub(super) const fn idle() -> Self {
        Self {
            made_progress: false,
            more_work: false,
        }
    }

    pub(super) const fn made_progress(&self) -> bool {
        self.made_progress
    }

    pub(super) const fn more_work(&self) -> bool {
        self.more_work
    }
}
