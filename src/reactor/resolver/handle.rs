//! Reactor-side request admission and fairness-bounded outcome collection.

use std::{
    io,
    sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
};

use kafka_driver_core::{DnsOutcome, DnsRequest, ResolutionLimits};

use crate::{ResolverLimits, reactor::WakeHandle};

use super::{ResolverSubmitError, worker};

/// Reactor-owned half of one bounded internal DNS worker.
pub(in crate::reactor) struct Resolver {
    requests: SyncSender<DnsRequest>,
    outcomes: Receiver<DnsOutcome>,
    outcome_budget: usize,
}

impl Resolver {
    #[cfg(test)]
    pub(super) fn isolated(
        limits: ResolverLimits,
    ) -> (Self, Receiver<DnsRequest>, SyncSender<DnsOutcome>) {
        let (requests, request_receiver) = sync_channel(limits.request_capacity().get());
        let (outcome_sender, outcomes) = sync_channel(limits.outcome_capacity().get());
        (
            Self {
                requests,
                outcomes,
                outcome_budget: limits.outcome_budget().get(),
            },
            request_receiver,
            outcome_sender,
        )
    }

    pub(in crate::reactor) fn spawn(limits: ResolverLimits, wake: WakeHandle) -> io::Result<Self> {
        let (requests, request_receiver) = sync_channel(limits.request_capacity().get());
        let (outcome_sender, outcomes) = sync_channel(limits.outcome_capacity().get());
        worker::spawn(
            request_receiver,
            outcome_sender,
            ResolutionLimits::new(limits.max_addresses()),
            wake,
        )?;
        Ok(Self {
            requests,
            outcomes,
            outcome_budget: limits.outcome_budget().get(),
        })
    }

    pub(in crate::reactor) fn submit(
        &self,
        request: DnsRequest,
    ) -> Result<(), ResolverSubmitError> {
        self.requests
            .try_send(request)
            .map_err(|error| match error {
                TrySendError::Full(request) => ResolverSubmitError::Full(request),
                TrySendError::Disconnected(request) => ResolverSubmitError::Closed(request),
            })
    }

    pub(in crate::reactor) fn drain_into(
        &self,
        destination: &mut Vec<DnsOutcome>,
    ) -> ResolverProgress {
        let mut disconnected = false;
        for _ in 0..self.outcome_budget {
            match self.outcomes.try_recv() {
                Ok(outcome) => destination.push(outcome),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        ResolverProgress {
            outcomes: destination.len(),
            more_work: !disconnected && destination.len() == self.outcome_budget,
        }
    }
}

/// Fairness result after collecting one bounded reactor batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ResolverProgress {
    outcomes: usize,
    more_work: bool,
}

impl ResolverProgress {
    pub(in crate::reactor) const fn outcomes(self) -> usize {
        self.outcomes
    }

    pub(in crate::reactor) const fn more_work(self) -> bool {
        self.more_work
    }
}
