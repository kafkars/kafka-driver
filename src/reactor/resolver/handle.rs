//! Reactor-side request admission and fairness-bounded outcome collection.

use std::{
    io,
    sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
    thread,
};

use kafka_driver_core::{DnsOutcome, DnsRequest, Moment, ResolutionLimits};

use crate::{
    ResolverLimits,
    reactor::{
        WakeHandle,
        worker_shutdown::{WorkerShutdown, WorkerShutdownPoll},
    },
};

use super::{ResolverSubmitError, ResolverWorkerError, worker};

/// Reactor-owned half of one bounded internal DNS worker.
pub(in crate::reactor) struct Resolver {
    requests: Option<SyncSender<DnsRequest>>,
    outcomes: Option<Receiver<DnsOutcome>>,
    outcome_budget: usize,
    worker: Option<thread::JoinHandle<()>>,
}

impl Resolver {
    #[cfg(test)]
    pub(in crate::reactor) fn isolated(
        limits: ResolverLimits,
    ) -> (Self, Receiver<DnsRequest>, SyncSender<DnsOutcome>) {
        let (requests, request_receiver) = sync_channel(limits.request_capacity().get());
        let (outcome_sender, outcomes) = sync_channel(limits.outcome_capacity().get());
        (
            Self {
                requests: Some(requests),
                outcomes: Some(outcomes),
                outcome_budget: limits.outcome_budget().get(),
                worker: None,
            },
            request_receiver,
            outcome_sender,
        )
    }

    pub(in crate::reactor) fn spawn(limits: ResolverLimits, wake: WakeHandle) -> io::Result<Self> {
        let (requests, request_receiver) = sync_channel(limits.request_capacity().get());
        let (outcome_sender, outcomes) = sync_channel(limits.outcome_capacity().get());
        let worker = worker::spawn(
            request_receiver,
            outcome_sender,
            ResolutionLimits::new(limits.max_addresses()),
            wake,
        )?;
        Ok(Self {
            requests: Some(requests),
            outcomes: Some(outcomes),
            outcome_budget: limits.outcome_budget().get(),
            worker: Some(worker),
        })
    }

    pub(in crate::reactor) fn submit(
        &self,
        request: DnsRequest,
    ) -> Result<(), ResolverSubmitError> {
        let Some(requests) = &self.requests else {
            return Err(ResolverSubmitError::Closed(request));
        };
        requests.try_send(request).map_err(|error| match error {
            TrySendError::Full(request) => ResolverSubmitError::Full(request),
            TrySendError::Disconnected(request) => ResolverSubmitError::Closed(request),
        })
    }

    pub(in crate::reactor) fn drain_into(
        &self,
        destination: &mut Vec<DnsOutcome>,
    ) -> Result<ResolverProgress, ResolverWorkerError> {
        let Some(outcomes) = &self.outcomes else {
            return Ok(ResolverProgress {
                outcomes: 0,
                more_work: false,
            });
        };
        for _ in 0..self.outcome_budget {
            match outcomes.try_recv() {
                Ok(outcome) => destination.push(outcome),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err(ResolverWorkerError::Lost),
            }
        }
        Ok(ResolverProgress {
            outcomes: destination.len(),
            more_work: destination.len() == self.outcome_budget,
        })
    }

    #[cfg(test)]
    pub(in crate::reactor) fn shutdown(mut self) -> io::Result<()> {
        self.close_channels();
        ResolverShutdown {
            worker: WorkerShutdown::new(self.worker.take(), Moment::ORIGIN, "DNS worker panicked"),
        }
        .join()
    }

    pub(in crate::reactor) fn begin_shutdown(mut self, now: Moment) -> ResolverShutdown {
        self.close_channels();
        ResolverShutdown {
            worker: WorkerShutdown::new(self.worker.take(), now, "DNS worker panicked"),
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn from_worker(worker: thread::JoinHandle<()>) -> Self {
        Self {
            requests: None,
            outcomes: None,
            outcome_budget: 1,
            worker: Some(worker),
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn install_worker_for_test(&mut self, worker: thread::JoinHandle<()>) {
        assert!(self.worker.replace(worker).is_none());
    }

    fn close_channels(&mut self) {
        self.requests = None;
        self.outcomes = None;
    }
}

impl Drop for Resolver {
    fn drop(&mut self) {
        self.close_channels();
    }
}

/// Graceful-shutdown ownership of a DNS worker pending nonblocking observation.
pub(in crate::reactor) struct ResolverShutdown {
    worker: WorkerShutdown,
}

impl ResolverShutdown {
    pub(in crate::reactor) fn poll_complete(
        &mut self,
        now: Moment,
    ) -> io::Result<WorkerShutdownPoll> {
        self.worker.poll(now)
    }

    pub(in crate::reactor) const fn deadline(&self) -> Moment {
        self.worker.deadline()
    }

    #[cfg(test)]
    fn join(self) -> io::Result<()> {
        self.worker.join()
    }

    #[cfg(test)]
    pub(in crate::reactor) fn from_worker(
        worker: thread::JoinHandle<()>,
        started_at: Moment,
    ) -> Self {
        Self {
            worker: WorkerShutdown::new(Some(worker), started_at, "DNS worker panicked"),
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
