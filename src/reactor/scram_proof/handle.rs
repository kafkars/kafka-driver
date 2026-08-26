//! Bounded SCRAM proof admission and fairness-bounded outcome collection.

use std::{
    fmt, io,
    sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
    thread,
};

use kafka_driver_core::Moment;

use crate::{
    ScramProofLimits,
    reactor::{
        WakeHandle,
        worker_shutdown::{WorkerShutdown, WorkerShutdownPoll},
    },
};

use super::{
    ScramProofOutcome, ScramProofRequest, ScramProofSubmitError, ScramProofWorkerError, worker,
};

pub(in crate::reactor) struct ScramProofWorker {
    sender: Option<ScramProofSender>,
    outcomes: Option<Receiver<ScramProofOutcome>>,
    outcome_budget: usize,
    worker: Option<thread::JoinHandle<()>>,
}

impl ScramProofWorker {
    #[cfg(test)]
    pub(in crate::reactor) fn isolated(
        limits: ScramProofLimits,
    ) -> (
        Self,
        Receiver<ScramProofRequest>,
        SyncSender<ScramProofOutcome>,
    ) {
        let (requests, request_receiver) = sync_channel(limits.request_capacity().get());
        let (outcome_sender, outcomes) = sync_channel(limits.outcome_capacity().get());
        (
            Self {
                sender: Some(ScramProofSender { requests }),
                outcomes: Some(outcomes),
                outcome_budget: limits.outcome_budget().get(),
                worker: None,
            },
            request_receiver,
            outcome_sender,
        )
    }

    pub(in crate::reactor) fn spawn(
        limits: ScramProofLimits,
        wake: WakeHandle,
    ) -> io::Result<Self> {
        let (requests, request_receiver) = sync_channel(limits.request_capacity().get());
        let (outcome_sender, outcomes) = sync_channel(limits.outcome_capacity().get());
        let worker = worker::spawn(request_receiver, outcome_sender, wake)?;
        Ok(Self {
            sender: Some(ScramProofSender { requests }),
            outcomes: Some(outcomes),
            outcome_budget: limits.outcome_budget().get(),
            worker: Some(worker),
        })
    }

    pub(in crate::reactor) fn sender(&self) -> ScramProofSender {
        self.sender
            .as_ref()
            .unwrap_or_else(|| unreachable!("a live proof worker owns its sender"))
            .clone()
    }

    pub(in crate::reactor) fn drain_into(
        &self,
        destination: &mut Vec<ScramProofOutcome>,
    ) -> Result<ScramProofProgress, ScramProofWorkerError> {
        let Some(outcomes) = &self.outcomes else {
            return Ok(ScramProofProgress {
                outcomes: 0,
                more_work: false,
            });
        };
        for _ in 0..self.outcome_budget {
            match outcomes.try_recv() {
                Ok(outcome) => destination.push(outcome),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err(ScramProofWorkerError::Lost),
            }
        }
        Ok(ScramProofProgress {
            outcomes: destination.len(),
            more_work: destination.len() == self.outcome_budget,
        })
    }

    #[cfg(test)]
    pub(in crate::reactor) fn shutdown(mut self) -> io::Result<()> {
        self.close_channels();
        ScramProofShutdown {
            worker: WorkerShutdown::new(
                self.worker.take(),
                Moment::ORIGIN,
                "SCRAM proof worker panicked",
            ),
        }
        .join()
    }

    pub(in crate::reactor) fn begin_shutdown(mut self, now: Moment) -> ScramProofShutdown {
        self.close_channels();
        ScramProofShutdown {
            worker: WorkerShutdown::new(self.worker.take(), now, "SCRAM proof worker panicked"),
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn from_worker(worker: thread::JoinHandle<()>) -> Self {
        Self {
            sender: None,
            outcomes: None,
            outcome_budget: 1,
            worker: Some(worker),
        }
    }

    fn close_channels(&mut self) {
        self.sender = None;
        self.outcomes = None;
    }
}

impl Drop for ScramProofWorker {
    fn drop(&mut self) {
        self.close_channels();
    }
}

/// Graceful-shutdown ownership of a proof worker pending nonblocking observation.
pub(in crate::reactor) struct ScramProofShutdown {
    worker: WorkerShutdown,
}

impl ScramProofShutdown {
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
            worker: WorkerShutdown::new(Some(worker), started_at, "SCRAM proof worker panicked"),
        }
    }
}

#[derive(Clone)]
pub(in crate::reactor) struct ScramProofSender {
    requests: SyncSender<ScramProofRequest>,
}

impl ScramProofSender {
    pub(in crate::reactor) fn submit(
        &self,
        request: ScramProofRequest,
    ) -> Result<(), ScramProofSubmitError> {
        self.requests
            .try_send(request)
            .map_err(|error| match error {
                TrySendError::Full(request) => ScramProofSubmitError::Full(Box::new(request)),
                TrySendError::Disconnected(request) => {
                    ScramProofSubmitError::Closed(Box::new(request))
                }
            })
    }
}

impl fmt::Debug for ScramProofSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScramProofSender(..)")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ScramProofProgress {
    outcomes: usize,
    more_work: bool,
}

impl ScramProofProgress {
    pub(in crate::reactor) const fn outcomes(self) -> usize {
        self.outcomes
    }

    pub(in crate::reactor) const fn more_work(self) -> bool {
        self.more_work
    }
}
