//! Bounded SCRAM proof admission and fairness-bounded outcome collection.

use std::{
    fmt, io,
    sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
    thread,
};

use crate::{ScramProofLimits, reactor::WakeHandle};

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

    pub(in crate::reactor) fn shutdown(mut self) -> io::Result<()> {
        self.close_channels();
        self.join_worker()
    }

    fn close_channels(&mut self) {
        self.sender = None;
        self.outcomes = None;
    }

    fn join_worker(&mut self) -> io::Result<()> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .join()
            .map_err(|_| io::Error::other("SCRAM proof worker panicked"))
    }
}

impl Drop for ScramProofWorker {
    fn drop(&mut self) {
        self.close_channels();
        drop(self.join_worker());
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
