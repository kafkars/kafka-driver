//! Bounded SCRAM proof admission and fairness-bounded outcome collection.

use std::{
    fmt, io,
    sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
};

use crate::{ScramProofLimits, reactor::WakeHandle};

use super::{ScramProofOutcome, ScramProofRequest, ScramProofSubmitError, worker};

pub(in crate::reactor) struct ScramProofWorker {
    sender: ScramProofSender,
    outcomes: Receiver<ScramProofOutcome>,
    outcome_budget: usize,
}

impl ScramProofWorker {
    #[cfg(test)]
    pub(super) fn isolated(
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
                sender: ScramProofSender { requests },
                outcomes,
                outcome_budget: limits.outcome_budget().get(),
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
        worker::spawn(request_receiver, outcome_sender, wake)?;
        Ok(Self {
            sender: ScramProofSender { requests },
            outcomes,
            outcome_budget: limits.outcome_budget().get(),
        })
    }

    pub(in crate::reactor) fn sender(&self) -> ScramProofSender {
        self.sender.clone()
    }

    pub(in crate::reactor) fn drain_into(
        &self,
        destination: &mut Vec<ScramProofOutcome>,
    ) -> ScramProofProgress {
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
        ScramProofProgress {
            outcomes: destination.len(),
            more_work: !disconnected && destination.len() == self.outcome_budget,
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
