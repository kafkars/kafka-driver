//! Shared bounded grace policy for private blocking-worker teardown.

use std::{io, thread, time::Duration};

use kafka_driver_core::Moment;

/// Maximum time graceful shutdown retains an unfinished worker handle.
pub(super) const WORKER_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkerShutdownPoll {
    Pending,
    Complete,
    Abandoned,
}

pub(super) struct WorkerShutdown {
    worker: Option<thread::JoinHandle<()>>,
    deadline: Moment,
    panic_message: &'static str,
}

impl WorkerShutdown {
    pub(super) fn new(
        worker: Option<thread::JoinHandle<()>>,
        started_at: Moment,
        panic_message: &'static str,
    ) -> Self {
        let deadline = started_at
            .checked_add(WORKER_SHUTDOWN_GRACE)
            .unwrap_or_else(|| Moment::from_nanos(u64::MAX));
        Self {
            worker,
            deadline,
            panic_message,
        }
    }

    pub(super) fn poll(&mut self, now: Moment) -> io::Result<WorkerShutdownPoll> {
        let Some(worker) = &self.worker else {
            return Ok(WorkerShutdownPoll::Complete);
        };
        if worker.is_finished() {
            self.join_worker()?;
            return Ok(WorkerShutdownPoll::Complete);
        }
        if now >= self.deadline {
            drop(self.worker.take());
            return Ok(WorkerShutdownPoll::Abandoned);
        }
        Ok(WorkerShutdownPoll::Pending)
    }

    pub(super) const fn deadline(&self) -> Moment {
        self.deadline
    }

    #[cfg(test)]
    pub(super) fn join(mut self) -> io::Result<()> {
        self.join_worker()
    }

    fn join_worker(&mut self) -> io::Result<()> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .join()
            .map_err(|_| io::Error::other(self.panic_message))
    }
}
