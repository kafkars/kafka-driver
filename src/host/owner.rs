//! Join ownership for one thread repeatedly driving the embedded reactor.

use std::{fmt, io, thread, time::Duration};

use crate::{Reactor, ReactorError, TurnOutcome};

use super::DriverHostError;

const THREAD_NAME: &str = "kafka-driver-io";
const POLL_LIMIT: Duration = Duration::from_secs(60);

/// Join handle for one dedicated driver reactor thread.
///
/// Dropping this value detaches join observation; it does not request shutdown.
/// Request shutdown through [`crate::Driver::shutdown`] or drop every `Driver`
/// handle before joining.
pub struct DriverHost {
    owner: thread::JoinHandle<Result<(), ReactorError>>,
}

impl DriverHost {
    pub(crate) fn spawn(reactor: Reactor) -> io::Result<Self> {
        let owner = thread::Builder::new()
            .name(THREAD_NAME.into())
            .spawn(move || run(reactor))?;
        Ok(Self { owner })
    }

    /// Returns whether the dedicated thread has reached a terminal outcome.
    pub fn is_finished(&self) -> bool {
        self.owner.is_finished()
    }

    /// Waits for terminal reactor shutdown and reports thread or reactor failure.
    ///
    /// This blocks while any `Driver` still keeps admission open unless another
    /// handle has already requested shutdown.
    pub fn join(self) -> Result<(), DriverHostError> {
        match self.owner.join() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(DriverHostError::Reactor(error)),
            Err(_) => Err(DriverHostError::Panicked),
        }
    }
}

impl fmt::Debug for DriverHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriverHost")
            .field("is_finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

fn run(mut reactor: Reactor) -> Result<(), ReactorError> {
    loop {
        match reactor.turn(POLL_LIMIT)? {
            TurnOutcome::Shutdown { .. } => return Ok(()),
            TurnOutcome::Idle | TurnOutcome::Progress { .. } => {}
        }
    }
}
