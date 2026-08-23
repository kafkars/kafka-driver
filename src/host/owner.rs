//! Public join ownership over one Calandria-hosted driver duty.

use std::{fmt, io};

use calandria::{HostConfig, ReactorOutcome, Span};

use crate::{
    Reactor, ReactorError,
    reactor::{DriverWaiter, ReactorClock},
};

use super::DriverHostError;

const THREAD_NAME: &str = "kafka-driver-io";
const POLL_LIMIT: Span = Span::from_nanos(60_000_000_000);

type HostedReactor = calandria::Reactor<Reactor, ReactorClock, DriverWaiter>;
type HostedHandle = calandria::ReactorHandle<Reactor, ReactorClock, DriverWaiter>;

/// Join handle for one dedicated driver reactor thread.
///
/// Dropping this value detaches join observation; it does not request shutdown.
/// Request shutdown through [`crate::Driver::shutdown`] or drop every `Driver`
/// handle before joining.
pub struct DriverHost {
    owner: HostedHandle,
}

impl DriverHost {
    pub(crate) fn spawn(reactor: Reactor) -> io::Result<Self> {
        let clock = reactor.clock();
        let wake = reactor.wake_handle();
        let termination_wake = calandria::WakeHandle::new(move || wake.wake());
        let hosted = HostedReactor::with_config(
            reactor,
            clock,
            DriverWaiter,
            termination_wake,
            HostConfig::new(POLL_LIMIT),
        );
        let owner = hosted
            .spawn(THREAD_NAME)
            .map_err(|error| error.into_parts().0)?;
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
        let exit = self.owner.join().map_err(|_| DriverHostError::Panicked)?;
        let (_, _, outcome, _) = exit.into_parts();
        match outcome {
            ReactorOutcome::Stopped => Ok(()),
            ReactorOutcome::Terminated => Err(DriverHostError::Reactor(ReactorError::host(
                io::Error::other("the driver reactor was forcefully terminated"),
            ))),
            ReactorOutcome::Failed(failure) => Err(DriverHostError::Reactor(ReactorError::host(
                io::Error::other(failure),
            ))),
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
