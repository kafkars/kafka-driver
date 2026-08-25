//! Public join ownership over one Calandria-hosted driver duty.

use std::{fmt, io};

use calandria::{HostConfig, ReactorOutcome, Span};

use crate::{
    Driver, DriverBuildError, Reactor, ReactorError,
    reactor::{DriverWaiter, ReactorClock},
};

use super::{
    DriverHostError,
    local::{LocalOwner, LocalSpawnError},
};

const THREAD_NAME: &str = "kafka-driver-io";
const POLL_LIMIT: Span = Span::from_nanos(60_000_000_000);

type HostedReactor = calandria::Reactor<Reactor, ReactorClock, DriverWaiter>;

/// Join handle for one dedicated driver reactor thread.
///
/// Dropping this value detaches join observation; it does not request shutdown.
/// Request shutdown through [`crate::Driver::shutdown`] or drop every `Driver`
/// handle before joining.
pub struct DriverHost {
    owner: LocalOwner<HostTerminal>,
}

impl DriverHost {
    pub(crate) fn spawn<B>(build: B) -> Result<(Driver, Self), DriverBuildError>
    where
        B: FnOnce() -> Result<(Driver, Reactor), DriverBuildError> + Send + 'static,
    {
        let (driver, owner) =
            super::local::spawn(THREAD_NAME, build, run_reactor).map_err(map_spawn_error)?;
        Ok((driver, Self { owner }))
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
        let terminal = self
            .owner
            .join()
            .map_err(|()| DriverHostError::Panicked)?
            .ok_or_else(abandoned_host)?;
        match terminal {
            HostTerminal::Stopped => Ok(()),
            HostTerminal::Terminated => {
                Err(host_failure("the driver reactor was forcefully terminated"))
            }
            HostTerminal::Failed(failure) => Err(host_failure(failure)),
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

enum HostTerminal {
    Stopped,
    Terminated,
    Failed(String),
}

fn run_reactor(reactor: Reactor) -> HostTerminal {
    let clock = reactor.clock();
    let termination_wake = reactor.termination_wake();
    let hosted = HostedReactor::with_config(
        reactor,
        clock,
        DriverWaiter,
        termination_wake,
        HostConfig::new(POLL_LIMIT),
    );
    let exit = hosted.run();
    let terminal = match exit.outcome() {
        ReactorOutcome::Stopped => HostTerminal::Stopped,
        ReactorOutcome::Terminated => HostTerminal::Terminated,
        ReactorOutcome::Failed(failure) => HostTerminal::Failed(failure.to_string()),
    };
    drop(exit);
    terminal
}

fn map_spawn_error(error: LocalSpawnError<DriverBuildError>) -> DriverBuildError {
    match error {
        LocalSpawnError::Thread(source) => DriverBuildError::new(source),
        LocalSpawnError::Startup(source) => source,
        LocalSpawnError::Panicked => DriverBuildError::new(io::Error::other(
            "the dedicated driver host panicked during startup",
        )),
    }
}

fn abandoned_host() -> DriverHostError {
    host_failure("the dedicated driver host abandoned its startup handshake")
}

fn host_failure(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DriverHostError {
    DriverHostError::Reactor(ReactorError::host(io::Error::other(source)))
}
