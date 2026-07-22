//! Public construction, admission, and shutdown handle for one driver reactor.

use std::{
    fmt, io,
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_wire::RequestResponsePair;

use crate::{
    completion::completion_pair,
    reactor::{Command, MailboxSender, TrySendError},
    request::{erased_request, erased_request_in, routed_request_in},
};

use super::{
    Call, DriverBuilder, RequestError, Route, RoutedCall, TrafficClass, identity::CallIds,
};

/// Cloneable command-admission handle for one driver reactor.
#[derive(Clone, Debug)]
pub struct Driver {
    commands: MailboxSender<Command>,
    call_ids: Arc<CallIds>,
}

impl Driver {
    pub(super) const fn new(commands: MailboxSender<Command>, call_ids: Arc<CallIds>) -> Self {
        Self { commands, call_ids }
    }

    /// Starts configuration of a driver and its reactor host.
    pub fn builder() -> DriverBuilder {
        DriverBuilder::default()
    }

    /// Requests terminal shutdown through the separately bounded control lane.
    ///
    /// A full request lane cannot reject or delay this command. Admission can
    /// still fail if the shutdown-control bound itself is exhausted, command
    /// admission is closed, or the reactor cannot be woken.
    pub fn shutdown(&self) -> Result<Call<()>, SubmitError> {
        let (completion, sender) = completion_pair();
        let call = Call::new(completion);
        let command = Command::Shutdown { completion: sender };
        self.commands
            .try_send_control(command)
            .map_err(SubmitError::from)?;
        Ok(call)
    }

    /// Submits one generated request using that connection's negotiated version.
    ///
    /// For an accepted command, `timeout` starts immediately before bounded
    /// mailbox admission and includes every later routing and connection wait.
    pub fn call<R>(
        &self,
        request: R,
        timeout: Duration,
    ) -> Result<Call<Result<R::Response, RequestError>>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        self.request(Route::AnyBroker, request, timeout)
    }

    /// Submits one generated request through a semantic cluster route.
    ///
    /// For an accepted command, `timeout` includes mailbox residence, route
    /// discovery, DNS, reconnect backoff, write progress, and response wait.
    pub fn request<R>(
        &self,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<Call<Result<R::Response, RequestError>>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let (call, request) = erased_request(call_id, request, timeout);
        let submitted_at = Instant::now();
        self.commands
            .try_send(Command::Submit {
                route,
                request,
                submitted_at,
            })
            .map_err(SubmitError::from)?;
        Ok(call)
    }

    /// Submits one generated cluster request through an explicit isolation lane.
    ///
    /// Discovered-broker routes lazily own a physical connection per class. The
    /// bootstrap-oriented [`Route::AnyBroker`] path continues to use its seed.
    /// The timeout uses the same end-to-end admission semantics as [`Self::request`].
    pub fn request_in<R>(
        &self,
        traffic_class: TrafficClass,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<Call<Result<R::Response, RequestError>>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let (call, request) = erased_request_in(call_id, traffic_class, request, timeout);
        let submitted_at = Instant::now();
        self.commands
            .try_send(Command::Submit {
                route,
                request,
                submitted_at,
            })
            .map_err(SubmitError::from)?;
        Ok(call)
    }

    /// Submits one generated request and retains the exact semantic route used.
    ///
    /// The returned outcome has no receipt when the request settles before a
    /// controller, coordinator, or partition-leader fact reaches broker
    /// ownership. Its timeout has the same end-to-end semantics as
    /// [`Self::request`].
    pub fn request_tracked<R>(
        &self,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<RoutedCall<R::Response>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        self.request_tracked_in(TrafficClass::Interactive, route, request, timeout)
    }

    /// Submits one route-tracked request through an explicit isolation lane.
    pub fn request_tracked_in<R>(
        &self,
        traffic_class: TrafficClass,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<RoutedCall<R::Response>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let (call, request) = routed_request_in(call_id, traffic_class, request, timeout);
        let submitted_at = Instant::now();
        self.commands
            .try_send(Command::Submit {
                route,
                request,
                submitted_at,
            })
            .map_err(SubmitError::from)?;
        Ok(call)
    }
}

/// Why a command could not enter the bounded reactor mailbox.
#[derive(Debug)]
pub enum SubmitError {
    /// The mailbox has reached its configured command capacity.
    Full,
    /// The reactor has closed command admission permanently.
    Closed,
    /// The command remained unadmitted because the OS poller could not be woken.
    Wake(io::Error),
    /// Every public call identity has been allocated for this driver instance.
    IdentityExhausted,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("the driver command mailbox is full"),
            Self::Closed => formatter.write_str("the driver command mailbox is closed"),
            Self::Wake(_) => formatter.write_str("the driver I/O shard could not be woken"),
            Self::IdentityExhausted => {
                formatter.write_str("the driver call identity space is exhausted")
            }
        }
    }
}

impl std::error::Error for SubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wake(source) => Some(source),
            Self::Full | Self::Closed | Self::IdentityExhausted => None,
        }
    }
}

impl From<TrySendError<Command>> for SubmitError {
    fn from(error: TrySendError<Command>) -> Self {
        match error {
            TrySendError::Full(_) => Self::Full,
            TrySendError::Closed(_) => Self::Closed,
            TrySendError::Wake { command, source } => {
                drop(command);
                Self::Wake(source)
            }
        }
    }
}
