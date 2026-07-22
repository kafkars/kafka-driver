//! Public construction, admission, and shutdown handle for one driver reactor.

use std::{fmt, io, net::SocketAddr, sync::Arc, time::Duration};

use kafka_wire::RequestResponsePair;

use crate::{
    completion::completion_pair,
    config::{BrokerConfig, DriverLimits},
    reactor::{Command, MailboxSender, Reactor, TrySendError},
    request::erased_request,
};

use super::{Call, DriverBuildError, RequestError, identity::CallIds};

/// Cloneable command-admission handle for one driver reactor.
#[derive(Clone, Debug)]
pub struct Driver {
    commands: MailboxSender<Command>,
    call_ids: Arc<CallIds>,
}

impl Driver {
    /// Starts configuration of a driver and its reactor host.
    pub fn builder() -> DriverBuilder {
        DriverBuilder::default()
    }

    /// Requests terminal shutdown through the bounded command mailbox.
    pub fn shutdown(&self) -> Result<Call<()>, SubmitError> {
        let (completion, sender) = completion_pair();
        let call = Call::new(completion);
        let command = Command::Shutdown { completion: sender };
        self.commands.try_send(command).map_err(SubmitError::from)?;
        Ok(call)
    }

    /// Submits one generated request using that connection's negotiated version.
    pub fn call<R>(
        &self,
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
        self.commands
            .try_send(Command::Submit { request })
            .map_err(SubmitError::from)?;
        Ok(call)
    }
}

/// Builder for one command handle and embedded reactor pair.
#[derive(Clone, Debug, Default)]
pub struct DriverBuilder {
    limits: DriverLimits,
    broker: Option<BrokerConfig>,
}

impl DriverBuilder {
    /// Replaces the default admission and fairness limits.
    #[must_use]
    pub const fn limits(mut self, limits: DriverLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Configures the single plaintext broker endpoint owned by this reactor.
    #[must_use]
    pub fn broker(mut self, address: SocketAddr) -> Self {
        self.broker = Some(BrokerConfig::plaintext(address));
        self
    }

    /// Configures one broker protected by the supplied rustls client policy.
    #[cfg(feature = "tls-rustls")]
    #[must_use]
    pub fn rustls_broker(mut self, address: SocketAddr, tls: crate::TlsClientConfig) -> Self {
        self.broker = Some(BrokerConfig::rustls(address, tls));
        self
    }

    /// Builds a driver handle and an embedded, caller-driven reactor.
    pub fn build_reactor(self) -> Result<(Driver, Reactor), DriverBuildError> {
        let (commands, reactor) =
            Reactor::new(self.limits, self.broker).map_err(DriverBuildError::new)?;
        Ok((
            Driver {
                commands,
                call_ids: Arc::new(CallIds::new()),
            },
            reactor,
        ))
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
