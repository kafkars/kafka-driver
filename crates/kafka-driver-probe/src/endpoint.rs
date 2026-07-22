//! Conversion from one operator endpoint into bounded driver bootstrap ownership.

use std::{error::Error, fmt, net::SocketAddr, num::NonZeroU16};

use kafka_driver::{BootstrapLimits, BootstrapSet, BrokerEndpoint, HostName};

pub(crate) fn bootstrap(value: &str) -> Result<BootstrapSet, EndpointError> {
    let (host, port) = split(value).ok_or(EndpointError::Shape)?;
    let host = HostName::new(host).map_err(|_| EndpointError::Host)?;
    let port = port
        .parse::<u16>()
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(EndpointError::Port)?;
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(host, port)],
        BootstrapLimits::default(),
    )
    .map_err(|_| EndpointError::Bootstrap)
}

pub(crate) fn socket(value: &str) -> Result<SocketAddr, EndpointError> {
    value.parse().map_err(|_| EndpointError::Socket)
}

fn split(value: &str) -> Option<(String, &str)> {
    if let Some(rest) = value.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        return (!host.is_empty() && !port.is_empty()).then(|| (host.to_owned(), port));
    }

    let (host, port) = value.rsplit_once(':')?;
    (!host.is_empty() && !port.is_empty() && !host.contains(':')).then(|| (host.to_owned(), port))
}

/// Why a configured smoke endpoint could not enter the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndpointError {
    Shape,
    Host,
    Port,
    Bootstrap,
    Socket,
}

impl fmt::Display for EndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape => {
                formatter.write_str("bootstrap endpoint must be host:port or [ipv6]:port")
            }
            Self::Host => formatter.write_str("bootstrap host is invalid"),
            Self::Port => formatter.write_str("bootstrap port must be between 1 and 65535"),
            Self::Bootstrap => formatter.write_str("bootstrap endpoint exceeded driver admission"),
            Self::Socket => formatter.write_str("direct endpoint must be a numeric IP and port"),
        }
    }
}

impl Error for EndpointError {}
