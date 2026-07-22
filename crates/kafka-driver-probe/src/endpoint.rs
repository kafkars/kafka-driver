//! Conversion from bounded operator endpoints into driver bootstrap ownership.

use std::{error::Error, fmt, net::SocketAddr, num::NonZeroU16};

use kafka_driver::{BootstrapLimits, BootstrapSet, BrokerEndpoint, HostName};

pub(crate) fn bootstrap(value: &str) -> Result<BootstrapSet, EndpointError> {
    let limits = BootstrapLimits::default();
    let mut endpoints = Vec::with_capacity(limits.max_endpoints().get().min(4));
    for (inspected, value) in value.split(',').enumerate() {
        if inspected == limits.max_endpoints().get() {
            return Err(EndpointError::Bootstrap);
        }
        endpoints.push(endpoint(value)?);
    }
    BootstrapSet::try_from_iter(endpoints, limits).map_err(|_| EndpointError::Bootstrap)
}

fn endpoint(value: &str) -> Result<BrokerEndpoint, EndpointError> {
    let (host, port) = split(value).ok_or(EndpointError::Shape)?;
    let host = HostName::new(host.to_owned()).map_err(|_| EndpointError::Host)?;
    let port = port
        .parse::<u16>()
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(EndpointError::Port)?;
    Ok(BrokerEndpoint::new(host, port))
}

pub(crate) fn socket(value: &str) -> Result<SocketAddr, EndpointError> {
    value.parse().map_err(|_| EndpointError::Socket)
}

fn split(value: &str) -> Option<(&str, &str)> {
    if let Some(rest) = value.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        return (!host.is_empty() && !port.is_empty()).then_some((host, port));
    }

    let (host, port) = value.rsplit_once(':')?;
    (!host.is_empty() && !port.is_empty() && !host.contains(':')).then_some((host, port))
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
            Self::Shape => formatter.write_str(
                "bootstrap endpoints must be comma-separated host:port or [ipv6]:port values",
            ),
            Self::Host => formatter.write_str("bootstrap host is invalid"),
            Self::Port => formatter.write_str("bootstrap port must be between 1 and 65535"),
            Self::Bootstrap => formatter.write_str("bootstrap endpoints exceeded driver admission"),
            Self::Socket => formatter.write_str("direct endpoint must be a numeric IP and port"),
        }
    }
}

impl Error for EndpointError {}
