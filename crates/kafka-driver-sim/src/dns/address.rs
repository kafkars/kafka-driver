//! Broker names and logical resolved addresses without operating-system sockets.

use std::{error::Error, fmt, num::NonZeroU16};

/// Nonempty broker host name retained exactly as configured.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HostName(String);

impl HostName {
    /// Validates and owns a configured broker host name.
    pub fn new(value: impl Into<String>) -> Result<Self, HostNameError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(HostNameError);
        }
        Ok(Self(value))
    }

    /// Returns the configured host name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HostName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A configured broker name and nonzero TCP port.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BrokerEndpoint {
    host: HostName,
    port: NonZeroU16,
}

impl BrokerEndpoint {
    /// Creates a broker endpoint from already validated parts.
    pub const fn new(host: HostName, port: NonZeroU16) -> Self {
        Self { host, port }
    }

    /// Returns the configured broker host name.
    pub const fn host(&self) -> &HostName {
        &self.host
    }

    /// Returns the configured broker port.
    pub const fn port(&self) -> NonZeroU16 {
        self.port
    }
}

/// Logical IP address usable in deterministic scripts without OS networking types.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IpAddress {
    /// Four IPv4 octets in network order.
    V4([u8; 4]),
    /// Sixteen IPv6 octets in network order.
    V6([u8; 16]),
}

/// One logical address returned by a scripted resolver.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResolvedAddress {
    ip: IpAddress,
    port: NonZeroU16,
}

impl ResolvedAddress {
    /// Creates a resolved endpoint from its logical address and port.
    pub const fn new(ip: IpAddress, port: NonZeroU16) -> Self {
        Self { ip, port }
    }

    /// Returns the logical IP address.
    pub const fn ip(self) -> IpAddress {
        self.ip
    }

    /// Returns the resolved broker port.
    pub const fn port(self) -> NonZeroU16 {
        self.port
    }
}

/// Rejection of an empty or whitespace-only host name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostNameError;

impl fmt::Display for HostNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("broker host name must not be empty")
    }
}

impl Error for HostNameError {}
