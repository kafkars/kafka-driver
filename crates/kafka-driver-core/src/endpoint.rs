//! Socket-free broker endpoint vocabulary shared by policy and simulation.

use std::{error::Error, fmt, num::NonZeroU16};

/// Nonempty bounded broker host retained exactly as configured or advertised.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HostName(String);

impl HostName {
    /// Maximum encoded bytes accepted for one resolver host.
    pub const MAX_BYTES: usize = 253;

    /// Validates and owns a broker resolver host.
    pub fn new(value: impl Into<String>) -> Result<Self, HostNameError> {
        let value = value.into();
        if value.is_empty() {
            return Err(HostNameError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(HostNameError::TooLong {
                bytes: value.len(),
                limit: Self::MAX_BYTES,
            });
        }
        if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(HostNameError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    /// Returns the configured or advertised resolver host.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HostName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A configured or advertised broker host and nonzero TCP port.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BrokerEndpoint {
    host: HostName,
    port: NonZeroU16,
}

impl BrokerEndpoint {
    /// Creates a broker endpoint from already validated parts.
    pub const fn new(host: HostName, port: NonZeroU16) -> Self {
        Self { host, port }
    }

    /// Returns the broker resolver host.
    pub const fn host(&self) -> &HostName {
        &self.host
    }

    /// Returns the broker TCP port.
    pub const fn port(&self) -> NonZeroU16 {
        self.port
    }
}

/// Logical IP address usable without granting operating-system capabilities.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IpAddress {
    /// Four IPv4 octets in network order.
    V4([u8; 4]),
    /// Sixteen IPv6 octets in network order.
    V6([u8; 16]),
}

/// One logical address returned by a resolver.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResolvedAddress {
    socket: ResolvedSocketAddress,
}

impl ResolvedAddress {
    /// Creates a resolved endpoint with no IPv6 flow or interface scope.
    pub const fn new(ip: IpAddress, port: NonZeroU16) -> Self {
        let socket = match ip {
            IpAddress::V4(octets) => ResolvedSocketAddress::V4 { octets, port },
            IpAddress::V6(octets) => ResolvedSocketAddress::V6 {
                octets,
                port,
                flow_info: 0,
                scope_id: 0,
            },
        };
        Self { socket }
    }

    /// Creates an IPv6 endpoint retaining resolver-supplied flow and interface scope.
    pub const fn ipv6(octets: [u8; 16], port: NonZeroU16, flow_info: u32, scope_id: u32) -> Self {
        Self {
            socket: ResolvedSocketAddress::V6 {
                octets,
                port,
                flow_info,
                scope_id,
            },
        }
    }

    /// Returns the logical IP address.
    pub const fn ip(self) -> IpAddress {
        match self.socket {
            ResolvedSocketAddress::V4 { octets, .. } => IpAddress::V4(octets),
            ResolvedSocketAddress::V6 { octets, .. } => IpAddress::V6(octets),
        }
    }

    /// Returns the resolved broker port.
    pub const fn port(self) -> NonZeroU16 {
        match self.socket {
            ResolvedSocketAddress::V4 { port, .. } | ResolvedSocketAddress::V6 { port, .. } => port,
        }
    }

    /// Returns IPv6 flow information, or zero for IPv4.
    pub const fn flow_info(self) -> u32 {
        match self.socket {
            ResolvedSocketAddress::V4 { .. } => 0,
            ResolvedSocketAddress::V6 { flow_info, .. } => flow_info,
        }
    }

    /// Returns the IPv6 interface scope, or zero for IPv4.
    pub const fn scope_id(self) -> u32 {
        match self.socket {
            ResolvedSocketAddress::V4 { .. } => 0,
            ResolvedSocketAddress::V6 { scope_id, .. } => scope_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ResolvedSocketAddress {
    V4 {
        octets: [u8; 4],
        port: NonZeroU16,
    },
    V6 {
        octets: [u8; 16],
        port: NonZeroU16,
        flow_info: u32,
        scope_id: u32,
    },
}

/// Why a broker resolver host was rejected before external work.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostNameError {
    /// The resolver host contained no bytes.
    Empty,
    /// The resolver host exceeded its persistent byte bound.
    TooLong {
        /// Observed encoded byte count.
        bytes: usize,
        /// Maximum accepted encoded byte count.
        limit: usize,
    },
    /// The resolver host contained whitespace or a non-ASCII code point.
    InvalidCharacter,
}

impl fmt::Display for HostNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("broker host must not be empty"),
            Self::TooLong { bytes, limit } => {
                write!(
                    formatter,
                    "broker host uses {bytes} bytes, limit is {limit}"
                )
            }
            Self::InvalidCharacter => {
                formatter.write_str("broker host must be printable ASCII without whitespace")
            }
        }
    }
}

impl Error for HostNameError {}
