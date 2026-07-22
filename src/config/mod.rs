//! Public resource limits controlling driver admission and fairness.

mod broker;
mod limits;
mod resolver;
mod sasl;
#[cfg(feature = "tls-rustls")]
mod tls;

#[cfg(test)]
mod resolver_test;
#[cfg(test)]
mod sasl_test;

pub use limits::DriverLimits;
pub use resolver::ResolverLimits;
pub use sasl::{SaslConfig, SaslConfigError};
#[cfg(feature = "tls-rustls")]
pub use tls::TlsClientConfig;

pub(crate) use broker::{BrokerConfig, BrokerSecurity};
