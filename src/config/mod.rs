//! Public resource limits controlling driver admission and fairness.

mod bootstrap;
mod broker;
mod coordinator;
mod limits;
mod metadata;
mod resolver;
mod sasl;
mod scram_proof;
mod target;
#[cfg(feature = "tls-rustls")]
mod tls;

#[cfg(test)]
mod coordinator_test;
#[cfg(test)]
mod metadata_test;
#[cfg(test)]
mod resolver_test;
#[cfg(test)]
mod sasl_test;
#[cfg(test)]
mod scram_proof_test;

pub use coordinator::CoordinatorLimits;
pub use limits::DriverLimits;
pub use metadata::MetadataLimits;
pub use resolver::ResolverLimits;
pub use sasl::{SaslConfig, SaslConfigError};
pub use scram_proof::ScramProofLimits;
#[cfg(feature = "tls-rustls")]
pub use tls::TlsClientConfig;

pub(crate) use bootstrap::BootstrapConfig;
pub(crate) use broker::{BrokerConfig, BrokerSecurity, BrokerTemplate};
pub(crate) use target::DriverTarget;
