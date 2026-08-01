//! Public resource limits controlling driver admission and fairness.

mod bootstrap;
mod broker;
mod client_id;
mod controller_waiting;
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
mod client_id_test;
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
#[cfg(all(test, feature = "tls-rustls"))]
mod tls_test;

pub use controller_waiting::ControllerWaitingLimits;
pub use coordinator::CoordinatorLimits;
pub use limits::DriverLimits;
pub use metadata::MetadataLimits;
pub use resolver::ResolverLimits;
pub use sasl::{SaslConfig, SaslConfigError};
pub use scram_proof::ScramProofLimits;
#[cfg(feature = "tls-rustls")]
pub use tls::{TlsClientConfig, TlsClientPolicy};

pub(crate) use bootstrap::BootstrapConfig;
pub(crate) use broker::{BrokerAddresses, BrokerConfig, BrokerSecurity, BrokerTemplate};
pub(crate) use client_id::{ClientId, ClientIdError};
pub(crate) use target::DriverTarget;
#[cfg(feature = "tls-rustls")]
pub(crate) use tls::{TlsConnectionConfig, TlsSessionError};
