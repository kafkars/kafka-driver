//! Public selection of one driver target, security policy, and hosting mode.

use std::{net::SocketAddr, sync::Arc};

use kafka_driver_core::BootstrapSet;

use crate::{
    config::{BootstrapConfig, BrokerConfig, DriverLimits, DriverTarget},
    host::DriverHost,
    observation::Observation,
    reactor::Reactor,
};

use super::{CallIds, Driver, DriverBuildError};

/// Builder for one driver using either embedded or dedicated hosting.
#[derive(Clone, Debug, Default)]
pub struct DriverBuilder {
    limits: DriverLimits,
    target: Option<DriverTarget>,
    sasl: Option<crate::SaslConfig>,
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
        self.target = Some(DriverTarget::Direct(BrokerConfig::plaintext(address)));
        self
    }

    /// Configures bounded plaintext bootstrap membership for cluster discovery.
    #[must_use]
    pub fn bootstrap(mut self, endpoints: BootstrapSet) -> Self {
        self.target = Some(DriverTarget::Bootstrap(BootstrapConfig::plaintext(
            endpoints,
        )));
        self
    }

    /// Configures one broker protected by an explicitly bound rustls identity.
    #[cfg(feature = "tls-rustls")]
    #[must_use]
    pub fn rustls_broker(mut self, address: SocketAddr, tls: crate::TlsClientConfig) -> Self {
        self.target = Some(DriverTarget::Direct(BrokerConfig::rustls(address, tls)));
        self
    }

    /// Configures bounded TLS bootstrap with identity derived per logical endpoint.
    #[cfg(feature = "tls-rustls")]
    #[must_use]
    pub fn rustls_bootstrap(
        mut self,
        endpoints: BootstrapSet,
        tls: crate::TlsClientPolicy,
    ) -> Self {
        self.target = Some(DriverTarget::Bootstrap(BootstrapConfig::rustls(
            endpoints, tls,
        )));
        self
    }

    /// Requires the configured broker endpoint to complete SASL authentication.
    #[must_use]
    pub fn sasl(mut self, sasl: crate::SaslConfig) -> Self {
        self.sasl = Some(sasl);
        self
    }

    /// Builds a driver handle and an embedded, caller-driven reactor.
    pub fn build_reactor(self) -> Result<(Driver, Reactor), DriverBuildError> {
        let Some(target) = self.target else {
            return Err(DriverBuildError::MissingTarget);
        };
        let target = target.with_sasl(self.sasl);
        let call_ids = Arc::new(CallIds::new());
        let observation = Arc::new(Observation::default());
        let (commands, shutdown, reactor) = Reactor::new(
            &self.limits,
            Some(target),
            Arc::clone(&call_ids),
            Arc::clone(&observation),
        )
        .map_err(DriverBuildError::new)?;
        Ok((
            Driver::new(commands, shutdown, call_ids, observation),
            reactor,
        ))
    }

    /// Builds a driver and starts its reactor on one dedicated thread.
    pub fn spawn(self) -> Result<(Driver, DriverHost), DriverBuildError> {
        let (driver, reactor) = self.build_reactor()?;
        let host = DriverHost::spawn(reactor).map_err(DriverBuildError::new)?;
        Ok((driver, host))
    }
}
