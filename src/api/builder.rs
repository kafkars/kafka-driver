//! Public selection of one driver target, security policy, and hosting mode.

use std::{net::SocketAddr, sync::Arc};

use kafka_driver_core::BootstrapSet;

use crate::{
    config::{BootstrapConfig, BrokerConfig, ClientId, DriverLimits, DriverTarget},
    host::DriverHost,
    observation::Observation,
    reactor::Reactor,
};

use super::{CallIds, Driver, DriverBuildError, identity::DriverIdentity};

/// Builder for one driver using either embedded or dedicated hosting.
#[derive(Clone, Debug, Default)]
pub struct DriverBuilder {
    limits: DriverLimits,
    target: Option<DriverTarget>,
    sasl: Option<crate::SaslConfig>,
    client_id: Option<String>,
}

impl DriverBuilder {
    /// Replaces the default admission and fairness limits.
    #[must_use]
    pub const fn limits(mut self, limits: DriverLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Configures the single plaintext broker endpoint owned by this reactor.
    ///
    /// This direct numeric target owns one connection generation and does not
    /// reconnect after a terminal connection failure during the Bornera cutover.
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
    ///
    /// This direct numeric target owns one connection generation and does not
    /// reconnect after a terminal connection failure during the Bornera cutover.
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

    /// Sets the immutable client identifier written into every Kafka request header.
    #[must_use]
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Builds a driver handle and an embedded, caller-driven reactor.
    ///
    /// The reactor is thread-affine: construct and drive it on its owner thread.
    pub fn build_reactor(self) -> Result<(Driver, Reactor), DriverBuildError> {
        let Some(target) = self.target else {
            return Err(DriverBuildError::MissingTarget);
        };
        let client_id = self
            .client_id
            .map(ClientId::try_new)
            .transpose()
            .map_err(DriverBuildError::client_id)?;
        let identity = DriverIdentity::allocate().ok_or(DriverBuildError::IdentityExhausted)?;
        let target = target.with_sasl(self.sasl).with_client_id(client_id);
        let call_ids = Arc::new(CallIds::new());
        let observation = Arc::new(Observation::default());
        let (commands, shutdown, reactor) = Reactor::new(
            &self.limits,
            Some(target),
            Arc::clone(&call_ids),
            Arc::clone(&observation),
        )
        .map_err(DriverBuildError::new)?;
        let topic_view_result_capacity_bytes = crate::TopicView::maximum_available_bytes(
            self.limits.metadata().partition_leaders().max_partitions(),
        );
        Ok((
            Driver::new(
                commands,
                shutdown,
                call_ids,
                observation,
                identity,
                topic_view_result_capacity_bytes,
            ),
            reactor,
        ))
    }

    /// Builds a driver and starts its reactor on one dedicated thread.
    pub fn spawn(self) -> Result<(Driver, DriverHost), DriverBuildError> {
        DriverHost::spawn(move || self.build_reactor())
    }
}
