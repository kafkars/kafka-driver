//! Internal bootstrap membership paired with reusable broker connection policy.

use kafka_driver_core::BootstrapSet;

#[cfg(feature = "tls-rustls")]
use super::TlsClientPolicy;
use super::{BrokerTemplate, ClientId, SaslConfig};

/// Configured endpoints and security policy retained through bootstrap resolution.
#[derive(Clone, Debug)]
pub(crate) struct BootstrapConfig {
    endpoints: BootstrapSet,
    broker: BrokerTemplate,
}

impl BootstrapConfig {
    pub(crate) fn plaintext(endpoints: BootstrapSet) -> Self {
        Self {
            endpoints,
            broker: BrokerTemplate::plaintext(),
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) fn rustls(endpoints: BootstrapSet, tls: TlsClientPolicy) -> Self {
        Self {
            endpoints,
            broker: BrokerTemplate::rustls(tls),
        }
    }

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.broker = self.broker.with_sasl(sasl);
        self
    }

    pub(crate) fn with_client_id(mut self, client_id: Option<ClientId>) -> Self {
        self.broker = self.broker.with_client_id(client_id);
        self
    }

    pub(crate) const fn broker_template(&self) -> &BrokerTemplate {
        &self.broker
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        self.broker.requires_proof_worker()
    }

    pub(crate) fn into_parts(self) -> (BootstrapSet, BrokerTemplate) {
        (self.endpoints, self.broker)
    }
}
