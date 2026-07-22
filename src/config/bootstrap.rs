//! Internal bootstrap membership paired with reusable broker connection policy.

use kafka_driver_core::BootstrapSet;

use super::{BrokerTemplate, SaslConfig};

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

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.broker = self.broker.with_sasl(sasl);
        self
    }

    pub(crate) const fn broker_template(&self) -> &BrokerTemplate {
        &self.broker
    }

    pub(crate) fn into_parts(self) -> (BootstrapSet, BrokerTemplate) {
        (self.endpoints, self.broker)
    }
}
