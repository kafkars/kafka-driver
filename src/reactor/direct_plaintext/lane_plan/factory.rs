//! Repeatable typed lane-plan factories for one endpoint family.

use std::io;

use bornera::{RegisteredTransport, TcpTransport};
use kafka_driver_core::{BrokerEndpoint, ResolvedAddressSet};

#[cfg(feature = "tls-rustls")]
use crate::config::{TlsClientConfig, TlsClientPolicy, TlsSessionError};
use crate::{
    config::{
        BrokerAddresses, BrokerTemplate, BrokerTemplateParts, ClientId, DriverLimits, SaslConfig,
    },
    reactor::broker::BrokerLimits,
};

#[cfg(feature = "tls-rustls")]
use super::super::rustls_transport::DirectRustlsTransport;
use super::BorneraLanePlan;

#[cfg(test)]
#[path = "factory_test.rs"]
mod test;

/// One homogeneous transport family selected before a connection set exists.
pub(in crate::reactor::direct_plaintext) enum BorneraEndpointFamily {
    Plaintext(PlaintextLanePlanFactory),
    #[cfg(feature = "tls-rustls")]
    Rustls(RustlsLanePlanFactory),
}

impl BorneraEndpointFamily {
    pub(in crate::reactor::direct_plaintext) fn from_template(
        driver: &DriverLimits,
        broker: BrokerLimits,
        template: BrokerTemplate,
    ) -> Self {
        match template.into_parts() {
            BrokerTemplateParts::Plaintext { sasl, client_id } => {
                Self::Plaintext(PlaintextLanePlanFactory {
                    policy: LanePolicy::new(driver, broker, sasl, client_id),
                })
            }
            #[cfg(feature = "tls-rustls")]
            BrokerTemplateParts::Rustls {
                tls,
                sasl,
                client_id,
            } => Self::Rustls(RustlsLanePlanFactory {
                policy: LanePolicy::new(driver, broker, sasl, client_id),
                tls,
            }),
        }
    }
}

/// Builds a fresh lane plan after one logical endpoint has been resolved.
pub(in crate::reactor::direct_plaintext) trait BorneraLanePlanFactory<T: RegisteredTransport> {
    fn at_resolved(
        &self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<T>>;
}

pub(in crate::reactor) struct PlaintextLanePlanFactory {
    policy: LanePolicy,
}

impl BorneraLanePlanFactory<TcpTransport> for PlaintextLanePlanFactory {
    fn at_resolved(
        &self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        Ok(BorneraLanePlan::plaintext(
            &self.policy.driver,
            self.policy.broker,
            BrokerAddresses::Resolved {
                endpoint,
                addresses,
            },
            self.policy.sasl.clone(),
            self.policy.client_id.clone(),
        ))
    }
}

#[cfg(feature = "tls-rustls")]
pub(in crate::reactor) struct RustlsLanePlanFactory {
    policy: LanePolicy,
    tls: TlsClientPolicy,
}

#[cfg(feature = "tls-rustls")]
impl RustlsLanePlanFactory {
    fn bind_endpoint(&self, endpoint: &BrokerEndpoint) -> Result<TlsClientConfig, TlsSessionError> {
        self.tls.bind_endpoint(endpoint)
    }
}

#[cfg(feature = "tls-rustls")]
impl BorneraLanePlanFactory<DirectRustlsTransport> for RustlsLanePlanFactory {
    fn at_resolved(
        &self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<DirectRustlsTransport>> {
        let tls = self.bind_endpoint(&endpoint).map_err(io::Error::other)?;
        Ok(BorneraLanePlan::rustls(
            &self.policy.driver,
            self.policy.broker,
            BrokerAddresses::Resolved {
                endpoint,
                addresses,
            },
            tls,
            self.policy.sasl.clone(),
            self.policy.client_id.clone(),
        ))
    }
}

struct LanePolicy {
    driver: DriverLimits,
    broker: BrokerLimits,
    sasl: Option<SaslConfig>,
    client_id: Option<ClientId>,
}

impl LanePolicy {
    const fn new(
        driver: &DriverLimits,
        broker: BrokerLimits,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
    ) -> Self {
        Self {
            driver: *driver,
            broker,
            sasl,
            client_id,
        }
    }
}
