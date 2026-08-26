//! Reusable broker policy consumed after endpoint resolution.

#[cfg(feature = "tls-rustls")]
use crate::config::TlsClientPolicy;
use crate::config::{ClientId, SaslConfig};

/// Reusable transport and authentication policy applied after address selection.
#[derive(Clone, Debug)]
pub(crate) struct BrokerTemplate {
    parts: BrokerTemplateParts,
}

impl BrokerTemplate {
    pub(crate) const fn plaintext() -> Self {
        Self {
            parts: BrokerTemplateParts::Plaintext {
                sasl: None,
                client_id: None,
            },
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(tls: TlsClientPolicy) -> Self {
        Self {
            parts: BrokerTemplateParts::Rustls {
                tls,
                sasl: None,
                client_id: None,
            },
        }
    }

    pub(crate) fn with_sasl(mut self, replacement: Option<SaslConfig>) -> Self {
        match &mut self.parts {
            BrokerTemplateParts::Plaintext { sasl, .. } => *sasl = replacement,
            #[cfg(feature = "tls-rustls")]
            BrokerTemplateParts::Rustls { sasl, .. } => *sasl = replacement,
        }
        self
    }

    pub(crate) fn with_client_id(mut self, replacement: Option<ClientId>) -> Self {
        match &mut self.parts {
            BrokerTemplateParts::Plaintext { client_id, .. } => *client_id = replacement,
            #[cfg(feature = "tls-rustls")]
            BrokerTemplateParts::Rustls { client_id, .. } => *client_id = replacement,
        }
        self
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        match &self.parts {
            BrokerTemplateParts::Plaintext { sasl, .. } => sasl,
            #[cfg(feature = "tls-rustls")]
            BrokerTemplateParts::Rustls { sasl, .. } => sasl,
        }
        .as_ref()
        .is_some_and(SaslConfig::requires_proof_worker)
    }

    pub(crate) fn into_parts(self) -> BrokerTemplateParts {
        self.parts
    }
}

/// Consumed endpoint-family policy for building repeatable typed lane plans.
#[derive(Clone, Debug)]
pub(crate) enum BrokerTemplateParts {
    Plaintext {
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
    },
    #[cfg(feature = "tls-rustls")]
    Rustls {
        tls: TlsClientPolicy,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
    },
}
