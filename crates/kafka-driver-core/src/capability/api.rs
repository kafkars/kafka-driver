//! One broker-supported API version selected by local negotiation policy.

use kafka_wire_core::{ApiKey, ApiVersion};

/// One API version usable for the lifetime of a connection epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NegotiatedApi {
    api_key: ApiKey,
    version: ApiVersion,
}

impl NegotiatedApi {
    /// Creates one negotiated API entry from protocol-owned numeric vocabulary.
    pub const fn new(api_key: ApiKey, version: ApiVersion) -> Self {
        Self { api_key, version }
    }

    /// Returns the Kafka API key.
    pub const fn api_key(self) -> ApiKey {
        self.api_key
    }

    /// Returns the mutually supported version selected for this epoch.
    pub const fn version(self) -> ApiVersion {
        self.version
    }
}
