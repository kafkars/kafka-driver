//! Public mechanism identity and wire-version selections without credentials.

use kafka_wire_core::ApiVersion;

/// SASL mechanism selected for one broker connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SaslMechanism {
    /// SASL PLAIN using an authorization identity, username, and password.
    Plain,
    /// SCRAM with SHA-256.
    ScramSha256,
    /// SCRAM with SHA-512.
    ScramSha512,
}

/// Negotiated Kafka API versions used to carry one SASL mechanism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SaslProtocol {
    mechanism: SaslMechanism,
    handshake_version: ApiVersion,
    authenticate_version: ApiVersion,
}

impl SaslProtocol {
    /// Retains versions already selected from the connection capability set.
    pub const fn new(
        mechanism: SaslMechanism,
        handshake_version: ApiVersion,
        authenticate_version: ApiVersion,
    ) -> Self {
        Self {
            mechanism,
            handshake_version,
            authenticate_version,
        }
    }

    /// Returns the selected mechanism without credential material.
    pub const fn mechanism(self) -> SaslMechanism {
        self.mechanism
    }

    /// Returns the negotiated `SaslHandshake` version.
    pub const fn handshake_version(self) -> ApiVersion {
        self.handshake_version
    }

    /// Returns the negotiated `SaslAuthenticate` version.
    pub const fn authenticate_version(self) -> ApiVersion {
        self.authenticate_version
    }
}
