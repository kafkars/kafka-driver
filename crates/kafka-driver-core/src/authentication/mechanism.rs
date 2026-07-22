//! Public mechanism identity and wire-version selections without credentials.

use kafka_wire_core::{ApiKey, ApiVersion};

use super::AuthenticationLimits;

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

impl SaslMechanism {
    /// Returns the exact Kafka SASL mechanism name used by `SaslHandshake`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// Negotiated Kafka API versions used to carry one SASL mechanism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SaslProtocol {
    mechanism: SaslMechanism,
    handshake_version: ApiVersion,
    authenticate_version: ApiVersion,
}

/// Connection-owned non-secret policy for selecting SASL wire capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticationPolicy {
    mechanism: SaslMechanism,
    handshake_api_key: ApiKey,
    authenticate_api_key: ApiKey,
    limits: AuthenticationLimits,
}

impl AuthenticationPolicy {
    /// Names the generated wire APIs without duplicating their numeric keys.
    pub const fn new(
        mechanism: SaslMechanism,
        handshake_api_key: ApiKey,
        authenticate_api_key: ApiKey,
        limits: AuthenticationLimits,
    ) -> Self {
        Self {
            mechanism,
            handshake_api_key,
            authenticate_api_key,
            limits,
        }
    }

    pub(crate) const fn mechanism(self) -> SaslMechanism {
        self.mechanism
    }

    pub(crate) const fn handshake_api_key(self) -> ApiKey {
        self.handshake_api_key
    }

    pub(crate) const fn authenticate_api_key(self) -> ApiKey {
        self.authenticate_api_key
    }

    pub(crate) const fn limits(self) -> AuthenticationLimits {
        self.limits
    }
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
