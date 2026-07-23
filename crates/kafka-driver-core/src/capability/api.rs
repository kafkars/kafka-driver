//! One broker-and-driver API version overlap retained for request selection.

use kafka_wire_core::{ApiKey, ApiVersion, VersionRange};

/// One API version range usable for the lifetime of a connection epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NegotiatedApi {
    api_key: ApiKey,
    versions: VersionRange,
}

impl NegotiatedApi {
    /// Creates an exact negotiated API entry for fixtures and fixed-version APIs.
    pub const fn new(api_key: ApiKey, version: ApiVersion) -> Self {
        Self {
            api_key,
            versions: VersionRange::new(version.value(), version.value()),
        }
    }

    /// Creates one negotiated API entry from an already validated overlap.
    pub const fn with_range(api_key: ApiKey, versions: VersionRange) -> Self {
        Self { api_key, versions }
    }

    /// Returns the Kafka API key.
    pub const fn api_key(self) -> ApiKey {
        self.api_key
    }

    /// Returns the mutually supported version range for this epoch.
    pub const fn versions(self) -> VersionRange {
        self.versions
    }

    /// Returns the highest mutually supported version for this epoch.
    pub const fn version(self) -> ApiVersion {
        self.versions.max()
    }

    /// Selects the highest negotiated version no greater than `maximum`.
    pub const fn highest_at_most(self, maximum: ApiVersion) -> Option<ApiVersion> {
        if maximum.value() < self.versions.min().value() {
            return None;
        }
        if maximum.value() < self.versions.max().value() {
            return Some(maximum);
        }
        Some(self.versions.max())
    }
}
