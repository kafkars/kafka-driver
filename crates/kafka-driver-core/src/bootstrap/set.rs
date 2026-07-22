//! Stable bounded bootstrap construction with first-occurrence deduplication.

use crate::BrokerEndpoint;

use super::{BootstrapError, BootstrapLimits};

/// Nonempty configured endpoints retained in stable selection order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapSet {
    endpoints: Vec<BrokerEndpoint>,
}

impl BootstrapSet {
    /// Admits a bounded sequence and retains only each endpoint's first occurrence.
    pub fn try_from_iter(
        endpoints: impl IntoIterator<Item = BrokerEndpoint>,
        limits: BootstrapLimits,
    ) -> Result<Self, BootstrapError> {
        let limit = limits.max_endpoints().get();
        let mut unique = Vec::with_capacity(limit.min(4));
        for (inspected, endpoint) in endpoints.into_iter().enumerate() {
            if inspected == limit {
                return Err(BootstrapError::Capacity { limit });
            }
            if !unique.contains(&endpoint) {
                unique.push(endpoint);
            }
        }
        if unique.is_empty() {
            return Err(BootstrapError::Empty);
        }
        Ok(Self { endpoints: unique })
    }

    /// Returns the distinct configured endpoint count.
    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Returns whether no endpoint exists.
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Iterates in stable configured order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &BrokerEndpoint> {
        self.endpoints.iter()
    }

    pub(super) fn at(&self, index: usize) -> &BrokerEndpoint {
        &self.endpoints[index]
    }
}
