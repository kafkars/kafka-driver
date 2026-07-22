//! Immutable bounded lookup of APIs usable on one connection epoch.

use std::num::NonZeroUsize;

use kafka_wire_core::{ApiKey, ApiVersion};

use super::{CapabilityError, NegotiatedApi};

/// Canonically ordered API versions negotiated for one connection epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NegotiatedCapabilities {
    apis: Vec<NegotiatedApi>,
}

impl NegotiatedCapabilities {
    /// Builds a bounded set from entries ordered by strictly increasing API key.
    pub fn try_from_iter(
        apis: impl IntoIterator<Item = NegotiatedApi>,
        capacity: NonZeroUsize,
    ) -> Result<Self, CapabilityError> {
        let limit = capacity.get();
        let mut retained: Vec<NegotiatedApi> = Vec::new();
        for api in apis {
            if let Some(previous) = retained.last().copied()
                && previous.api_key() >= api.api_key()
            {
                return Err(CapabilityError::NonAscending {
                    previous,
                    rejected: api,
                });
            }
            if retained.len() == limit {
                return Err(CapabilityError::CapacityReached {
                    limit,
                    rejected: api,
                });
            }
            retained.push(api);
        }
        Ok(Self { apis: retained })
    }

    /// Returns the selected version for `api_key`, if the broker and driver overlap.
    pub fn version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.apis
            .binary_search_by_key(&api_key, |api| api.api_key())
            .ok()
            .map(|index| self.apis[index].version())
    }

    /// Returns the number of retained mutually supported APIs.
    pub const fn len(&self) -> usize {
        self.apis.len()
    }

    /// Returns whether no mutually supported API was advertised.
    pub const fn is_empty(&self) -> bool {
        self.apis.is_empty()
    }

    /// Iterates negotiated APIs in ascending key order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = NegotiatedApi> + '_ {
        self.apis.iter().copied()
    }
}
