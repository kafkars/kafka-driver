//! Stable bounded ownership of one successful resolver result.

use crate::ResolvedAddress;

use super::{ResolutionLimits, ResolvedAddressSetError};

/// Nonempty distinct addresses retained in resolver preference order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedAddressSet {
    addresses: Vec<ResolvedAddress>,
}

impl ResolvedAddressSet {
    /// Admits a bounded result and retains each address's first occurrence.
    pub fn try_from_iter(
        addresses: impl IntoIterator<Item = ResolvedAddress>,
        limits: ResolutionLimits,
    ) -> Result<Self, ResolvedAddressSetError> {
        let limit = limits.max_addresses().get();
        let mut unique = Vec::with_capacity(limit.min(4));
        for (inspected, address) in addresses.into_iter().enumerate() {
            if inspected == limit {
                return Err(ResolvedAddressSetError::Capacity { limit });
            }
            if !unique.contains(&address) {
                unique.push(address);
            }
        }
        if unique.is_empty() {
            return Err(ResolvedAddressSetError::Empty);
        }
        Ok(Self { addresses: unique })
    }

    /// Returns the distinct usable address count.
    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    /// Returns whether this successful result contains no address.
    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }

    /// Iterates in resolver preference order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ResolvedAddress> {
        self.addresses.iter()
    }

    /// Returns one retained address by resolver-order index.
    pub fn get(&self, index: usize) -> Option<ResolvedAddress> {
        self.addresses.get(index).copied()
    }
}
