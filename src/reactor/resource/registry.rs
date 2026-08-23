//! Driver error vocabulary over Calandria's bounded resource table.

use std::num::NonZeroUsize;

#[cfg(test)]
use calandria::ResourceGeneration;
use calandria::ResourceTable;
use kafka_driver_core::TransportId;

use super::{
    ResourceAdmissionError, ResourceAdmissionFailure, ResourceIdentity, ResourceNamespace,
    ResourceToken,
};

/// Single-owner registry for resources associated with poll readiness tokens.
#[derive(Debug)]
pub(in crate::reactor) struct ResourceRegistry<R> {
    table: ResourceTable<ResourceIdentity, R>,
    transport_ids: Vec<TransportId>,
}

impl<R> ResourceRegistry<R> {
    #[cfg(test)]
    pub(in crate::reactor) fn new(capacity: NonZeroUsize) -> Self {
        Self::in_namespace(capacity, ResourceNamespace::single())
    }

    pub(in crate::reactor) fn in_namespace(
        capacity: NonZeroUsize,
        namespace: ResourceNamespace,
    ) -> Self {
        Self {
            table: ResourceTable::new(namespace.owner(), capacity),
            transport_ids: Vec::with_capacity(capacity.get()),
        }
    }

    pub(in crate::reactor) fn admit(
        &mut self,
        identity: ResourceIdentity,
        resource: R,
    ) -> Result<ResourceToken, ResourceAdmissionError<R>> {
        if self.transport_ids.contains(&identity.transport_id()) {
            return Err(ResourceAdmissionError::new(
                ResourceAdmissionFailure::IdentityInUse {
                    transport_id: identity.transport_id(),
                },
                resource,
            ));
        }
        let token = self.table.admit(identity, resource).map_err(|error| {
            let (identity, resource, failure) = error.into_parts();
            let failure = match failure {
                calandria::ResourceAdmissionFailure::IdentityInUse => {
                    ResourceAdmissionFailure::IdentityInUse {
                        transport_id: identity.transport_id(),
                    }
                }
                calandria::ResourceAdmissionFailure::CapacityReached { limit } => {
                    ResourceAdmissionFailure::CapacityReached { limit: limit.get() }
                }
                calandria::ResourceAdmissionFailure::TokenSpaceExhausted => {
                    ResourceAdmissionFailure::TokenSpaceExhausted
                }
            };
            ResourceAdmissionError::new(failure, resource)
        })?;
        self.transport_ids.push(identity.transport_id());
        Ok(token)
    }

    pub(in crate::reactor) fn get_mut(
        &mut self,
        token: ResourceToken,
    ) -> Option<(ResourceIdentity, &mut R)> {
        self.table
            .get_mut(token)
            .ok()
            .map(|(identity, resource)| (*identity, resource))
    }

    pub(in crate::reactor) fn get(&self, token: ResourceToken) -> Option<(ResourceIdentity, &R)> {
        self.table
            .get(token)
            .ok()
            .map(|(identity, resource)| (*identity, resource))
    }

    pub(in crate::reactor) fn token_for(
        &self,
        identity: ResourceIdentity,
    ) -> Option<ResourceToken> {
        self.table.token_for(&identity)
    }

    pub(in crate::reactor) fn remove(
        &mut self,
        token: ResourceToken,
    ) -> Option<(ResourceIdentity, R)> {
        let (identity, resource) = self.table.remove(token).ok()?;
        if let Some(position) = self
            .transport_ids
            .iter()
            .position(|transport_id| *transport_id == identity.transport_id())
        {
            self.transport_ids.swap_remove(position);
        }
        Some((identity, resource))
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn len(&self) -> usize {
        self.table.len()
    }

    #[cfg(test)]
    pub(super) fn with_generation(capacity: NonZeroUsize, generation: u64) -> Self {
        Self {
            table: ResourceTable::starting_at(
                ResourceNamespace::single().owner(),
                capacity,
                ResourceGeneration::new(generation),
            ),
            transport_ids: Vec::with_capacity(capacity.get()),
        }
    }
}
