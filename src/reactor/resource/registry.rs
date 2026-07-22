//! Bounded slots with constant-time stale-token rejection.

use std::{mem, num::NonZeroUsize};

use kafka_driver_core::TransportId;

use super::{
    ResourceAdmissionError, ResourceAdmissionFailure, ResourceIdentity, ResourceNamespace,
    ResourceToken,
};

/// Single-owner registry for resources associated with poll readiness tokens.
#[derive(Debug)]
pub(in crate::reactor) struct ResourceRegistry<R> {
    slots: Vec<ResourceSlot<R>>,
    active: usize,
    namespace: ResourceNamespace,
}

impl<R> ResourceRegistry<R> {
    pub(in crate::reactor) fn new(capacity: NonZeroUsize) -> Self {
        Self::in_namespace(capacity, ResourceNamespace::single())
    }

    pub(in crate::reactor) fn in_namespace(
        capacity: NonZeroUsize,
        namespace: ResourceNamespace,
    ) -> Self {
        Self {
            slots: (0..capacity.get())
                .map(|_| ResourceSlot::Vacant { generation: 0 })
                .collect(),
            active: 0,
            namespace,
        }
    }

    pub(in crate::reactor) fn admit(
        &mut self,
        identity: ResourceIdentity,
        resource: R,
    ) -> Result<ResourceToken, ResourceAdmissionError<R>> {
        if self.contains(identity.transport_id()) {
            return Err(ResourceAdmissionError::new(
                ResourceAdmissionFailure::IdentityInUse {
                    transport_id: identity.transport_id(),
                },
                resource,
            ));
        }
        if self.active == self.slots.len() {
            return Err(ResourceAdmissionError::new(
                ResourceAdmissionFailure::CapacityReached {
                    limit: self.slots.len(),
                },
                resource,
            ));
        }
        let Some((slot_index, generation, token)) = self.next_vacant() else {
            return Err(ResourceAdmissionError::new(
                ResourceAdmissionFailure::TokenSpaceExhausted,
                resource,
            ));
        };

        self.slots[slot_index] = ResourceSlot::Occupied {
            generation,
            identity,
            resource,
        };
        self.active += 1;
        Ok(token)
    }

    pub(in crate::reactor) fn get_mut(
        &mut self,
        token: ResourceToken,
    ) -> Option<(ResourceIdentity, &mut R)> {
        let (slot_index, generation) = token.decode_in(self.slots.len(), self.namespace)?;
        let ResourceSlot::Occupied {
            generation: current,
            identity,
            resource,
        } = self.slots.get_mut(slot_index)?
        else {
            return None;
        };
        (*current == generation).then_some((*identity, resource))
    }

    pub(in crate::reactor) fn token_for(
        &self,
        identity: ResourceIdentity,
    ) -> Option<ResourceToken> {
        self.slots
            .iter()
            .enumerate()
            .find_map(|(slot_index, slot)| match slot {
                ResourceSlot::Occupied {
                    generation,
                    identity: current,
                    ..
                } if *current == identity => ResourceToken::encode_in(
                    self.slots.len(),
                    self.namespace,
                    slot_index,
                    *generation,
                ),
                ResourceSlot::Vacant { .. }
                | ResourceSlot::Occupied { .. }
                | ResourceSlot::Exhausted => None,
            })
    }

    pub(in crate::reactor) fn remove(
        &mut self,
        token: ResourceToken,
    ) -> Option<(ResourceIdentity, R)> {
        let capacity = self.slots.len();
        let (slot_index, generation) = token.decode_in(capacity, self.namespace)?;
        let slot = self.slots.get_mut(slot_index)?;
        let current = mem::replace(slot, ResourceSlot::Exhausted);
        let ResourceSlot::Occupied {
            generation: current_generation,
            identity,
            resource,
        } = current
        else {
            *slot = current;
            return None;
        };
        if current_generation != generation {
            *slot = ResourceSlot::Occupied {
                generation: current_generation,
                identity,
                resource,
            };
            return None;
        }

        *slot = next_slot(capacity, self.namespace, slot_index, current_generation);
        self.active -= 1;
        Some((identity, resource))
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn len(&self) -> usize {
        self.active
    }

    fn contains(&self, transport_id: TransportId) -> bool {
        self.slots.iter().any(|slot| {
            matches!(
                slot,
                ResourceSlot::Occupied { identity, .. }
                    if identity.transport_id() == transport_id
            )
        })
    }

    fn next_vacant(&self) -> Option<(usize, usize, ResourceToken)> {
        self.slots
            .iter()
            .enumerate()
            .find_map(|(slot_index, slot)| {
                let ResourceSlot::Vacant { generation } = slot else {
                    return None;
                };
                ResourceToken::encode_in(self.slots.len(), self.namespace, slot_index, *generation)
                    .map(|token| (slot_index, *generation, token))
            })
    }

    #[cfg(test)]
    pub(super) fn with_generation(capacity: NonZeroUsize, generation: usize) -> Self {
        Self {
            slots: (0..capacity.get())
                .map(|_| ResourceSlot::Vacant { generation })
                .collect(),
            active: 0,
            namespace: ResourceNamespace::single(),
        }
    }
}

fn next_slot<R>(
    capacity: usize,
    namespace: ResourceNamespace,
    slot_index: usize,
    generation: usize,
) -> ResourceSlot<R> {
    let Some(next_generation) = generation.checked_add(1) else {
        return ResourceSlot::Exhausted;
    };
    if ResourceToken::encode_in(capacity, namespace, slot_index, next_generation).is_none() {
        return ResourceSlot::Exhausted;
    }
    ResourceSlot::Vacant {
        generation: next_generation,
    }
}

#[derive(Debug)]
enum ResourceSlot<R> {
    Vacant {
        generation: usize,
    },
    Occupied {
        generation: usize,
        identity: ResourceIdentity,
        resource: R,
    },
    Exhausted,
}
