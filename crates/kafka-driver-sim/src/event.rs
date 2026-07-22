//! Identity and delivery envelope for one scripted simulation event.

use kafka_driver_core::Moment;

/// Stable insertion identity for a scripted event.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimEventId(u64);

impl SimEventId {
    pub(crate) const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the simulator-local numeric identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One scripted event selected for deterministic delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scheduled<E> {
    id: SimEventId,
    at: Moment,
    event: E,
}

impl<E> Scheduled<E> {
    pub(crate) const fn new(id: SimEventId, at: Moment, event: E) -> Self {
        Self { id, at, event }
    }

    /// Returns the stable insertion identity.
    pub const fn id(&self) -> SimEventId {
        self.id
    }

    /// Returns the virtual delivery time.
    pub const fn at(&self) -> Moment {
        self.at
    }

    /// Borrows the scripted event value.
    pub const fn event(&self) -> &E {
        &self.event
    }

    /// Consumes the envelope and returns the scripted event value.
    pub fn into_event(self) -> E {
        self.event
    }
}
