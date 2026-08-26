//! Monotonic identity ownership for logical endpoints and physical Kafka lanes.

use std::{error::Error, fmt};

use bornera_core::{ConnectionId, EndpointId, LaneId};
use calandria::TimerOwnerId;

/// Stable Bornera identity domains owned by one connection-local lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct BorneraLaneOwner {
    endpoint: EndpointId,
    lane: LaneId,
    connection: ConnectionId,
    timer: TimerOwnerId,
}

impl BorneraLaneOwner {
    const fn allocated(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        timer: TimerOwnerId,
    ) -> Self {
        Self {
            endpoint,
            lane,
            connection,
            timer,
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn new(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        timer: TimerOwnerId,
    ) -> Self {
        Self::allocated(endpoint, lane, connection, timer)
    }

    pub(in crate::reactor) const fn endpoint(self) -> EndpointId {
        self.endpoint
    }

    pub(in crate::reactor) const fn lane(self) -> LaneId {
        self.lane
    }

    pub(in crate::reactor) const fn connection(self) -> ConnectionId {
        self.connection
    }

    pub(in crate::reactor) const fn timer(self) -> TimerOwnerId {
        self.timer
    }
}

/// Non-reusing identities for endpoint and lane incarnations in one set.
///
/// The narrower lane domain governs every lane-specific identity so an issued
/// tuple can never be partially reused.
pub(in crate::reactor) struct BorneraIdentityAllocator {
    next_endpoint: Option<u64>,
    next_lane: Option<u32>,
}

impl BorneraIdentityAllocator {
    pub(in crate::reactor) const fn new() -> Self {
        Self {
            next_endpoint: Some(1),
            next_lane: Some(1),
        }
    }

    pub(in crate::reactor) fn reserve_endpoint_lanes<const N: usize>(
        &mut self,
    ) -> Result<(EndpointId, [BorneraLaneOwner; N]), BorneraIdentityError> {
        let endpoint_value = self
            .next_endpoint
            .ok_or(BorneraIdentityError::EndpointExhausted)?;
        let lane_value = self.next_lane.ok_or(BorneraIdentityError::LaneExhausted)?;
        let count = u32::try_from(N).map_err(|_| BorneraIdentityError::LaneExhausted)?;
        let last_offset = count
            .checked_sub(1)
            .ok_or(BorneraIdentityError::EmptyLaneGroup)?;
        let last_lane = lane_value
            .checked_add(last_offset)
            .ok_or(BorneraIdentityError::LaneExhausted)?;
        let endpoint = EndpointId::new(endpoint_value);
        let mut value = lane_value;
        let owners = std::array::from_fn(|_| {
            let owner = u64::from(value);
            let lane = BorneraLaneOwner::allocated(
                endpoint,
                LaneId::new(value),
                ConnectionId::new(owner),
                TimerOwnerId::new(owner),
            );
            value = value.saturating_add(1);
            lane
        });
        self.next_endpoint = endpoint_value.checked_add(1);
        self.next_lane = last_lane.checked_add(1);
        Ok((endpoint, owners))
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn at(next_endpoint: Option<u64>, next_lane: Option<u32>) -> Self {
        Self {
            next_endpoint,
            next_lane,
        }
    }
}

/// Explicit exhaustion of a stable Bornera identity domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum BorneraIdentityError {
    EmptyLaneGroup,
    EndpointExhausted,
    LaneExhausted,
}

impl fmt::Display for BorneraIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLaneGroup => formatter.write_str("Bornera lane group must be nonempty"),
            Self::EndpointExhausted => formatter.write_str("Bornera endpoint identities exhausted"),
            Self::LaneExhausted => formatter.write_str("Bornera lane identities exhausted"),
        }
    }
}

impl Error for BorneraIdentityError {}
