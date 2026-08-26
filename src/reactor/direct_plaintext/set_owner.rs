//! Sole Bornera selector ownership shared by bounded Kafka lanes.

use std::{io, net::SocketAddr};

#[cfg(test)]
use bornera::OwnerFailure;
use bornera::{ConnectionSet, ConnectionSetConfig, ConnectionToken, RegisteredTransport};
use bornera_core::ConnectionEpoch;
use calandria::{ResourceOwnerId, Turn};
use kafka_driver_core::Moment;

use crate::config::DriverLimits;

use super::{
    attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
    drive::DirectDrivePreparation,
    limits::{DirectSetBounds, set_limits},
    owner::{DirectLane, DirectLaneAccess, DirectLaneView, DirectSet, SET_OWNER_ID, message},
};

pub(super) struct DirectSetOwner<T: RegisteredTransport> {
    pub(super) set: DirectSet<T>,
    pub(super) last_turn: Turn,
    pub(super) preparations: Vec<(usize, Option<DirectDrivePreparation>)>,
    pub(super) lane_capacity: usize,
    pub(super) deferred_failure: Option<io::Error>,
    #[cfg(test)]
    pub(super) turns: u64,
}

impl<T: RegisteredTransport> DirectSetOwner<T> {
    pub(super) fn new(driver: &DriverLimits, bounds: DirectSetBounds) -> io::Result<Self> {
        let lane_capacity = bounds.max_connections().get();
        let set = ConnectionSet::new(
            ConnectionSetConfig::new(ResourceOwnerId::new(SET_OWNER_ID)),
            set_limits(driver, bounds),
        )
        .map_err(message)?;
        Ok(Self {
            set,
            last_turn: Turn::waiting(),
            preparations: Vec::new(),
            lane_capacity,
            deferred_failure: None,
            #[cfg(test)]
            turns: 0,
        })
    }

    pub(super) fn access<'a>(&'a mut self, lane: &'a mut DirectLane<T>) -> DirectLaneAccess<'a, T> {
        DirectLaneAccess {
            lane,
            set: &mut self.set,
        }
    }

    pub(super) fn view<'a>(&'a self, lane: &'a DirectLane<T>) -> DirectLaneView<'a, T> {
        DirectLaneView {
            lane,
            set: &self.set,
        }
    }

    pub(super) fn connect_lane(
        &mut self,
        attempt: &dyn DirectConnectionAttempt<T>,
        owner: BorneraLaneOwner,
        address: SocketAddr,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        attempt.connect(&mut self.set, owner, address, epoch, now)
    }

    pub(super) fn ensure_lane_capacity(&self, lanes: usize) -> io::Result<()> {
        if lanes > self.lane_capacity {
            return Err(io::Error::other(
                "direct lane collection exceeds the shared set capacity",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn abandon_unpublished(&mut self, connection: ConnectionToken) -> io::Result<()> {
        let report = self
            .set
            .abandon(connection, OwnerFailure::OwnerInvariant)
            .map_err(message)?;
        if report.ownership_diverged
            || !report.operations.is_empty()
            || !report.unmatched_writes.is_empty()
            || !report.outcomes.is_empty()
        {
            return Err(io::Error::other(
                "unpublished Bornera connection rollback diverged",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> bornera::ConnectionSetSnapshot {
        self.set.snapshot()
    }

    pub(super) fn wake_handle(&self) -> calandria::WakeHandle {
        self.set.wake_handle()
    }

    pub(super) fn pulse_handle(&self) -> bornera::ConnectionPulseHandle {
        self.set.pulse_handle()
    }

    #[cfg(test)]
    pub(super) const fn turns_for_test(&self) -> u64 {
        self.turns
    }
}
