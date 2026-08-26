//! Sole Bornera selector ownership shared by bounded Kafka lanes.

use std::{io, net::SocketAddr};

use bornera::OwnerFailure;
use bornera::{ConnectionSet, ConnectionSetConfig, ConnectionToken, RegisteredTransport};
use bornera_core::ConnectionEpoch;
use calandria::{ResourceOwnerId, Span, Turn, WaitOutcome};
use kafka_driver_core::Moment;

use crate::config::DriverLimits;

use super::{
    attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
    decoder_gate::DirectFrameDecoder,
    drive::DirectDrivePreparation,
    limits::{DirectSetBounds, set_limits},
    owner::{
        DirectLane, DirectLaneAccess, DirectLaneView, SET_OWNER_ID, calandria_moment, message,
    },
};
use crate::reactor::bornera::KafkaReplyClassifier;

pub(super) type DirectSet<T> = ConnectionSet<DirectFrameDecoder, KafkaReplyClassifier, T>;

pub(super) struct DirectSetOwner<T: RegisteredTransport> {
    #[cfg(not(test))]
    set: DirectSet<T>,
    #[cfg(test)]
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
        ConnectionSet::wake_handle(&self.set)
    }

    pub(super) fn pulse_handle(&self) -> bornera::ConnectionPulseHandle {
        ConnectionSet::pulse_handle(&self.set)
    }

    pub(super) fn drive_selector(&mut self, now: Moment) -> io::Result<Turn> {
        ConnectionSet::turn_component(&mut self.set, calandria_moment(now)).map_err(message)
    }

    pub(super) fn wait_selector(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        ConnectionSet::poll_io(&mut self.set, maximum).map_err(message)
    }

    #[cfg(test)]
    pub(super) const fn turns_for_test(&self) -> u64 {
        self.turns
    }
}
