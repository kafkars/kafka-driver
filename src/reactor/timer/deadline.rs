//! Identity retained while one connection deadline awaits delivery.

use kafka_driver_core::{CallId, ConnectionEpoch, Moment, TimerId};

/// One scheduled connection deadline and the identities its event must echo.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct DeadlineTimer {
    timer_id: TimerId,
    epoch: ConnectionEpoch,
    call_id: CallId,
    at: Moment,
}

impl DeadlineTimer {
    pub(in crate::reactor) const fn new(
        timer_id: TimerId,
        epoch: ConnectionEpoch,
        call_id: CallId,
        at: Moment,
    ) -> Self {
        Self {
            timer_id,
            epoch,
            call_id,
            at,
        }
    }

    pub(in crate::reactor) const fn timer_id(self) -> TimerId {
        self.timer_id
    }

    pub(in crate::reactor) const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    pub(in crate::reactor) const fn call_id(self) -> CallId {
        self.call_id
    }

    pub(in crate::reactor) const fn at(self) -> Moment {
        self.at
    }
}
