//! Identity retained while one connection deadline awaits delivery.

use kafka_driver_core::{CallId, ConnectionEpoch, Moment, TimerId};

/// Machine work whose deadline is represented by one timer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum DeadlineSubject {
    /// One public RPC call.
    Call(CallId),
    /// Initial API version negotiation.
    Negotiation,
    /// Delay before creating a fresh connection generation.
    Reconnect,
}

/// One scheduled connection deadline and the identities its event must echo.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct DeadlineTimer {
    timer_id: TimerId,
    epoch: ConnectionEpoch,
    subject: DeadlineSubject,
    at: Moment,
}

impl DeadlineTimer {
    pub(in crate::reactor) const fn for_call(
        timer_id: TimerId,
        epoch: ConnectionEpoch,
        call_id: CallId,
        at: Moment,
    ) -> Self {
        Self {
            timer_id,
            epoch,
            subject: DeadlineSubject::Call(call_id),
            at,
        }
    }

    pub(in crate::reactor) const fn for_negotiation(
        timer_id: TimerId,
        epoch: ConnectionEpoch,
        at: Moment,
    ) -> Self {
        Self {
            timer_id,
            epoch,
            subject: DeadlineSubject::Negotiation,
            at,
        }
    }

    pub(in crate::reactor) const fn for_reconnect(
        timer_id: TimerId,
        failed_epoch: ConnectionEpoch,
        at: Moment,
    ) -> Self {
        Self {
            timer_id,
            epoch: failed_epoch,
            subject: DeadlineSubject::Reconnect,
            at,
        }
    }

    pub(in crate::reactor) const fn timer_id(self) -> TimerId {
        self.timer_id
    }

    pub(in crate::reactor) const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    pub(in crate::reactor) const fn subject(self) -> DeadlineSubject {
        self.subject
    }

    pub(in crate::reactor) const fn at(self) -> Moment {
        self.at
    }
}
