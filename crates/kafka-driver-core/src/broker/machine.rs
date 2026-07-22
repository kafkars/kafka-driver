//! Deterministic broker dispatcher and reconnect-state transitions.

use crate::{ConnectionEpoch, Moment, TimerId};

use super::{
    BackoffPolicy, BrokerCloseReason, BrokerEffect, BrokerInput, BrokerState, BrokerTransition,
    ReconnectSchedule, RetryOrdinal,
};

/// Long-lived policy owner above one replaceable connection child at a time.
#[must_use]
#[derive(Debug)]
pub struct BrokerMachine {
    state: BrokerState,
    backoff: BackoffPolicy,
}

impl BrokerMachine {
    /// Creates a dormant broker with its first connection generation reserved.
    pub const fn new(initial_epoch: ConnectionEpoch, backoff: BackoffPolicy) -> Self {
        Self {
            state: BrokerState::Dormant { initial_epoch },
            backoff,
        }
    }

    /// Applies one command or connection-child outcome.
    #[must_use = "broker effects must be interpreted in order"]
    pub fn apply(&mut self, input: BrokerInput) -> BrokerTransition {
        match input {
            BrokerInput::Start => self.start(),
            BrokerInput::ConnectionReady { epoch } => self.connection_ready(epoch),
            BrokerInput::ConnectionFailed { epoch, reconnect } => {
                self.connection_failed(epoch, reconnect)
            }
            BrokerInput::ConnectionRejected { epoch, failure } => {
                self.connection_rejected(epoch, failure)
            }
            BrokerInput::ConnectionDrained { epoch } => self.connection_drained(epoch),
            BrokerInput::ReconnectElapsed {
                failed_epoch,
                timer_id,
                now,
            } => self.reconnect_elapsed(failed_epoch, timer_id, now),
            BrokerInput::BeginDrain => self.begin_drain(),
        }
    }

    /// Returns the current immutable broker lifecycle state.
    pub const fn state(&self) -> BrokerState {
        self.state
    }

    fn start(&mut self) -> BrokerTransition {
        let BrokerState::Dormant { initial_epoch } = self.state else {
            return BrokerTransition::ignored();
        };
        self.state = BrokerState::Connecting {
            epoch: initial_epoch,
            retry: None,
        };
        BrokerTransition::applied(vec![BrokerEffect::OpenConnection {
            epoch: initial_epoch,
        }])
    }

    fn connection_ready(&mut self, epoch: ConnectionEpoch) -> BrokerTransition {
        let BrokerState::Connecting {
            epoch: expected, ..
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if epoch != expected {
            return BrokerTransition::stale();
        }
        self.state = BrokerState::Available { epoch };
        BrokerTransition::applied(Vec::new())
    }

    fn connection_failed(
        &mut self,
        epoch: ConnectionEpoch,
        reconnect: ReconnectSchedule,
    ) -> BrokerTransition {
        let retry = match self.state {
            BrokerState::Connecting {
                epoch: expected,
                retry,
            } if epoch == expected => match retry {
                Some(current) => current.next(),
                None => Some(RetryOrdinal::first()),
            },
            BrokerState::Available { epoch: expected } if epoch == expected => {
                Some(RetryOrdinal::first())
            }
            _ => return BrokerTransition::stale(),
        };
        let Some(retry) = retry else {
            return self.close(BrokerCloseReason::RetryExhausted);
        };
        let Some(next_epoch) = epoch.get().checked_add(1).map(ConnectionEpoch::from_raw) else {
            return self.close(BrokerCloseReason::EpochExhausted);
        };
        let delay = self.backoff.delay(retry, reconnect.jitter);
        let Some(deadline) = reconnect.now.checked_add(delay) else {
            return self.close(BrokerCloseReason::ClockOverflow);
        };
        self.state = BrokerState::Backoff {
            failed_epoch: epoch,
            next_epoch,
            retry,
            timer_id: reconnect.timer_id,
            deadline,
        };
        BrokerTransition::applied(vec![BrokerEffect::ScheduleReconnect {
            failed_epoch: epoch,
            timer_id: reconnect.timer_id,
            at: deadline,
        }])
    }

    fn connection_rejected(
        &mut self,
        epoch: ConnectionEpoch,
        failure: crate::AuthenticationFailure,
    ) -> BrokerTransition {
        let BrokerState::Connecting {
            epoch: expected, ..
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if epoch != expected {
            return BrokerTransition::stale();
        }
        self.close(BrokerCloseReason::AuthenticationFailed(failure))
    }

    fn reconnect_elapsed(
        &mut self,
        failed_epoch: ConnectionEpoch,
        timer_id: TimerId,
        now: Moment,
    ) -> BrokerTransition {
        let BrokerState::Backoff {
            failed_epoch: expected_epoch,
            next_epoch,
            retry,
            timer_id: expected_timer,
            deadline,
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if failed_epoch != expected_epoch || timer_id != expected_timer {
            return BrokerTransition::stale();
        }
        if now < deadline {
            return BrokerTransition::applied(vec![BrokerEffect::ScheduleReconnect {
                failed_epoch,
                timer_id,
                at: deadline,
            }]);
        }
        self.state = BrokerState::Connecting {
            epoch: next_epoch,
            retry: Some(retry),
        };
        BrokerTransition::applied(vec![BrokerEffect::OpenConnection { epoch: next_epoch }])
    }

    fn begin_drain(&mut self) -> BrokerTransition {
        match self.state {
            BrokerState::Dormant { .. } => self.close(BrokerCloseReason::Requested),
            BrokerState::Connecting { epoch, .. } | BrokerState::Available { epoch } => {
                self.state = BrokerState::Draining { epoch };
                BrokerTransition::applied(vec![BrokerEffect::DrainConnection { epoch }])
            }
            BrokerState::Backoff { timer_id, .. } => {
                self.state = BrokerState::Closed {
                    reason: BrokerCloseReason::Requested,
                };
                BrokerTransition::applied(vec![BrokerEffect::CancelReconnect { timer_id }])
            }
            BrokerState::Draining { .. } | BrokerState::Closed { .. } => {
                BrokerTransition::ignored()
            }
        }
    }

    fn connection_drained(&mut self, epoch: ConnectionEpoch) -> BrokerTransition {
        let BrokerState::Draining { epoch: expected } = self.state else {
            return BrokerTransition::stale();
        };
        if epoch != expected {
            return BrokerTransition::stale();
        }
        self.close(BrokerCloseReason::Requested)
    }

    fn close(&mut self, reason: BrokerCloseReason) -> BrokerTransition {
        self.state = BrokerState::Closed { reason };
        BrokerTransition::applied(Vec::new())
    }
}
