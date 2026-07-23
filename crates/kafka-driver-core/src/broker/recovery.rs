//! Retry generation ownership with and without fresh endpoint evidence.

use crate::ConnectionEpoch;

use super::{
    BrokerCloseReason, BrokerEffect, BrokerMachine, BrokerState, BrokerTransition,
    ReconnectSchedule, RetryOrdinal,
};

impl BrokerMachine {
    pub(super) fn connection_failed(
        &mut self,
        epoch: ConnectionEpoch,
        reconnect: ReconnectSchedule,
    ) -> BrokerTransition {
        let recovery = match self.recovery_after(epoch, reconnect) {
            Ok(recovery) => recovery,
            Err(transition) => return transition,
        };
        self.state = BrokerState::Backoff {
            failed_epoch: epoch,
            next_epoch: recovery.next_epoch,
            retry: recovery.retry,
            timer_id: recovery.timer_id,
            deadline: recovery.deadline,
        };
        BrokerTransition::applied(vec![BrokerEffect::ScheduleReconnect {
            failed_epoch: epoch,
            timer_id: recovery.timer_id,
            at: recovery.deadline,
        }])
    }

    pub(super) fn endpoint_exhausted(
        &mut self,
        epoch: ConnectionEpoch,
        reconnect: ReconnectSchedule,
    ) -> BrokerTransition {
        let recovery = match self.recovery_after(epoch, reconnect) {
            Ok(recovery) => recovery,
            Err(transition) => return transition,
        };
        self.state = BrokerState::Refreshing {
            failed_epoch: epoch,
            next_epoch: recovery.next_epoch,
            retry: recovery.retry,
            timer_id: recovery.timer_id,
            deadline: recovery.deadline,
        };
        BrokerTransition::applied(Vec::new())
    }

    pub(super) fn endpoint_refreshed(
        &mut self,
        failed_epoch: ConnectionEpoch,
        now: crate::Moment,
    ) -> BrokerTransition {
        let BrokerState::Refreshing {
            failed_epoch: expected,
            next_epoch,
            retry,
            timer_id,
            deadline,
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if failed_epoch != expected {
            return BrokerTransition::stale();
        }
        if now < deadline {
            self.state = BrokerState::Backoff {
                failed_epoch,
                next_epoch,
                retry,
                timer_id,
                deadline,
            };
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

    fn recovery_after(
        &mut self,
        epoch: ConnectionEpoch,
        reconnect: ReconnectSchedule,
    ) -> Result<RecoveryGeneration, BrokerTransition> {
        let retry = match self.state {
            BrokerState::Connecting {
                epoch: expected,
                retry,
            } if epoch == expected => {
                retry.map_or_else(|| Some(RetryOrdinal::first()), RetryOrdinal::next)
            }
            BrokerState::Available { epoch: expected } if epoch == expected => {
                Some(RetryOrdinal::first())
            }
            _ => return Err(BrokerTransition::stale()),
        };
        let Some(retry) = retry else {
            return Err(self.close(BrokerCloseReason::RetryExhausted));
        };
        let Some(next_epoch) = epoch.get().checked_add(1).map(ConnectionEpoch::from_raw) else {
            return Err(self.close(BrokerCloseReason::EpochExhausted));
        };
        let delay = self.backoff.delay(retry, reconnect.jitter);
        let Some(deadline) = reconnect.now.checked_add(delay) else {
            return Err(self.close(BrokerCloseReason::ClockOverflow));
        };
        Ok(RecoveryGeneration {
            next_epoch,
            retry,
            timer_id: reconnect.timer_id,
            deadline,
        })
    }
}

struct RecoveryGeneration {
    next_epoch: ConnectionEpoch,
    retry: RetryOrdinal,
    timer_id: crate::TimerId,
    deadline: crate::Moment,
}
