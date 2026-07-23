//! Retry generation ownership with and without fresh endpoint evidence.

use crate::ConnectionEpoch;

use super::{
    AddressRefreshState, BrokerCloseReason, BrokerEffect, BrokerMachine, BrokerState,
    BrokerTransition, ReconnectSchedule, RetryOrdinal,
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
            refresh: AddressRefreshState::Pending { last_retry: None },
        };
        BrokerTransition::applied(Vec::new())
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
