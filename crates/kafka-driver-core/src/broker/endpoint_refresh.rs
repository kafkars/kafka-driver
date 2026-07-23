//! Endpoint-resolution attempt, backoff, and terminal-failure policy.

use crate::{ConnectionEpoch, DnsFailure, Moment, TimerId};

use super::{
    AddressRefreshState, BrokerCloseReason, BrokerEffect, BrokerMachine, BrokerState,
    BrokerTransition, EndpointRefreshSchedule, RetryOrdinal,
};

impl BrokerMachine {
    pub(super) fn endpoint_refresh_started(
        &mut self,
        failed_epoch: ConnectionEpoch,
    ) -> BrokerTransition {
        let BrokerState::Refreshing {
            failed_epoch: expected,
            next_epoch,
            retry,
            timer_id,
            deadline,
            refresh: AddressRefreshState::Pending { last_retry },
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if failed_epoch != expected {
            return BrokerTransition::stale();
        }
        self.state = BrokerState::Refreshing {
            failed_epoch,
            next_epoch,
            retry,
            timer_id,
            deadline,
            refresh: AddressRefreshState::Resolving { last_retry },
        };
        BrokerTransition::applied(Vec::new())
    }

    pub(super) fn endpoint_refresh_deferred(
        &mut self,
        failed_epoch: ConnectionEpoch,
    ) -> BrokerTransition {
        let BrokerState::Refreshing {
            failed_epoch: expected,
            next_epoch,
            retry,
            timer_id,
            deadline,
            refresh: AddressRefreshState::Resolving { last_retry },
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if failed_epoch != expected {
            return BrokerTransition::stale();
        }
        self.state = BrokerState::Refreshing {
            failed_epoch,
            next_epoch,
            retry,
            timer_id,
            deadline,
            refresh: AddressRefreshState::Pending { last_retry },
        };
        BrokerTransition::applied(Vec::new())
    }

    pub(super) fn endpoint_refresh_failed(
        &mut self,
        failed_epoch: ConnectionEpoch,
        failure: DnsFailure,
        schedule: EndpointRefreshSchedule,
    ) -> BrokerTransition {
        let BrokerState::Refreshing {
            failed_epoch: expected,
            next_epoch,
            retry: reconnect_retry,
            timer_id: reconnect_timer,
            deadline: reconnect_deadline,
            refresh: AddressRefreshState::Resolving { last_retry },
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if failed_epoch != expected {
            return BrokerTransition::stale();
        }
        if failure == DnsFailure::NoUsableAddress {
            return self.close(BrokerCloseReason::EndpointResolutionFailed(failure));
        }
        let Some(retry) =
            last_retry.map_or_else(|| Some(RetryOrdinal::first()), RetryOrdinal::next)
        else {
            return self.close(BrokerCloseReason::RetryExhausted);
        };
        let delay = self.backoff.delay(retry, schedule.jitter);
        let Some(deadline) = schedule.now.checked_add(delay) else {
            return self.close(BrokerCloseReason::ClockOverflow);
        };
        self.state = BrokerState::Refreshing {
            failed_epoch,
            next_epoch,
            retry: reconnect_retry,
            timer_id: reconnect_timer,
            deadline: reconnect_deadline,
            refresh: AddressRefreshState::Backoff {
                retry,
                timer_id: schedule.timer_id,
                deadline,
            },
        };
        BrokerTransition::applied(vec![BrokerEffect::ScheduleEndpointRefreshRetry {
            failed_epoch,
            timer_id: schedule.timer_id,
            at: deadline,
        }])
    }

    pub(super) fn endpoint_refresh_retry_elapsed(
        &mut self,
        failed_epoch: ConnectionEpoch,
        timer_id: TimerId,
        now: Moment,
    ) -> BrokerTransition {
        let BrokerState::Refreshing {
            failed_epoch: expected_epoch,
            next_epoch,
            retry: reconnect_retry,
            timer_id: reconnect_timer,
            deadline: reconnect_deadline,
            refresh:
                AddressRefreshState::Backoff {
                    retry,
                    timer_id: expected_timer,
                    deadline,
                },
        } = self.state
        else {
            return BrokerTransition::stale();
        };
        if failed_epoch != expected_epoch || timer_id != expected_timer {
            return BrokerTransition::stale();
        }
        if now < deadline {
            return BrokerTransition::applied(vec![BrokerEffect::ScheduleEndpointRefreshRetry {
                failed_epoch,
                timer_id,
                at: deadline,
            }]);
        }
        self.state = BrokerState::Refreshing {
            failed_epoch,
            next_epoch,
            retry: reconnect_retry,
            timer_id: reconnect_timer,
            deadline: reconnect_deadline,
            refresh: AddressRefreshState::Pending {
                last_retry: Some(retry),
            },
        };
        BrokerTransition::applied(Vec::new())
    }

    pub(super) fn endpoint_refreshed(
        &mut self,
        failed_epoch: ConnectionEpoch,
        now: Moment,
    ) -> BrokerTransition {
        let BrokerState::Refreshing {
            failed_epoch: expected,
            next_epoch,
            retry,
            timer_id,
            deadline,
            refresh: AddressRefreshState::Resolving { .. },
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
}
