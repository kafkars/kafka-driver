//! Broker effects interpreted over one persistent Bornera connection set.

use std::{collections::VecDeque, io};

use bornera::RegisteredTransport;
use kafka_driver_core::{
    BrokerCloseReason, BrokerEffect, BrokerState, CallFailure, CloseReason, ConnectionEpoch,
    Delivery, KafkaSessionInput, Moment,
};

use crate::RequestError;

use crate::reactor::causality::CausalSequence;

use super::{
    failure_translation::{not_sent, recovery},
    owner::DirectLaneAccess,
};

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn settle_generation_lifecycle(
        &mut self,
        epoch: ConnectionEpoch,
        reason: CloseReason,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        let result = self
            .end_generation(epoch, reason, now)
            .and_then(|effects| self.interpret_lifecycle_effects(effects, now, Some(causality)))
            .and_then(|()| self.settle_policy_close(reason, Some(causality)));
        result.map_err(|error| self.host_fatal(error))
    }

    pub(super) fn fire_due_reconnect(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let effects = self
            .lifecycle
            .fire_due(now)
            .map_err(|error| self.host_fatal(error))?;
        if effects.is_empty() {
            return Ok(false);
        }
        self.interpret_lifecycle_effects(effects, now, Some(causality))
            .map_err(|error| self.host_fatal(error))?;
        Ok(true)
    }

    pub(super) fn begin_lifecycle_drain(&mut self, now: Moment) -> io::Result<()> {
        let effects = self
            .lifecycle
            .begin_drain()
            .map_err(|error| self.host_fatal(error))?;
        self.interpret_lifecycle_effects(effects, now, None)
            .map_err(|error| self.host_fatal(error))?;
        if self.lifecycle.is_closed() {
            self.endpoint_refresh = None;
            self.terminal = true;
            self.mark_waiting();
        }
        Ok(())
    }

    fn interpret_lifecycle_effects(
        &mut self,
        effects: Vec<BrokerEffect>,
        now: Moment,
        mut causality: Option<&mut CausalSequence>,
    ) -> io::Result<()> {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            match effect {
                BrokerEffect::OpenConnection { epoch } => match self.open_generation(epoch, now) {
                    Ok(None) => {}
                    Ok(Some(reason)) => {
                        let follow = self
                            .end_generation(epoch, reason, now)
                            .map_err(|error| self.host_fatal(error))?;
                        effects.extend(follow);
                        self.settle_policy_close(reason, causality.as_deref_mut())?;
                    }
                    Err(error) => return Err(self.host_fatal(error)),
                },
                BrokerEffect::ScheduleReconnect {
                    failed_epoch,
                    timer_id,
                    at,
                } => {
                    let expected = matches!(
                        self.lifecycle.state(),
                        BrokerState::Backoff {
                            failed_epoch: current_epoch,
                            timer_id: current_timer,
                            deadline,
                            ..
                        } if current_epoch == failed_epoch
                            && current_timer == timer_id
                            && deadline == at
                    );
                    if !expected {
                        return Err(io::Error::other(
                            "direct reconnect effect diverged from broker state",
                        ));
                    }
                    self.mark_waiting();
                }
                BrokerEffect::CancelReconnect { .. } => {
                    self.mark_waiting();
                }
                BrokerEffect::DrainConnection { epoch } => {
                    if epoch != core_epoch(self.live_connection()?.epoch()) {
                        return Err(io::Error::other(
                            "direct drain named a stale connection generation",
                        ));
                    }
                    self.apply_session(KafkaSessionInput::BeginDrain, now)?;
                }
                BrokerEffect::ScheduleEndpointRefreshRetry { .. }
                | BrokerEffect::CancelEndpointRefreshRetry { .. } => {
                    return Err(io::Error::other(
                        "fixed direct lifecycle emitted an endpoint-refresh effect",
                    ));
                }
            }
        }
        Ok(())
    }

    fn settle_policy_close(
        &mut self,
        preceding: CloseReason,
        causality: Option<&mut CausalSequence>,
    ) -> io::Result<()> {
        let BrokerState::Closed { reason } = self.lifecycle.state() else {
            return Ok(());
        };
        self.terminal = true;
        self.mark_waiting();
        let failure = terminal_failure(reason, preceding);
        self.fail_pending(&failure, causality)
    }

    pub(super) fn host_fatal(&mut self, error: io::Error) -> io::Error {
        let reason = self
            .generation_close_reason
            .or(self.last_close_reason)
            .unwrap_or(CloseReason::TransportLost(
                kafka_driver_core::TransportFailure::Other,
            ));
        self.admission_open = false;
        self.clear_authentication_ownership();
        self.session_deadline = None;
        let _ = self.fail_remaining(&recovery(reason, Delivery::PossiblySent), None, Some(()));
        let _ = self.fail_pending(&recovery(reason, Delivery::NotSent), None);
        self.terminal = true;
        self.mark_waiting();
        error
    }
}

pub(super) const fn core_epoch(epoch: bornera_core::ConnectionEpoch) -> ConnectionEpoch {
    ConnectionEpoch::from_raw(epoch.get())
}

pub(super) const fn bornera_epoch(epoch: ConnectionEpoch) -> bornera_core::ConnectionEpoch {
    bornera_core::ConnectionEpoch::new(epoch.get())
}

pub(super) fn terminal_failure(reason: BrokerCloseReason, preceding: CloseReason) -> RequestError {
    match reason {
        BrokerCloseReason::AuthenticationFailed(_) => recovery(preceding, Delivery::NotSent),
        BrokerCloseReason::Requested => not_sent(CallFailure::Draining),
        BrokerCloseReason::EpochExhausted
        | BrokerCloseReason::RetryExhausted
        | BrokerCloseReason::RetryResourcesUnavailable
        | BrokerCloseReason::ClockOverflow
        | BrokerCloseReason::EndpointResolutionFailed(_) => not_sent(CallFailure::Closed),
    }
}
