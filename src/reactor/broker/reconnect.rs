//! Interpretation of long-lived broker effects across connection generations.

use kafka_driver_core::{
    AuthenticationFailureDisposition, BrokerDisposition, BrokerEffect, BrokerInput, BrokerPhase,
    ConnectionEpoch, ConnectionInput, ConnectionPhase, ConnectionState, Moment, ReconnectSchedule,
    TimerId,
};

use crate::reactor::{Poller, timer::DeadlineTimer};

use super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(in crate::reactor) fn start(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let transition = self.broker.apply(BrokerInput::Start);
        require_applied(transition.disposition())?;
        self.interpret_broker_effects(poller, transition.into_effects(), now)?;
        self.reconcile_connection(poller, now)
    }

    pub(super) fn mark_connection_ready(
        &mut self,
        epoch: ConnectionEpoch,
    ) -> Result<(), BrokerError> {
        let transition = self.broker.apply(BrokerInput::ConnectionReady { epoch });
        require_applied(transition.disposition())?;
        expect_no_broker_effects(&transition.into_effects())?;
        self.addresses.ready();
        Ok(())
    }

    pub(super) fn deliver_reconnect(
        &mut self,
        poller: &Poller,
        failed_epoch: ConnectionEpoch,
        timer_id: TimerId,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let transition = self.broker.apply(BrokerInput::ReconnectElapsed {
            failed_epoch,
            timer_id,
            now,
        });
        self.interpret_broker_effects(poller, transition.into_effects(), now)
    }

    pub(super) fn reconcile_connection(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        while self.connection.state().phase() == ConnectionPhase::Closed {
            self.observe_closed_state();
            let epoch = self.connection.epoch();
            let broker_phase = self.broker.state().phase();
            if matches!(
                broker_phase,
                BrokerPhase::Connecting | BrokerPhase::Available
            ) && let Some(endpoint) = self.addresses.failed()
            {
                self.address_refresh = Some(endpoint);
            }
            let connection_state = self.connection.state();
            let input = match (broker_phase, connection_state) {
                (
                    BrokerPhase::Connecting,
                    ConnectionState::Closed {
                        reason: kafka_driver_core::CloseReason::AuthenticationFailed(failure),
                        ..
                    },
                ) if failure.disposition() == AuthenticationFailureDisposition::Permanent => {
                    BrokerInput::ConnectionRejected { epoch, failure }
                }
                (BrokerPhase::Connecting | BrokerPhase::Available, _) => {
                    let Some(timer_id) = self.ids.reserve_reconnect_timer() else {
                        return Err(BrokerError::IdentityExhausted);
                    };
                    BrokerInput::ConnectionFailed {
                        epoch,
                        reconnect: ReconnectSchedule::new(
                            timer_id,
                            now,
                            self.entropy.next_sample(),
                        ),
                    }
                }
                (BrokerPhase::Draining, _) => BrokerInput::ConnectionDrained { epoch },
                (
                    BrokerPhase::Dormant
                    | BrokerPhase::Backoff
                    | BrokerPhase::Refreshing
                    | BrokerPhase::Closed,
                    _,
                ) => break,
            };
            let transition = self.broker.apply(input);
            require_applied(transition.disposition())?;
            self.interpret_broker_effects(poller, transition.into_effects(), now)?;
        }
        Ok(())
    }

    pub(super) fn interpret_broker_effects(
        &mut self,
        poller: &Poller,
        effects: Vec<BrokerEffect>,
        now: Moment,
    ) -> Result<(), BrokerError> {
        for effect in effects {
            match effect {
                BrokerEffect::OpenConnection { epoch } => {
                    self.open_connection(poller, epoch, now)?;
                }
                BrokerEffect::ScheduleReconnect {
                    failed_epoch,
                    timer_id,
                    at,
                } => self.timers.schedule(DeadlineTimer::for_reconnect(
                    timer_id,
                    failed_epoch,
                    at,
                ))?,
                BrokerEffect::CancelReconnect { timer_id } => {
                    self.timers.cancel(timer_id);
                }
                BrokerEffect::DrainConnection { epoch } => {
                    if epoch != self.connection.epoch() {
                        return Err(BrokerError::MissingEffect);
                    }
                    let transition = self.connection.apply(ConnectionInput::BeginDrain)?;
                    self.interpret_close(poller, transition.into_effects(), None)?;
                }
            }
        }
        Ok(())
    }
}

fn require_applied(disposition: BrokerDisposition) -> Result<(), BrokerError> {
    match disposition {
        BrokerDisposition::Applied => Ok(()),
        BrokerDisposition::Ignored | BrokerDisposition::IgnoredStale => {
            Err(BrokerError::MissingEffect)
        }
    }
}

fn expect_no_broker_effects(effects: &[BrokerEffect]) -> Result<(), BrokerError> {
    match effects.first().copied() {
        Some(effect) => Err(BrokerError::UnexpectedBrokerEffect(effect)),
        None => Ok(()),
    }
}
