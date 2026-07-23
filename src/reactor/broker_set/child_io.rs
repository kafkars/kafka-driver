//! I/O, timer, and shutdown delegation for one lazy broker child.

use kafka_driver_core::{
    BrokerCloseReason, BrokerState, CallFailure, CloseReason, Delivery, Moment, OutcomeStamp,
};

use crate::{
    RequestError,
    reactor::{
        PollEvent, Poller,
        broker::{DeadlineProgress, SingleBroker},
    },
};

use super::{BrokerSetError, child::BrokerChild};

impl BrokerChild {
    pub(super) fn observe(
        &mut self,
        poller: &Poller,
        event: PollEvent,
        now: Moment,
        observed_at: OutcomeStamp,
    ) -> Result<bool, BrokerSetError> {
        let progress = self.connection.as_mut().map_or(Ok(false), |connection| {
            connection
                .observe(poller, event, now, observed_at)
                .map_err(BrokerSetError::Broker)
        })?;
        Ok(progress | self.settle_terminal_waiting())
    }

    pub(super) fn continue_io(
        &mut self,
        poller: &Poller,
        now: Moment,
        observed_at: OutcomeStamp,
    ) -> Result<bool, BrokerSetError> {
        let mut progress = false;
        if self.retired && !self.retirement_started {
            if let Some(connection) = &mut self.connection
                && !connection.is_terminal()
            {
                connection
                    .begin_drain(poller, now)
                    .map_err(BrokerSetError::Broker)?;
            }
            self.retirement_started = true;
            progress = true;
        }
        progress |= self.connection.as_mut().map_or(Ok(false), |connection| {
            connection
                .continue_io(poller, now, observed_at)
                .map_err(BrokerSetError::Broker)
        })?;
        Ok(progress | self.settle_terminal_waiting())
    }

    pub(super) fn fire_due(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<DeadlineProgress, BrokerSetError> {
        let expiration = self.waiting.expire_due(now);
        let mut progress = DeadlineProgress::from_work(expiration.settled(), expiration.more_due());
        progress = progress.merge(self.connection.as_mut().map_or(
            Ok(DeadlineProgress::idle()),
            |connection| {
                connection
                    .fire_due(poller, now)
                    .map_err(BrokerSetError::Broker)
            },
        )?);
        self.settle_terminal_waiting();
        Ok(progress)
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.waiting
            .next_deadline()
            .into_iter()
            .chain(
                self.connection
                    .as_ref()
                    .and_then(SingleBroker::next_deadline),
            )
            .min()
    }

    pub(super) fn begin_drain(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        self.waiting.fail_all(&draining());
        self.abandon_pending();
        self.connection.as_mut().map_or(Ok(()), |connection| {
            connection
                .begin_drain(poller, now)
                .map_err(BrokerSetError::Broker)
        })
    }

    pub(super) fn is_terminal(&self) -> bool {
        self.connection
            .as_ref()
            .is_none_or(SingleBroker::is_terminal)
    }

    pub(super) fn needs_runnable_turn(&self) -> bool {
        self.connection
            .as_ref()
            .is_some_and(SingleBroker::has_continuation_io)
            || (self.is_ready() && !self.waiting.is_empty())
            || (self
                .connection
                .as_ref()
                .is_some_and(SingleBroker::is_terminal)
                && !self.waiting.is_empty())
            || self.has_installable()
            || self.is_reusable()
            || (self.retired
                && !self.retirement_started
                && self
                    .connection
                    .as_ref()
                    .is_some_and(|connection| !connection.is_terminal()))
    }

    fn is_ready(&self) -> bool {
        self.connection.as_ref().is_some_and(|connection| {
            connection.state().phase() == kafka_driver_core::ConnectionPhase::Ready
                && connection.broker_state().phase() == kafka_driver_core::BrokerPhase::Available
        })
    }

    fn settle_terminal_waiting(&mut self) -> bool {
        let Some(BrokerState::Closed { reason }) =
            self.connection.as_ref().map(SingleBroker::broker_state)
        else {
            return false;
        };
        if self.waiting.is_empty() {
            return false;
        }
        if reason == BrokerCloseReason::Requested && self.replacement_in_flight() {
            return false;
        }
        self.waiting.fail_all(&terminal(reason));
        true
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}

fn terminal(reason: BrokerCloseReason) -> RequestError {
    if let BrokerCloseReason::EndpointResolutionFailed(failure) = reason {
        return RequestError::NameResolutionFailed { failure };
    }
    let failure = match reason {
        BrokerCloseReason::AuthenticationFailed(failure) => CallFailure::ConnectionClosed {
            reason: CloseReason::AuthenticationFailed(failure),
        },
        BrokerCloseReason::Requested => CallFailure::Draining,
        BrokerCloseReason::EpochExhausted
        | BrokerCloseReason::RetryExhausted
        | BrokerCloseReason::ClockOverflow => CallFailure::Closed,
        BrokerCloseReason::EndpointResolutionFailed(_) => {
            unreachable!("endpoint resolution returned above")
        }
    };
    RequestError::Rejected {
        failure,
        delivery: Delivery::NotSent,
    }
}
