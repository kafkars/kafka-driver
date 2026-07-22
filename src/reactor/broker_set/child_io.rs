//! I/O, timer, and shutdown delegation for one lazy broker child.

use kafka_driver_core::{CallFailure, Delivery, Moment};

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
    ) -> Result<bool, BrokerSetError> {
        self.connection.as_mut().map_or(Ok(false), |connection| {
            connection
                .observe(poller, event, now)
                .map_err(BrokerSetError::Broker)
        })
    }

    pub(super) fn continue_io(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        self.connection.as_mut().map_or(Ok(false), |connection| {
            connection
                .continue_io(poller, now)
                .map_err(BrokerSetError::Broker)
        })
    }

    pub(super) fn fire_due(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<DeadlineProgress, BrokerSetError> {
        self.connection
            .as_mut()
            .map_or(Ok(DeadlineProgress::idle()), |connection| {
                connection
                    .fire_due(poller, now)
                    .map_err(BrokerSetError::Broker)
            })
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.connection
            .as_ref()
            .and_then(SingleBroker::next_deadline)
    }

    pub(super) fn begin_drain(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        self.waiting.fail_all(&draining());
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

    pub(super) fn has_local_io(&self) -> bool {
        self.connection
            .as_ref()
            .is_some_and(SingleBroker::has_local_io)
            || (self.is_ready() && !self.waiting.is_empty())
    }

    fn is_ready(&self) -> bool {
        self.connection.as_ref().is_some_and(|connection| {
            connection.state().phase() == kafka_driver_core::ConnectionPhase::Ready
        })
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}
