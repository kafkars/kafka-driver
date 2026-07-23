//! Machine-owned drain initiation and terminal-state observation.

use kafka_driver_core::{BrokerInput, BrokerPhase, Moment};

use crate::reactor::Poller;

use super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(in crate::reactor) fn begin_drain(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let transition = self.broker.apply(BrokerInput::BeginDrain);
        self.interpret_broker_effects(poller, transition.into_effects(), now)?;
        if self.broker.state().phase() == BrokerPhase::Closed {
            self.address_refresh = None;
        }
        self.reconcile_connection(poller, now)
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.broker.state().phase() == BrokerPhase::Closed
    }
}
