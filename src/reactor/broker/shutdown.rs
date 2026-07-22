//! Machine-owned drain initiation and terminal-state observation.

use kafka_driver_core::{ConnectionInput, ConnectionPhase};

use crate::reactor::Poller;

use super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(in crate::reactor) fn begin_drain(&mut self, poller: &Poller) -> Result<(), BrokerError> {
        let transition = self.machine.apply(ConnectionInput::BeginDrain)?;
        self.interpret_close(poller, transition.into_effects(), None)
    }

    pub(in crate::reactor) fn is_terminal(&self) -> bool {
        self.machine.state().phase() == ConnectionPhase::Closed
    }
}
