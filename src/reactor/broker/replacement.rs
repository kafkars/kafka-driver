//! Terminal broker reconfiguration that preserves stale-safe resource generations.

use kafka_driver_core::{BrokerMachine, ConnectionEpoch};
use kafka_wire_core::DecodeLimits;

use crate::{config::BrokerConfig, reactor::timer::TimerHeap, response::ResponseRegistry};

use super::{
    BrokerError, address_rotation::AddressRotation, entropy::BackoffEntropy, owner::SingleBroker,
};

impl SingleBroker {
    pub(in crate::reactor) fn reconfigure(
        &mut self,
        config: BrokerConfig,
        epoch: ConnectionEpoch,
    ) -> Result<(), BrokerError> {
        if !self.is_terminal() {
            return Err(BrokerError::ReplacementBeforeTerminal);
        }
        let (addresses, security, sasl) = config.into_parts();
        let addresses = AddressRotation::new(addresses);
        let primary = addresses.primary().ok_or(BrokerError::MissingEffect)?;
        self.entropy = BackoffEntropy::for_broker(primary);
        self.addresses = addresses;
        self.address_refresh = None;
        self.broker = BrokerMachine::new(epoch, self.limits.backoff());
        self.connection = Self::connection_machine(
            epoch,
            self.connection_limits,
            sasl.as_ref(),
            self.authentication_limits,
        );
        self.resources.replace_security(security);
        self.resource_token = None;
        self.responses =
            ResponseRegistry::new(self.limits.response_capacity(), DecodeLimits::default());
        self.timers = TimerHeap::new(self.limits.timer_capacity());
        self.due_timers.clear();
        self.negotiation_exchange = None;
        self.authentication_session = None;
        self.authentication_exchange = None;
        self.frames.clear();
        self.completed_writes.clear();
        self.retry_read = false;
        self.retry_write = false;
        self.sasl = sasl;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::reactor) fn resource_token_for_test(&self) -> Option<usize> {
        self.resource_token
            .map(crate::reactor::resource::ResourceToken::get)
    }
}
