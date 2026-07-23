//! Terminal broker reconfiguration that preserves stale-safe resource generations.

use kafka_driver_core::{BrokerMachine, BrokerState, ConnectionEpoch, Moment};
use kafka_wire_core::DecodeLimits;

use crate::{
    config::BrokerConfig,
    reactor::{Poller, timer::TimerHeap},
    response::ResponseRegistry,
};

use super::{BrokerError, address_rotation::AddressRotation, owner::SingleBroker};

impl SingleBroker {
    pub(in crate::reactor) fn replace_exhausted_endpoint(
        &mut self,
        config: BrokerConfig,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let epoch = match self.broker.state() {
            BrokerState::Refreshing { .. } => {
                let (addresses, security, sasl) = config.into_parts();
                if matches!(&addresses, crate::config::BrokerAddresses::Direct(_)) {
                    return Err(BrokerError::MissingEffect);
                }
                let failed_epoch = self.refresh_epoch()?;
                self.replace_connection_parts(addresses, security, sasl);
                self.resume_after_refresh(failed_epoch, poller, now)?;
                return Ok(());
            }
            BrokerState::Backoff { .. } => {
                self.replace_connection_config(config);
                return Ok(());
            }
            BrokerState::Closed { .. } => self
                .connection
                .epoch()
                .get()
                .checked_add(1)
                .map(ConnectionEpoch::from_raw)
                .ok_or(BrokerError::IdentityExhausted)?,
            _ => return Err(BrokerError::ReplacementBeforeTerminal),
        };
        self.begin_drain(poller, now)?;
        self.reconfigure(config, epoch)?;
        self.start(poller, now)
    }

    pub(in crate::reactor) fn reconfigure(
        &mut self,
        config: BrokerConfig,
        epoch: ConnectionEpoch,
    ) -> Result<(), BrokerError> {
        if !self.is_terminal() {
            return Err(BrokerError::ReplacementBeforeTerminal);
        }
        self.replace_connection_config(config);
        self.broker = BrokerMachine::new(epoch, self.limits.backoff());
        self.connection = Self::connection_machine(
            epoch,
            self.connection_limits,
            self.sasl.as_ref(),
            self.authentication_limits,
        );
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
        Ok(())
    }

    fn replace_connection_config(&mut self, config: BrokerConfig) {
        let (addresses, security, sasl) = config.into_parts();
        self.replace_connection_parts(addresses, security, sasl);
    }

    fn replace_connection_parts(
        &mut self,
        addresses: crate::config::BrokerAddresses,
        security: crate::config::BrokerSecurity,
        sasl: Option<crate::SaslConfig>,
    ) {
        self.entropy = AddressRotation::entropy_for(&addresses);
        self.addresses = AddressRotation::new(addresses);
        self.address_refresh = None;
        self.resources.replace_security(security);
        self.sasl = sasl;
    }

    #[cfg(test)]
    pub(in crate::reactor) fn resource_token_for_test(&self) -> Option<usize> {
        self.resource_token
            .map(crate::reactor::resource::ResourceToken::get)
    }
}
