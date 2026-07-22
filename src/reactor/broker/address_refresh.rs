//! Background DNS refresh ownership for a broker's logical endpoint.

use kafka_driver_core::{BrokerEndpoint, ResolvedAddressSet};

use super::owner::SingleBroker;

impl SingleBroker {
    pub(in crate::reactor) const fn address_refresh_needed(&self) -> bool {
        self.address_refresh.is_some()
    }

    pub(in crate::reactor) fn take_address_refresh(&mut self) -> Option<BrokerEndpoint> {
        self.address_refresh.take()
    }

    pub(in crate::reactor) fn request_address_refresh(&mut self, endpoint: BrokerEndpoint) {
        self.address_refresh = Some(endpoint);
    }

    pub(in crate::reactor) fn replace_resolved_addresses(
        &mut self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
    ) {
        self.addresses.replace(endpoint, addresses);
        self.address_refresh = None;
    }
}
