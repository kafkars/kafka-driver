//! Bounded ordered address selection across fresh connection epochs.

use std::net::SocketAddr;

use crate::{config::BrokerAddresses, reactor::resolver::socket_address};

/// Reactor-local cursor over one nonempty configured or resolved address set.
#[derive(Debug)]
pub(super) struct AddressRotation {
    addresses: Vec<SocketAddr>,
    next: usize,
}

impl AddressRotation {
    pub(super) fn new(addresses: BrokerAddresses) -> Self {
        let addresses = match addresses {
            BrokerAddresses::Direct(address) => vec![address],
            BrokerAddresses::Resolved(addresses) => {
                addresses.iter().copied().map(socket_address).collect()
            }
        };
        debug_assert!(!addresses.is_empty());
        Self { addresses, next: 0 }
    }

    pub(super) fn primary(&self) -> SocketAddr {
        self.addresses[0]
    }

    pub(super) fn next(&mut self) -> SocketAddr {
        let address = self.addresses[self.next];
        self.next = (self.next + 1) % self.addresses.len();
        address
    }
}
