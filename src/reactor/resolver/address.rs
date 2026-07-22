//! Conversion from capability-free resolver values into reactor socket addresses.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};

use kafka_driver_core::{IpAddress, ResolvedAddress};

pub(in crate::reactor) fn socket_address(address: ResolvedAddress) -> SocketAddr {
    match address.ip() {
        IpAddress::V4(octets) => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), address.port().get())
        }
        IpAddress::V6(octets) => SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(octets),
            address.port().get(),
            address.flow_info(),
            address.scope_id(),
        )),
    }
}
