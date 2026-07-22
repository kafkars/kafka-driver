//! Blocking host lookup and bounded logical-address conversion on one private thread.

use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    sync::mpsc::{Receiver, SyncSender},
    thread,
};

use kafka_driver_core::{
    DnsFailure, DnsOutcome, DnsRequest, IpAddress, ResolutionLimits, ResolvedAddress,
    ResolvedAddressSet,
};

use crate::reactor::WakeHandle;

const WORKER_NAME: &str = "kafka-driver-dns";

pub(super) fn spawn(
    requests: Receiver<DnsRequest>,
    outcomes: SyncSender<DnsOutcome>,
    limits: ResolutionLimits,
    wake: WakeHandle,
) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name(WORKER_NAME.into())
        .spawn(move || run(&requests, &outcomes, limits, &wake))
}

fn run(
    requests: &Receiver<DnsRequest>,
    outcomes: &SyncSender<DnsOutcome>,
    limits: ResolutionLimits,
    wake: &WakeHandle,
) {
    while let Ok(request) = requests.recv() {
        let outcome = resolve(&request, limits);
        if outcomes.send(outcome).is_err() {
            break;
        }
        if wake.wake().is_err() {
            break;
        }
    }
}

fn resolve(request: &DnsRequest, limits: ResolutionLimits) -> DnsOutcome {
    let result = resolve_endpoint(request.endpoint(), limits);
    DnsOutcome::new(request.epoch(), request.effect_id(), result)
}

fn resolve_endpoint(
    endpoint: &kafka_driver_core::BrokerEndpoint,
    limits: ResolutionLimits,
) -> Result<ResolvedAddressSet, DnsFailure> {
    let addresses = (endpoint.host().as_str(), endpoint.port().get())
        .to_socket_addrs()
        .map_err(|error| sanitize_failure(&error))?;
    ResolvedAddressSet::try_from_iter(addresses.map(logical_address), limits)
        .map_err(|_| DnsFailure::NoUsableAddress)
}

fn logical_address(address: SocketAddr) -> ResolvedAddress {
    match address {
        SocketAddr::V4(address) => ResolvedAddress::new(
            IpAddress::V4(address.ip().octets()),
            nonzero_port(address.port()),
        ),
        SocketAddr::V6(address) => ResolvedAddress::ipv6(
            address.ip().octets(),
            nonzero_port(address.port()),
            address.flowinfo(),
            address.scope_id(),
        ),
    }
}

fn sanitize_failure(error: &io::Error) -> DnsFailure {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::AddrNotAvailable => DnsFailure::NameNotFound,
        _ => DnsFailure::Temporary,
    }
}

fn nonzero_port(port: u16) -> std::num::NonZeroU16 {
    std::num::NonZeroU16::new(port)
        .unwrap_or_else(|| unreachable!("a nonzero resolver input port must be retained"))
}
