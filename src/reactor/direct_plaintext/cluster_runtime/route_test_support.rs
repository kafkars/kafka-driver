//! Shared construction helpers for private discovered-route runtime proofs.

use std::{
    cell::Cell,
    io,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId, CallId,
    DnsOutcome, DnsRequest, HostName, IpAddress, MetadataGeneration, Moment, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::{DriverLimits, MetadataLimits, RequestError, TrafficClass, request::ErasedRequest};

use crate::reactor::direct_plaintext::cluster_runtime::ClusterRuntime;
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        lane_plan::{
            BorneraLanePlan, KafkaSessionPlan,
            factory::{BorneraEndpointFamily, BorneraLanePlanFactory, PlaintextLanePlanFactory},
        },
        owner::DirectSet,
    },
};

pub(super) const NOW: Moment = Moment::from_nanos(1);

pub(super) fn runtime(
    max_brokers: usize,
    waiting_calls: usize,
    admission_budget: usize,
) -> ClusterRuntime<TcpTransport> {
    let metadata = MetadataLimits::new(
        BrokerDirectoryLimits::new(nonzero(max_brokers)),
        Duration::from_secs(1),
    )
    .with_waiting_limits(
        nonzero(waiting_calls),
        nonzero(32_768),
        nonzero(admission_budget),
    );
    ClusterRuntime::new(&DriverLimits::default().with_metadata_limits(metadata))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"))
}

pub(super) fn driver(max_brokers: usize, admission_budget: usize) -> DriverLimits {
    let metadata = MetadataLimits::new(
        BrokerDirectoryLimits::new(nonzero(max_brokers)),
        Duration::from_secs(1),
    )
    .with_waiting_limits(nonzero(16), nonzero(32_768), nonzero(admission_budget));
    DriverLimits::default().with_metadata_limits(metadata)
}

pub(super) fn directory(
    generation: u64,
    broker: BrokerId,
    endpoint: BrokerEndpoint,
    limit: usize,
) -> BrokerDirectory {
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(generation),
        [BrokerDirectoryEntry::new(broker, endpoint)],
        BrokerDirectoryLimits::new(nonzero(limit)),
    )
    .unwrap_or_else(|error| panic!("broker directory: {error}"))
}

pub(super) fn broker(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("broker ID: {error}"))
}

pub(super) fn endpoint(host: &str, port: u16) -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new(host).unwrap_or_else(|error| panic!("host: {error}")),
        port_nonzero(port),
    )
}

pub(super) fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            port_nonzero(port),
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("resolved addresses: {error}"))
}

pub(super) fn success(request: &DnsRequest, port: u16) -> DnsOutcome {
    DnsOutcome::new(request.epoch(), request.effect_id(), Ok(addresses(port)))
}

pub(super) fn request(
    id: u64,
    traffic: TrafficClass,
    timeout: Duration,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn ErasedRequest>,
) {
    crate::request::erased_request_in(
        CallId::from_raw(id),
        traffic,
        ApiVersionsRequest::default(),
        timeout,
    )
}

pub(super) fn plaintext_factory(driver: &DriverLimits) -> PlaintextLanePlanFactory {
    match BorneraEndpointFamily::from_template(
        driver,
        BrokerLimits::default(),
        crate::config::BrokerTemplate::plaintext(),
    ) {
        BorneraEndpointFamily::Plaintext(factory) => factory,
        #[cfg(feature = "tls-rustls")]
        BorneraEndpointFamily::Rustls(_) => panic!("plaintext template selected rustls"),
    }
}

pub(super) fn fail<T>(error: io::Error) -> T {
    panic!("discovered-route operation: {}", Box::new(error))
}

pub(super) struct CountingFactory {
    pub(super) attempts: Cell<usize>,
    physical_epochs: Arc<Mutex<Vec<BorneraEpoch>>>,
}

pub(super) struct FailingFactory {
    pub(super) attempts: Cell<usize>,
}

impl FailingFactory {
    pub(super) const fn new() -> Self {
        Self {
            attempts: Cell::new(0),
        }
    }
}

impl BorneraLanePlanFactory<TcpTransport> for FailingFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        self.attempts.set(self.attempts.get() + 1);
        Err(io::Error::other("synthetic route factory failure"))
    }
}

impl CountingFactory {
    pub(super) fn new() -> Self {
        Self {
            attempts: Cell::new(0),
            physical_epochs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(super) fn physical_epochs(&self) -> Vec<BorneraEpoch> {
        self.physical_epochs
            .lock()
            .unwrap_or_else(|error| panic!("physical epoch log: {error}"))
            .clone()
    }
}

impl BorneraLanePlanFactory<TcpTransport> for CountingFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        self.attempts.set(self.attempts.get() + 1);
        let broker = BrokerLimits::default();
        Ok(BorneraLanePlan::new(
            crate::config::BrokerAddresses::Direct(SocketAddr::from(([127, 0, 0, 1], 9))),
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            Box::new(ImmediateEndpointFailure {
                epochs: Arc::clone(&self.physical_epochs),
            }),
        ))
    }
}

struct ImmediateEndpointFailure {
    epochs: Arc<Mutex<Vec<BorneraEpoch>>>,
}

impl DirectConnectionAttempt<TcpTransport> for ImmediateEndpointFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: BorneraLaneOwner,
        _address: SocketAddr,
        epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        self.epochs
            .lock()
            .unwrap_or_else(|error| panic!("record physical epoch: {error}"))
            .push(epoch);
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

fn port_nonzero(value: u16) -> NonZeroU16 {
    NonZeroU16::new(value).unwrap_or(NonZeroU16::MIN)
}
