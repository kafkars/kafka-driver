//! Shared discovered-route replacement fixtures for focused proofs.

use std::{cell::Cell, io, net::SocketAddr, num::NonZeroUsize, rc::Rc, time::Duration};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    BrokerRoute, DnsRequest, EffectId, MetadataGeneration, Moment,
};
use kafka_wire::ApiVersionsResponse;

use crate::reactor::direct_plaintext::{
    attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt, PlaintextAttempt},
    endpoint_refresh::DirectRefreshOwner,
    lane_plan::{BorneraLanePlan, KafkaSessionPlan, factory::BorneraLanePlanFactory},
    owner::DirectSet,
};
use crate::reactor::route_waiting::RouteWaitingOutcome;
use crate::reactor::{broker::BrokerLimits, causality::CausalSequence};
use crate::{Call, DriverLimits, RequestError, TrafficClass, reactor::BrokerLane};

use super::super::{
    ClusterRuntime, route_resolution::RouteResolutionProgress, route_state::PendingInstall,
    route_test_support as support,
};

pub(super) type ResponseCall = Call<Result<ApiVersionsResponse, RequestError>>;

pub(super) fn directory<const N: usize>(
    generation: u64,
    entries: [(BrokerId, BrokerEndpoint); N],
    limit: usize,
) -> BrokerDirectory {
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(generation),
        entries.map(|(broker, endpoint)| BrokerDirectoryEntry::new(broker, endpoint)),
        BrokerDirectoryLimits::new(NonZeroUsize::new(limit).unwrap_or(NonZeroUsize::MIN)),
    )
    .unwrap_or_else(|error| panic!("broker directory: {error}"))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn submit_dns(
    runtime: &mut ClusterRuntime<TcpTransport>,
    route: BrokerRoute,
    id: u64,
    traffic: TrafficClass,
    timeout: Duration,
    effect: u64,
    now: Moment,
    causality: &mut CausalSequence,
) -> (ResponseCall, BrokerLane, DnsRequest) {
    let (call, request) = support::request(id, traffic, timeout);
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(effect)),
            request,
            now,
            causality,
        )
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("route must request DNS"));
    (call, lane, dns)
}

pub(super) fn route(directory: &BrokerDirectory, broker: BrokerId) -> BrokerRoute {
    directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("broker route"))
}

pub(super) fn owners(
    runtime: &ClusterRuntime<TcpTransport>,
    broker: BrokerId,
) -> [DirectRefreshOwner; TrafficClass::COUNT] {
    TrafficClass::ALL.map(|traffic| {
        runtime
            .family_owner(broker, traffic)
            .unwrap_or_else(|| panic!("broker family owner"))
    })
}

pub(super) fn pending(runtime: &ClusterRuntime<TcpTransport>, lane: BrokerLane) -> PendingInstall {
    runtime
        .routes
        .get(&lane)
        .and_then(|state| state.pending_install.clone())
        .unwrap_or_else(|| panic!("pending route install"))
}

pub(super) fn complete(
    runtime: &mut ClusterRuntime<TcpTransport>,
    lane: BrokerLane,
    dns: &DnsRequest,
    port: u16,
    factory: &dyn BorneraLanePlanFactory<TcpTransport>,
    now: Moment,
) -> RouteResolutionProgress {
    runtime
        .complete_route_resolution(lane, support::success(dns, port), factory, now)
        .unwrap_or_else(support::fail)
}

pub(super) fn defer(
    runtime: &mut ClusterRuntime<TcpTransport>,
    lane: BrokerLane,
    dns: &DnsRequest,
    port: u16,
    factory: &dyn BorneraLanePlanFactory<TcpTransport>,
) {
    assert!(matches!(
        complete(runtime, lane, dns, port, factory, support::NOW),
        RouteResolutionProgress::Deferred(_)
    ));
}

pub(super) fn drive(
    runtime: &mut ClusterRuntime<TcpTransport>,
    factory: &dyn BorneraLanePlanFactory<TcpTransport>,
    causality: &mut CausalSequence,
) -> io::Result<bool> {
    runtime.drive_route_installs(factory, support::NOW, causality)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn activate(
    runtime: &mut ClusterRuntime<TcpTransport>,
    route: BrokerRoute,
    id: u64,
    traffic: TrafficClass,
    effect: u64,
    port: u16,
    factory: &dyn BorneraLanePlanFactory<TcpTransport>,
    causality: &mut CausalSequence,
) -> (ResponseCall, BrokerLane) {
    let (call, lane, dns) = submit_dns(
        runtime,
        route,
        id,
        traffic,
        Duration::from_secs(5),
        effect,
        support::NOW,
        causality,
    );
    assert!(matches!(
        complete(runtime, lane, &dns, port, factory, support::NOW),
        RouteResolutionProgress::Activated(_)
    ));
    (call, lane)
}

pub(super) fn fail_front(
    runtime: &mut ClusterRuntime<TcpTransport>,
    lane: BrokerLane,
    expected: u64,
) {
    let RouteWaitingOutcome::Ready(request) = runtime
        .routes
        .get_mut(&lane)
        .unwrap_or_else(|| panic!("route waiter state"))
        .waiting
        .pop(support::NOW, None)
    else {
        panic!("ready route waiter")
    };
    assert_eq!(request.call_id().get(), expected);
    request.fail(closed());
}

pub(super) struct PartialStartFactory {
    address: SocketAddr,
    fatal_at: usize,
    attempts: Cell<usize>,
    successful_starts: Rc<Cell<usize>>,
}

impl PartialStartFactory {
    pub(super) fn new(address: SocketAddr, fatal_at: usize) -> Self {
        Self {
            address,
            fatal_at,
            attempts: Cell::new(0),
            successful_starts: Rc::new(Cell::new(0)),
        }
    }

    pub(super) fn attempts(&self) -> usize {
        self.attempts.get()
    }

    pub(super) fn successful_starts(&self) -> usize {
        self.successful_starts.get()
    }
}

impl BorneraLanePlanFactory<TcpTransport> for PartialStartFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: kafka_driver_core::ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        let index = self.attempts.get();
        self.attempts.set(index + 1);
        let broker = BrokerLimits::default();
        let attempt: Box<dyn DirectConnectionAttempt<TcpTransport>> = if index == self.fatal_at {
            Box::new(FatalStart)
        } else {
            Box::new(CountedPlaintextStart {
                inner: PlaintextAttempt::new(&DriverLimits::default(), broker),
                successful_starts: Rc::clone(&self.successful_starts),
            })
        };
        Ok(BorneraLanePlan::new(
            crate::config::BrokerAddresses::Direct(self.address),
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            attempt,
        ))
    }
}

struct CountedPlaintextStart {
    inner: PlaintextAttempt,
    successful_starts: Rc<Cell<usize>>,
}

impl DirectConnectionAttempt<TcpTransport> for CountedPlaintextStart {
    fn connect(
        &self,
        set: &mut DirectSet<TcpTransport>,
        owner: BorneraLaneOwner,
        address: SocketAddr,
        epoch: BorneraEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        let connection = self.inner.connect(set, owner, address, epoch, now)?;
        self.successful_starts.set(self.successful_starts.get() + 1);
        Ok(connection)
    }
}

struct FatalStart;

impl DirectConnectionAttempt<TcpTransport> for FatalStart {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: BorneraLaneOwner,
        _address: SocketAddr,
        _epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        Err(DirectConnectError::fatal("synthetic partial-start failure"))
    }
}

pub(super) fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::DeadlineExceeded,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}

pub(super) fn closed() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::Closed,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}
