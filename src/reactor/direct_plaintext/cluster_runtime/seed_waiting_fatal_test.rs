//! Fatal and stale-map totality for external seed-route ownership.

use std::{
    io,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerDirectoryLimits, BrokerEndpoint, BrokerState, CallFailure, CallId, ConnectionEpoch,
    Delivery, HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::{DriverLimits, MetadataLimits, RequestError, request::erased_request};

use super::ClusterRuntime;
use crate::reactor::{
    bootstrap::ResolvedSeed,
    broker::BrokerLimits,
    causality::CausalSequence,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        lane_plan::{BorneraLanePlan, KafkaSessionPlan, factory::BorneraLanePlanFactory},
        owner::DirectSet,
    },
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn pre_seed_factory_failure_totalizes_every_external_waiter() {
    let mut runtime = runtime(2, 1);
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(1);
    let (second_call, second) = request(2);
    for request in [first, second] {
        runtime
            .submit_seed(request, NOW, &mut causality)
            .unwrap_or_else(|error| panic!("retain pre-seed call: {error}"));
    }

    let error = runtime
        .install_resolved_seed(&FailingFactory, resolved_seed(), NOW)
        .err()
        .unwrap_or_else(|| panic!("seed factory must fail"));

    assert_eq!(error.to_string(), "synthetic seed factory failure");
    let expected = Some(Ok(Err(closed())));
    assert_eq!(first_call.try_result(), expected.clone());
    assert_eq!(second_call.try_result(), expected);
    assert!(runtime.seed_waiting.is_empty());
    assert!(runtime.seed.is_none());
    assert!(runtime.lanes.is_empty());
}

#[test]
fn host_fatal_drive_totalizes_every_external_waiter_before_returning_error() {
    let mut runtime = runtime(2, 1);
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install fatal seed: {error}"));
    let index = runtime
        .index(owner)
        .unwrap_or_else(|error| panic!("fatal seed index: {error}"));
    let deadline = runtime.lanes[index]
        .lifecycle
        .next_deadline()
        .unwrap_or_else(|| panic!("fatal seed reconnect deadline"));
    runtime.lanes[index].lifecycle.exhaust_timer_ids();
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(1);
    let (second_call, second) = request(2);
    for request in [first, second] {
        runtime
            .submit_seed(request, NOW, &mut causality)
            .unwrap_or_else(|error| panic!("retain fatal seed call: {error}"));
    }

    let error = runtime
        .drive(deadline, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("timer identity exhaustion must fail the host"));

    assert_eq!(
        error.to_string(),
        "direct reconnect timer identities were exhausted"
    );
    assert!(!matches!(
        runtime.lanes[index].lifecycle.state(),
        BrokerState::Closed { .. }
    ));
    let expected = Some(Ok(Err(closed())));
    assert_eq!(first_call.try_result(), expected.clone());
    assert_eq!(second_call.try_result(), expected);
    assert!(runtime.seed_waiting.is_empty());
    assert!(runtime.lanes[index].pending.is_empty());
}

#[test]
fn shutdown_ignores_a_stale_seed_map_until_bounded_waiters_are_totalized() {
    let mut runtime = runtime(2, 1);
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install stale shutdown seed: {error}"));
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(3);
    let (second_call, second) = request(4);
    for request in [first, second] {
        runtime
            .submit_seed(request, NOW, &mut causality)
            .unwrap_or_else(|error| panic!("retain stale shutdown call: {error}"));
    }
    assert!(runtime.slots.remove(&owner).is_some());

    runtime.begin_seed_waiting_drain();
    assert!(
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| { panic!("settle first stale shutdown batch: {error}") })
    );
    assert_eq!(first_call.try_result(), Some(Ok(Err(draining()))));
    assert!(second_call.try_result().is_none());
    assert!(runtime.has_local_work());

    assert!(
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| { panic!("settle second stale shutdown batch: {error}") })
    );
    assert_eq!(second_call.try_result(), Some(Ok(Err(draining()))));
    assert!(runtime.seed_waiting.is_empty());
}

fn runtime(waiting_calls: usize, admission_budget: usize) -> ClusterRuntime<TcpTransport> {
    let metadata = MetadataLimits::new(
        BrokerDirectoryLimits::new(nonzero(1)),
        Duration::from_secs(1),
    )
    .with_waiting_limits(
        nonzero(waiting_calls),
        nonzero(16_384),
        nonzero(admission_budget),
    );
    ClusterRuntime::new(&DriverLimits::default().with_metadata_limits(metadata))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"))
}

fn request(
    raw_call_id: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    )
}

fn failed_plan() -> BorneraLanePlan<TcpTransport> {
    let broker = BrokerLimits::default();
    BorneraLanePlan::new(
        crate::config::BrokerAddresses::Direct(SocketAddr::from(([127, 0, 0, 1], 9))),
        broker,
        None,
        KafkaSessionPlan::new(None, broker),
        Box::new(RecoverableFailure),
    )
}

fn resolved_seed() -> ResolvedSeed {
    let port = NonZeroU16::MIN;
    let endpoint = BrokerEndpoint::new(
        HostName::new("seed.test").unwrap_or_else(|error| panic!("seed host: {error}")),
        port,
    );
    let addresses = ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), port)],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("seed addresses: {error}"));
    ResolvedSeed::new(ConnectionEpoch::from_raw(1), endpoint, addresses)
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

struct RecoverableFailure;

struct FailingFactory;

impl BorneraLanePlanFactory<TcpTransport> for FailingFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        Err(io::Error::other("synthetic seed factory failure"))
    }
}

impl DirectConnectionAttempt<TcpTransport> for RecoverableFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: BorneraLaneOwner,
        _address: SocketAddr,
        _epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}
