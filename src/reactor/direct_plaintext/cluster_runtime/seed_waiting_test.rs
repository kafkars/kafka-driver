//! External seed-route waiting survives replacement and remains bounded.

use std::{io, net::SocketAddr, num::NonZeroUsize, time::Duration};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use calandria::Span;
use kafka_driver_core::{BrokerDirectoryLimits, CallFailure, CallId, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{DriverLimits, MetadataLimits, RequestError, request::erased_request};

use super::ClusterRuntime;
use crate::reactor::{
    broker::BrokerLimits,
    causality::CausalSequence,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        endpoint_refresh::DirectRefreshOwner,
        lane_plan::{BorneraLanePlan, KafkaSessionPlan},
        owner::DirectSet,
        shared_set_fixture_test::{address, listener, response, spawn_lane},
    },
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn pre_seed_waiting_uses_metadata_route_capacity() {
    let driver = driver(1, 1);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(1, Duration::from_secs(1));
    runtime
        .submit_seed(first, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain first seed call: {error}"));
    let (overflow_call, overflow) = request(2, Duration::from_secs(1));

    runtime
        .submit_seed(overflow, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("settle seed overflow: {error}"));

    assert!(first_call.try_result().is_none());
    assert!(matches!(
        overflow_call.wait(),
        Ok(Err(RequestError::RouteCapacityReached {
            call_limit: 1,
            ..
        }))
    ));
}

#[test]
fn stale_seed_mapping_settles_the_incoming_call_before_returning_error() {
    let driver = driver(1, 1);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let owner = runtime
        .install_seed(
            kafka_driver_core::ConnectionEpoch::from_raw(1),
            failed_plan(),
            NOW,
        )
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    assert!(runtime.slots.remove(&owner).is_some());
    let (call, request) = request(3, Duration::from_secs(1));

    let error = runtime
        .submit_seed(request, NOW, &mut CausalSequence::new())
        .err()
        .unwrap_or_else(|| panic!("stale seed mapping must fail"));

    assert_eq!(error.to_string(), "Bornera cluster seed owner is stale");
    assert_eq!(call.wait(), Ok(Err(RequestError::IdentityConflict)));
    assert!(runtime.seed_waiting.is_empty());
}

#[test]
fn stale_seed_mapping_remains_local_work_without_releasing_a_waiter() {
    let driver = driver(1, 1);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let owner = runtime
        .install_seed(
            kafka_driver_core::ConnectionEpoch::from_raw(1),
            failed_plan(),
            NOW,
        )
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    let mut causality = CausalSequence::new();
    let (call, request) = request(4, Duration::from_secs(1));
    runtime
        .submit_seed(request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain seed waiter: {error}"));
    let index = runtime
        .slots
        .remove(&owner)
        .unwrap_or_else(|| panic!("seed slot"));

    assert!(runtime.has_local_work());
    let error = runtime
        .drive(NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("stale seed mapping must fail"));
    assert_eq!(error.to_string(), "Bornera cluster seed owner is stale");
    assert!(call.try_result().is_none());
    assert!(!runtime.seed_waiting.is_empty());

    runtime.slots.insert(owner, index);
    assert!(
        runtime
            .drive(Moment::from_nanos(1_000_000_001), &mut causality)
            .unwrap_or_else(|error| panic!("expire restored seed waiter: {error}"))
    );
    assert_eq!(call.wait(), Ok(Err(deadline_exceeded())));
}

#[test]
fn aggregate_deadline_expires_seed_waiting_with_its_turn_budget() {
    let driver = driver(2, 1);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(1, Duration::from_nanos(10));
    let (second_call, second) = request(2, Duration::from_nanos(10));
    runtime
        .submit_seed(first, Moment::ORIGIN, &mut causality)
        .unwrap_or_else(|error| panic!("retain first deadline: {error}"));
    runtime
        .submit_seed(second, Moment::ORIGIN, &mut causality)
        .unwrap_or_else(|error| panic!("retain second deadline: {error}"));
    assert_eq!(runtime.next_deadline(), Some(Moment::from_nanos(10)));

    assert!(
        runtime
            .drive(Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(|error| panic!("expire first seed waiter: {error}"))
    );
    assert_eq!(first_call.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(second_call.try_result().is_none());
    assert_eq!(runtime.next_deadline(), Some(Moment::from_nanos(10)));

    assert!(
        runtime
            .drive(Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(|error| panic!("expire second seed waiter: {error}"))
    );
    assert_eq!(second_call.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert_eq!(runtime.next_deadline(), None);
}

#[test]
fn queued_call_survives_seed_lane_replacement_and_uses_the_new_owner() {
    let driver = driver(2, 2);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let old = runtime
        .install_seed(
            kafka_driver_core::ConnectionEpoch::from_raw(1),
            failed_plan(),
            NOW,
        )
        .unwrap_or_else(|error| panic!("install failed seed: {error}"));
    let mut causality = CausalSequence::new();
    let (call, request) = request(7, Duration::from_secs(5));
    runtime
        .submit_seed(request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain call outside failed seed: {error}"));
    assert!(!runtime.seed_waiting.is_empty());
    assert!(
        runtime
            .view(old)
            .is_some_and(|seed| seed.pending.is_empty())
    );
    make_reclaimable(&mut runtime, old, &mut causality);

    let listener = listener();
    let address = address(&listener);
    let server = spawn_lane(listener, None, 19);
    let replacement = runtime
        .replace_terminal_seed(
            kafka_driver_core::ConnectionEpoch::from_raw(2),
            live_plan(&driver, address),
            NOW,
        )
        .unwrap_or_else(|error| panic!("replace terminal seed: {error}"));
    assert!(matches!(
        replacement,
        super::super::seed::SeedReplacement::Replaced
    ));
    let fresh = runtime
        .seed
        .unwrap_or_else(|| panic!("replacement seed"))
        .owner;
    assert_ne!(fresh, old);
    assert!(!runtime.seed_waiting.is_empty());
    assert!(
        runtime
            .view(fresh)
            .is_some_and(|seed| seed.pending.is_empty())
    );

    let mut result = None;
    for _turn in 0..128 {
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("drive replacement seed: {error}"));
        result = result.or_else(|| call.try_result());
        if result.is_some() {
            break;
        }
        if !runtime.has_local_work() {
            runtime
                .wait(Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO))
                .unwrap_or_else(|error| panic!("wait for replacement seed: {error}"));
        }
    }

    assert_eq!(result, Some(Ok(Ok(response(19)))));
    assert!(runtime.seed_waiting.is_empty());
    server
        .join()
        .unwrap_or_else(|_| panic!("join replacement seed server"));
}

fn driver(waiting_calls: usize, admission_budget: usize) -> DriverLimits {
    let metadata = MetadataLimits::new(
        BrokerDirectoryLimits::new(nonzero(1)),
        Duration::from_secs(1),
    )
    .with_waiting_limits(
        nonzero(waiting_calls),
        nonzero(16_384),
        nonzero(admission_budget),
    );
    DriverLimits::default().with_metadata_limits(metadata)
}

fn request(
    raw_call_id: u64,
    timeout: Duration,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        timeout,
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

fn live_plan(driver: &DriverLimits, address: SocketAddr) -> BorneraLanePlan<TcpTransport> {
    BorneraLanePlan::plaintext(
        driver,
        BrokerLimits::default(),
        crate::config::BrokerAddresses::Direct(address),
        None,
        None,
    )
}

fn make_reclaimable(
    runtime: &mut ClusterRuntime<TcpTransport>,
    owner: DirectRefreshOwner,
    causality: &mut CausalSequence,
) {
    runtime
        .access(owner)
        .unwrap_or_else(|| panic!("seed lane access"))
        .begin_session_drain(NOW, causality)
        .unwrap_or_else(|error| panic!("drain old seed lane: {error}"));
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

struct RecoverableFailure;

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
