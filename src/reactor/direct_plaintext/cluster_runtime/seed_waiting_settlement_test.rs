//! Shutdown and permanent-failure settlement for external seed-route ownership.

use std::{
    io,
    net::{SocketAddr, TcpListener},
    num::NonZeroUsize,
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    AuthenticationFailure, BrokerCloseReason, BrokerDirectoryLimits, BrokerState, CallFailure,
    CallId, CloseReason, ConnectionEpoch, Delivery, Moment,
};
use kafka_wire::ApiVersionsRequest;

use crate::{DriverLimits, MetadataLimits, RequestError, request::erased_request};

use super::ClusterRuntime;
use crate::reactor::{
    broker::BrokerLimits,
    causality::CausalSequence,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        cluster_runtime::seed::SeedReplacement,
        endpoint_refresh::DirectRefreshOwner,
        lane_plan::{BorneraLanePlan, KafkaSessionPlan},
        owner::DirectSet,
    },
};

const NOW: Moment = Moment::from_nanos(1);
const TERMINAL_NOW: Moment = Moment::from_nanos(u64::MAX);

#[test]
fn global_drain_settles_external_waiters_in_bounded_fifo_batches() {
    let mut runtime = runtime(3, 1);
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(1);
    let (second_call, second) = request(2);
    let (third_call, third) = request(3);
    for request in [first, second, third] {
        runtime
            .submit_seed(request, NOW, &mut causality)
            .unwrap_or_else(|error| panic!("retain pre-drain seed call: {error}"));
    }

    runtime.begin_seed_waiting_drain();
    assert!(first_call.try_result().is_none());
    assert!(second_call.try_result().is_none());
    assert!(third_call.try_result().is_none());
    assert!(runtime.has_local_work());

    assert!(
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| { panic!("settle first shutdown batch: {error}") })
    );
    assert_eq!(first_call.try_result(), Some(Ok(Err(draining()))));
    assert!(second_call.try_result().is_none());
    assert!(third_call.try_result().is_none());
    assert!(runtime.has_local_work());

    assert!(
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| { panic!("settle second shutdown batch: {error}") })
    );
    assert_eq!(second_call.try_result(), Some(Ok(Err(draining()))));
    assert!(third_call.try_result().is_none());
    assert!(runtime.has_local_work());

    assert!(
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| { panic!("settle third shutdown batch: {error}") })
    );
    assert_eq!(third_call.try_result(), Some(Ok(Err(draining()))));
    assert!(runtime.seed_waiting.is_empty());

    let (later_call, later) = request(4);
    runtime
        .submit_seed(later, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("reject post-drain seed call: {error}"));
    assert_eq!(later_call.try_result(), Some(Ok(Err(draining()))));
    assert!(runtime.seed_waiting.is_empty());
}

#[test]
fn permanent_seed_failure_preserves_exact_not_sent_reason_and_lane_isolation() {
    let mut runtime = runtime(3, 1);
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(10);
    let (second_call, second) = request(11);
    runtime
        .submit_seed(first, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain first terminal seed call: {error}"));
    runtime
        .submit_seed(second, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain second terminal seed call: {error}"));
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind terminal seed: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("terminal seed address: {error}"));
    let plan = BorneraLanePlan::plaintext(
        &runtime.driver,
        BrokerLimits::default(),
        crate::config::BrokerAddresses::Direct(address),
        None,
        None,
    );
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), plan, NOW)
        .unwrap_or_else(|error| panic!("install terminal seed: {error}"));
    close_seed_with_authentication_failure(&mut runtime, owner, &mut causality);
    assert!(runtime.has_local_work());

    assert!(
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| { panic!("settle first permanent seed batch: {error}") })
    );
    let expected = authentication_failure();
    assert_eq!(first_call.try_result(), Some(Ok(Err(expected.clone()))));
    assert!(second_call.try_result().is_none());
    assert_seed_lane_empty(&runtime, owner);
    runtime.begin_seed_waiting_drain();

    let (later_call, later) = request(12);
    runtime
        .submit_seed(later, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("reject later terminal seed call: {error}"));
    assert_eq!(later_call.try_result(), Some(Ok(Err(draining()))));
    assert_seed_lane_empty(&runtime, owner);

    assert!(
        runtime
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| { panic!("settle second permanent seed batch: {error}") })
    );
    assert_eq!(second_call.try_result(), Some(Ok(Err(expected))));
    assert!(runtime.seed_waiting.is_empty());
    assert_seed_lane_empty(&runtime, owner);
}

#[test]
fn replacement_waits_for_terminal_settlement_then_reopens_external_admission() {
    let mut runtime = runtime(3, 1);
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(20);
    let (second_call, second) = request(21);
    runtime
        .submit_seed(first, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain first replaceable call: {error}"));
    runtime
        .submit_seed(second, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain second replaceable call: {error}"));
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), TERMINAL_NOW)
        .unwrap_or_else(|error| panic!("install replaceable terminal seed: {error}"));
    assert!(runtime.has_local_work());
    runtime
        .drive(TERMINAL_NOW, &mut causality)
        .unwrap_or_else(|error| panic!("settle first replacement batch: {error}"));
    assert!(matches!(first_call.try_result(), Some(Ok(Err(_)))));
    assert!(second_call.try_result().is_none());

    let SeedReplacement::Busy(retained) = runtime
        .replace_terminal_seed(ConnectionEpoch::from_raw(2), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("defer seed replacement: {error}"))
    else {
        panic!("terminal waiter must fence seed replacement");
    };
    runtime
        .drive(TERMINAL_NOW, &mut causality)
        .unwrap_or_else(|error| panic!("settle final replacement batch: {error}"));
    assert!(matches!(second_call.try_result(), Some(Ok(Err(_)))));
    assert!(runtime.seed_waiting.is_empty());

    assert!(matches!(
        runtime
            .replace_terminal_seed(ConnectionEpoch::from_raw(2), *retained, NOW)
            .unwrap_or_else(|error| panic!("replace settled terminal seed: {error}")),
        SeedReplacement::Replaced
    ));
    let fresh = runtime.seed.unwrap_or_else(|| panic!("fresh seed")).owner;
    let (later_call, later) = request(22);
    runtime
        .submit_seed(later, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain call after replacement: {error}"));
    assert!(later_call.try_result().is_none());
    assert!(!runtime.seed_waiting.is_empty());
    assert_seed_lane_empty(&runtime, fresh);
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

fn assert_seed_lane_empty(runtime: &ClusterRuntime<TcpTransport>, owner: DirectRefreshOwner) {
    let lane = runtime
        .view(owner)
        .unwrap_or_else(|| panic!("seed lane view"));
    let contexts = lane.contexts.snapshot();
    assert!(lane.pending.is_empty());
    assert_eq!(contexts.reserved(), 0);
    assert_eq!(contexts.published(), 0);
}

fn close_seed_with_authentication_failure(
    runtime: &mut ClusterRuntime<TcpTransport>,
    owner: DirectRefreshOwner,
    causality: &mut CausalSequence,
) {
    let index = runtime
        .index(owner)
        .unwrap_or_else(|error| panic!("terminal seed index: {error}"));
    let connection = runtime.lanes[index].connection_for_test();
    runtime
        .connections
        .abandon_unpublished(connection)
        .unwrap_or_else(|error| panic!("abandon terminal seed connection: {error}"));
    runtime.lanes[index].connection = None;
    runtime.lanes[index].last_close_reason = Some(authentication_rejection());
    runtime
        .access(owner)
        .unwrap_or_else(|| panic!("terminal seed access"))
        .settle_generation_lifecycle(
            ConnectionEpoch::from_raw(1),
            authentication_rejection(),
            NOW,
            causality,
        )
        .unwrap_or_else(|error| panic!("close terminal seed policy: {error}"));
    assert!(matches!(
        runtime.lanes[index].lifecycle.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(AuthenticationFailure::Rejected)
        }
    ));
}

const fn authentication_rejection() -> CloseReason {
    CloseReason::AuthenticationFailed(AuthenticationFailure::Rejected)
}

fn authentication_failure() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: authentication_rejection(),
        },
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
