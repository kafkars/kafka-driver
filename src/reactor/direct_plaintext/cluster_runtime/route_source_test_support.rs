//! Live seed and route fixtures for cross-source scheduler proofs.

use std::{thread::JoinHandle, time::Duration};

use bornera::TcpTransport;
use calandria::Span;
use kafka_driver_core::{BrokerRoute, ConnectionEpoch, DnsRequest, EffectId, Moment};
use kafka_wire::ApiVersionsResponse;

use crate::{
    RequestError, TrafficClass,
    reactor::{BrokerLane, causality::CausalSequence, route_waiting::RouteWaitingOutcome},
};

use super::super::{ClusterRuntime, route_resolution::RouteResolutionProgress};
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        lane_plan::BorneraLanePlan,
        shared_set_fixture_test::{address, listener, spawn_lane},
    },
};

use super::super::route_test_support as support;
pub(super) use support::fail;

pub(super) const NOW: Moment = support::NOW;

pub(super) struct ReadyRoute {
    pub(super) runtime: ClusterRuntime<TcpTransport>,
    pub(super) lane: BrokerLane,
    pub(super) causality: CausalSequence,
    pub(super) server: JoinHandle<()>,
}

pub(super) struct ReadySeed {
    pub(super) runtime: ClusterRuntime<TcpTransport>,
    pub(super) causality: CausalSequence,
    pub(super) server: JoinHandle<()>,
}

pub(super) type RouteRequest = (u64, TrafficClass, Duration, Option<EffectId>, Moment);

pub(super) fn ready_route(public_error_code: i16) -> ReadyRoute {
    let listener = listener();
    let address = address(&listener);
    let server = spawn_lane(listener, None, public_error_code);
    let driver = support::driver(1, 1);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver).unwrap_or_else(fail);
    let broker = support::broker(7);
    let directory = support::directory(
        1,
        broker,
        support::endpoint("route.test", address.port()),
        1,
    );
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("ready route"));
    let (warmup_call, warmup) =
        support::request(1, TrafficClass::Interactive, Duration::from_secs(5));
    let mut causality = CausalSequence::new();
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            warmup,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("ready-route DNS"));
    let progress = runtime
        .complete_route_resolution(
            lane,
            support::success(&dns, address.port()),
            &support::plaintext_factory(&driver),
            support::NOW,
        )
        .unwrap_or_else(fail);
    let RouteResolutionProgress::Activated(owner) = progress else {
        panic!("ready route must activate")
    };
    let RouteWaitingOutcome::Ready(warmup) = runtime
        .routes
        .get_mut(&lane)
        .unwrap_or_else(|| panic!("ready-route state"))
        .waiting
        .pop(support::NOW, None)
    else {
        panic!("warmup route waiter")
    };
    warmup.fail(closed());
    assert_eq!(warmup_call.try_result(), Some(Ok(Err(closed()))));
    drive_until_ready(&mut runtime, owner, &mut causality);
    ReadyRoute {
        runtime,
        lane,
        causality,
        server,
    }
}

pub(super) fn ready_seed(public_error_code: i16) -> ReadySeed {
    let listener = listener();
    let address = address(&listener);
    let server = spawn_lane(listener, None, public_error_code);
    let driver = support::driver(1, 1);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver).unwrap_or_else(fail);
    let plan = BorneraLanePlan::plaintext(
        &driver,
        BrokerLimits::default(),
        crate::config::BrokerAddresses::Direct(address),
        None,
        None,
    );
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), plan, support::NOW)
        .unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    drive_until_ready(&mut runtime, owner, &mut causality);
    ReadySeed {
        runtime,
        causality,
        server,
    }
}

pub(super) fn queue_ready_route(
    runtime: &mut ClusterRuntime<TcpTransport>,
    lane: BrokerLane,
    id: u64,
) -> crate::Call<Result<ApiVersionsResponse, RequestError>> {
    let (call, request) = support::request(id, lane.traffic_class(), Duration::from_secs(5));
    assert!(
        runtime
            .routes
            .get_mut(&lane)
            .unwrap_or_else(|| panic!("ready route state"))
            .waiting
            .admit(request, support::NOW)
    );
    call
}

pub(super) fn queue_seed(
    runtime: &mut ClusterRuntime<TcpTransport>,
    id: u64,
    timeout: Duration,
    now: Moment,
) -> crate::Call<Result<ApiVersionsResponse, RequestError>> {
    let (call, request) = support::request(id, TrafficClass::Control, timeout);
    runtime.seed_waiting.push(request, now);
    call
}

pub(super) fn queue_route(
    runtime: &mut ClusterRuntime<TcpTransport>,
    route: BrokerRoute,
    input: RouteRequest,
    causality: &mut CausalSequence,
) -> (
    crate::Call<Result<ApiVersionsResponse, RequestError>>,
    BrokerLane,
    Option<DnsRequest>,
) {
    let (id, traffic, timeout, effect_id, now) = input;
    let (call, request) = support::request(id, traffic, timeout);
    let dns = runtime
        .submit_route(route, effect_id, request, now, causality)
        .unwrap_or_else(fail);
    (
        call,
        BrokerLane::new(route.broker_id(), traffic),
        dns.map(|(_, dns)| dns),
    )
}

pub(super) fn finish_live_call(
    runtime: &mut ClusterRuntime<TcpTransport>,
    causality: &mut CausalSequence,
    call: &crate::Call<Result<ApiVersionsResponse, RequestError>>,
    now: Moment,
) -> ApiVersionsResponse {
    for _turn in 0..128 {
        runtime.drive(now, causality).unwrap_or_else(fail);
        if let Some(result) = call.try_result() {
            return match result {
                Ok(Ok(response)) => response,
                other => panic!("live source call failed: {other:?}"),
            };
        }
        if !runtime.has_local_work() {
            runtime
                .wait(Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO))
                .unwrap_or_else(fail);
        }
    }
    panic!("live source call did not settle")
}

pub(super) fn install_test_directory(runtime: &mut ClusterRuntime<TcpTransport>) -> BrokerRoute {
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("route.test", 9092), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("test route"))
}

fn drive_until_ready(
    runtime: &mut ClusterRuntime<TcpTransport>,
    owner: super::super::super::endpoint_refresh::DirectRefreshOwner,
    causality: &mut CausalSequence,
) {
    for _turn in 0..128 {
        runtime.drive(support::NOW, causality).unwrap_or_else(fail);
        let index = runtime.index(owner).unwrap_or_else(fail);
        if runtime.lanes[index].can_admit_public() {
            return;
        }
        if !runtime.has_local_work() {
            runtime
                .wait(Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO))
                .unwrap_or_else(fail);
        }
    }
    panic!("live source lane did not become ready")
}

pub(super) fn closed() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::Closed,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}
