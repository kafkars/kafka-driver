//! Compact scenario builders for endpoint-replacement assertions.

use std::time::Duration;

use bornera::TcpTransport;
use kafka_driver_core::{BrokerEndpoint, BrokerId, BrokerRoute, Moment};

use crate::reactor::causality::CausalSequence;
use crate::reactor::direct_plaintext::endpoint_refresh::DirectRefreshOwner;
use crate::{RequestError, TrafficClass, reactor::BrokerLane};

use super::super::{ClusterRuntime, route_test_support as support};
use super::test_support as fixture;
use support::fail;

pub(super) struct SparseReplacement {
    pub(super) runtime: ClusterRuntime<TcpTransport>,
    pub(super) broker: BrokerId,
    pub(super) new_endpoint: BrokerEndpoint,
    pub(super) old_owners: [DirectRefreshOwner; TrafficClass::COUNT],
    pub(super) new_owners: [DirectRefreshOwner; TrafficClass::COUNT],
    pub(super) control: BrokerLane,
    pub(super) long_poll: BrokerLane,
    pub(super) new_call: fixture::ResponseCall,
    pub(super) newer_call: fixture::ResponseCall,
    pub(super) long_call: fixture::ResponseCall,
    pub(super) deadline_call: fixture::ResponseCall,
    pub(super) replacement: support::CountingFactory,
}

struct WarmSparse {
    runtime: ClusterRuntime<TcpTransport>,
    broker: BrokerId,
    old_owners: [DirectRefreshOwner; TrafficClass::COUNT],
    old_factory: support::CountingFactory,
    causality: CausalSequence,
}

struct QueuedSparse {
    warm: WarmSparse,
    new_endpoint: BrokerEndpoint,
    control: BrokerLane,
    long_poll: BrokerLane,
    new_call: fixture::ResponseCall,
    newer_call: fixture::ResponseCall,
    long_call: fixture::ResponseCall,
    deadline_call: fixture::ResponseCall,
}

pub(super) fn sparse_replacement() -> SparseReplacement {
    let mut queued = queue_sparse_replacement(warm_sparse());
    let replacement = support::CountingFactory::new();
    assert!(queued.warm.runtime.route_install_has_local_work());
    assert!(
        fixture::drive(
            &mut queued.warm.runtime,
            &replacement,
            &mut queued.warm.causality,
        )
        .unwrap_or_else(fail)
    );
    assert!(queued.warm.runtime.families[&queued.warm.broker].is_retiring());
    assert!(
        fixture::drive(
            &mut queued.warm.runtime,
            &replacement,
            &mut queued.warm.causality,
        )
        .unwrap_or_else(fail)
    );
    let new_owners = fixture::owners(&queued.warm.runtime, queued.warm.broker);
    SparseReplacement {
        runtime: queued.warm.runtime,
        broker: queued.warm.broker,
        new_endpoint: queued.new_endpoint,
        old_owners: queued.warm.old_owners,
        new_owners,
        control: queued.control,
        long_poll: queued.long_poll,
        new_call: queued.new_call,
        newer_call: queued.newer_call,
        long_call: queued.long_call,
        deadline_call: queued.deadline_call,
        replacement,
    }
}

fn warm_sparse() -> WarmSparse {
    let mut runtime = support::runtime(1, 8, 8);
    let broker = support::broker(7);
    let old = support::directory(1, broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&old).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    let (old_call, _) = fixture::activate(
        &mut runtime,
        fixture::route(&old, broker),
        1,
        TrafficClass::Control,
        1,
        9092,
        &old_factory,
        &mut causality,
    );
    let old_owners = fixture::owners(&runtime, broker);
    assert!(runtime.view(old_owners[0]).is_some());
    assert!(
        old_owners[1..]
            .iter()
            .all(|owner| runtime.view(*owner).is_none())
    );
    let new = support::directory(2, broker, support::endpoint("new.test", 9093), 1);
    runtime.install_directory(&new).unwrap_or_else(fail);
    assert_eq!(
        old_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    WarmSparse {
        runtime,
        broker,
        old_owners,
        old_factory,
        causality,
    }
}

fn queue_sparse_replacement(mut warm: WarmSparse) -> QueuedSparse {
    let new_endpoint = support::endpoint("new.test", 9093);
    let new = support::directory(2, warm.broker, new_endpoint.clone(), 1);
    let route = fixture::route(&new, warm.broker);
    let (new_call, control, control_dns) = fixture::submit_dns(
        &mut warm.runtime,
        route,
        2,
        TrafficClass::Control,
        Duration::from_secs(5),
        2,
        support::NOW,
        &mut warm.causality,
    );
    let (newer_call, newer_request) =
        support::request(5, TrafficClass::Control, Duration::from_secs(5));
    assert!(
        warm.runtime
            .submit_route(
                route,
                None,
                newer_request,
                support::NOW,
                &mut warm.causality
            )
            .unwrap_or_else(fail)
            .is_none()
    );
    fixture::defer(
        &mut warm.runtime,
        control,
        &control_dns,
        9093,
        &warm.old_factory,
    );
    let (long_call, long_poll, long_dns) = fixture::submit_dns(
        &mut warm.runtime,
        route,
        3,
        TrafficClass::LongPoll,
        Duration::from_secs(5),
        3,
        support::NOW,
        &mut warm.causality,
    );
    fixture::defer(
        &mut warm.runtime,
        long_poll,
        &long_dns,
        9093,
        &warm.old_factory,
    );
    let (deadline_call, deadline_request) =
        support::request(4, TrafficClass::LongPoll, Duration::from_nanos(10));
    assert!(
        warm.runtime
            .submit_route(
                route,
                None,
                deadline_request,
                Moment::ORIGIN,
                &mut warm.causality
            )
            .unwrap_or_else(fail)
            .is_none()
    );
    QueuedSparse {
        warm,
        new_endpoint,
        control,
        long_poll,
        new_call,
        newer_call,
        long_call,
        deadline_call,
    }
}

pub(super) struct LazyPending {
    pub(super) runtime: ClusterRuntime<TcpTransport>,
    pub(super) broker: BrokerId,
    pub(super) route: BrokerRoute,
    pub(super) control: BrokerLane,
    pub(super) causality: CausalSequence,
    pub(super) replacement: support::CountingFactory,
}

pub(super) fn lazy_pending() -> LazyPending {
    let mut runtime = support::runtime(1, 8, 8);
    let broker = support::broker(7);
    let old = support::directory(1, broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&old).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    let (warmup, lane) = fixture::activate(
        &mut runtime,
        fixture::route(&old, broker),
        10,
        TrafficClass::Control,
        10,
        9092,
        &old_factory,
        &mut causality,
    );
    fixture::fail_front(&mut runtime, lane, 10);
    assert_eq!(warmup.try_result(), Some(Ok(Err(fixture::closed()))));
    let new = support::directory(2, broker, support::endpoint("new.test", 9093), 1);
    runtime.install_directory(&new).unwrap_or_else(fail);
    let route = fixture::route(&new, broker);
    let (expired, control, control_dns) = fixture::submit_dns(
        &mut runtime,
        route,
        11,
        TrafficClass::Control,
        Duration::from_nanos(10),
        11,
        Moment::ORIGIN,
        &mut causality,
    );
    let (_long_call, long_poll, long_dns) = fixture::submit_dns(
        &mut runtime,
        route,
        12,
        TrafficClass::LongPoll,
        Duration::from_secs(5),
        12,
        support::NOW,
        &mut causality,
    );
    fixture::defer(&mut runtime, control, &control_dns, 9093, &old_factory);
    fixture::defer(&mut runtime, long_poll, &long_dns, 9093, &old_factory);
    runtime
        .routes
        .get_mut(&control)
        .unwrap_or_else(|| panic!("expired control route"))
        .waiting
        .expire_due_bounded(Moment::from_nanos(10), None, 8);
    assert_eq!(
        expired.try_result(),
        Some(Ok(Err(fixture::deadline_exceeded())))
    );
    let replacement = support::CountingFactory::new();
    for _ in 0..2 {
        assert!(
            runtime
                .drive_route_installs(&replacement, Moment::from_nanos(10), &mut causality)
                .unwrap_or_else(fail)
        );
    }
    LazyPending {
        runtime,
        broker,
        route,
        control,
        causality,
        replacement,
    }
}
