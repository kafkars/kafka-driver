//! Lazy route-turn preparation preserves lane fairness and configured throughput.

use std::{num::NonZeroUsize, time::Duration};

use bornera::TcpTransport;
use kafka_driver_core::{BrokerDirectoryLimits, EffectId, Moment};

use crate::{DriverLimits, MetadataLimits, RequestError, TrafficClass};

use super::super::ClusterRuntime;
use super::source_test_support as fixture;

const DUE: Moment = Moment::from_nanos(10);

#[test]
fn seed_turns_do_not_advance_past_the_next_route_lane() {
    let mut runtime = runtime(8, 1, 1);
    let route = fixture::install_test_directory(&mut runtime);
    let mut causality = crate::reactor::causality::CausalSequence::new();
    let (bulk, _, _) = fixture::queue_route(
        &mut runtime,
        route,
        (
            1,
            TrafficClass::Bulk,
            Duration::from_nanos(10),
            Some(EffectId::from_raw(1)),
            Moment::ORIGIN,
        ),
        &mut causality,
    );
    let (long_poll, _, _) = fixture::queue_route(
        &mut runtime,
        route,
        (
            2,
            TrafficClass::LongPoll,
            Duration::from_nanos(10),
            Some(EffectId::from_raw(2)),
            Moment::ORIGIN,
        ),
        &mut causality,
    );
    let first_seed = fixture::queue_seed(&mut runtime, 3, Duration::from_nanos(10), Moment::ORIGIN);
    runtime.routes_first = false;

    runtime
        .drive(DUE, &mut causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(first_seed.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(bulk.try_result().is_none());
    assert!(long_poll.try_result().is_none());

    let second_seed =
        fixture::queue_seed(&mut runtime, 4, Duration::from_nanos(10), Moment::ORIGIN);
    runtime
        .drive(DUE, &mut causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(bulk.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(long_poll.try_result().is_none());
    let (replacement_bulk, _, _) = fixture::queue_route(
        &mut runtime,
        route,
        (
            5,
            TrafficClass::Bulk,
            Duration::from_nanos(10),
            None,
            Moment::ORIGIN,
        ),
        &mut causality,
    );

    runtime
        .drive(DUE, &mut causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(second_seed.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(long_poll.try_result().is_none());
    runtime
        .drive(DUE, &mut causality)
        .unwrap_or_else(fixture::fail);
    assert_eq!(long_poll.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(replacement_bulk.try_result().is_none());
}

#[test]
fn one_route_can_spend_the_full_multi_call_expiry_budget() {
    let mut runtime = runtime(4, 3, 1);
    let route = fixture::install_test_directory(&mut runtime);
    let mut causality = crate::reactor::causality::CausalSequence::new();
    let mut calls = Vec::new();
    for id in 10..13 {
        let effect = (id == 10).then(|| EffectId::from_raw(id));
        let (call, _, _) = fixture::queue_route(
            &mut runtime,
            route,
            (
                id,
                TrafficClass::Bulk,
                Duration::from_nanos(10),
                effect,
                Moment::ORIGIN,
            ),
            &mut causality,
        );
        calls.push(call);
    }
    runtime.routes_first = true;

    runtime
        .drive(DUE, &mut causality)
        .unwrap_or_else(fixture::fail);

    assert!(
        calls
            .iter()
            .all(|call| call.try_result() == Some(Ok(Err(deadline_exceeded()))))
    );
    assert_eq!(runtime.next_deadline(), None);
}

fn runtime(
    waiting_calls: usize,
    admission_budget: usize,
    lane_turn_budget: usize,
) -> ClusterRuntime<TcpTransport> {
    let metadata = MetadataLimits::new(
        BrokerDirectoryLimits::new(nonzero(1)),
        Duration::from_secs(1),
    )
    .with_waiting_limits(
        nonzero(waiting_calls),
        nonzero(32_768),
        nonzero(admission_budget),
    )
    .with_lane_turn_budget(nonzero(lane_turn_budget));
    ClusterRuntime::new(&DriverLimits::default().with_metadata_limits(metadata))
        .unwrap_or_else(fixture::fail)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::DeadlineExceeded,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}
