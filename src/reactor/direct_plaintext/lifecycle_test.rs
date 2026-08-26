//! Exact fixed-address reconnect timing and shutdown policy proofs.

use std::{net::SocketAddr, time::Duration};

use kafka_driver_core::{
    BrokerEffect, BrokerState, CloseReason, ConnectionEpoch, DnsFailure, Moment, TransportFailure,
};

use super::lifecycle::DirectLifecycle;
use crate::reactor::broker::BrokerLimits;

const NOW: Moment = Moment::from_nanos(1_000);

#[test]
fn unexpected_close_waits_for_its_exact_deadline_then_opens_epoch_two() {
    let mut lifecycle = lifecycle();
    lifecycle.replace_entropy(0);
    lifecycle
        .ready(ConnectionEpoch::from_raw(1))
        .unwrap_or_else(|error| panic!("ready first direct generation: {error}"));

    let effects = lifecycle
        .generation_ended(
            ConnectionEpoch::from_raw(1),
            CloseReason::TransportLost(TransportFailure::Reset),
            NOW,
            false,
        )
        .unwrap_or_else(|error| panic!("close first direct generation: {error}"));
    let BrokerState::Backoff {
        failed_epoch,
        next_epoch,
        timer_id,
        deadline,
        ..
    } = lifecycle.state()
    else {
        panic!("unexpected close must enter direct backoff");
    };
    assert_eq!(failed_epoch, ConnectionEpoch::from_raw(1));
    assert_eq!(next_epoch, ConnectionEpoch::from_raw(2));
    assert_eq!(lifecycle.next_deadline(), Some(deadline));
    assert!(deadline > NOW);
    assert!(
        deadline
            <= NOW
                .checked_add(Duration::from_millis(100))
                .unwrap_or_else(|| panic!("first reconnect cap must fit"))
    );
    assert_eq!(
        effects,
        vec![BrokerEffect::ScheduleReconnect {
            failed_epoch,
            timer_id,
            at: deadline,
        }]
    );

    let early = Moment::from_nanos(deadline.as_nanos() - 1);
    assert!(
        lifecycle
            .fire_due(early)
            .unwrap_or_else(|error| panic!("check early reconnect: {error}"))
            .is_none()
    );
    assert_eq!(lifecycle.next_deadline(), Some(deadline));
    assert_eq!(
        lifecycle
            .fire_due(deadline)
            .unwrap_or_else(|error| panic!("fire exact reconnect: {error}")),
        Some(vec![BrokerEffect::OpenConnection {
            epoch: ConnectionEpoch::from_raw(2),
        }])
    );
    assert!(matches!(
        lifecycle.state(),
        BrokerState::Connecting { epoch, .. } if epoch == ConnectionEpoch::from_raw(2)
    ));
}

#[test]
fn shutdown_owns_backoff_and_prevents_a_later_open() {
    let mut lifecycle = lifecycle();
    let effects = lifecycle
        .generation_ended(
            ConnectionEpoch::from_raw(1),
            CloseReason::TransportLost(TransportFailure::Other),
            NOW,
            false,
        )
        .unwrap_or_else(|error| panic!("schedule direct reconnect: {error}"));
    assert!(matches!(
        effects.as_slice(),
        [BrokerEffect::ScheduleReconnect { .. }]
    ));
    let cancel = lifecycle
        .begin_drain()
        .unwrap_or_else(|error| panic!("drain direct backoff: {error}"));
    assert!(matches!(
        cancel.as_slice(),
        [BrokerEffect::CancelReconnect { .. }]
    ));
    assert!(matches!(lifecycle.state(), BrokerState::Closed { .. }));
    assert_eq!(lifecycle.next_deadline(), None);
    assert!(
        lifecycle
            .fire_due(Moment::from_nanos(u64::MAX))
            .unwrap_or_else(|error| panic!("check post-shutdown reconnect: {error}"))
            .is_none()
    );
}

#[test]
fn refresh_backoff_shutdown_cancels_the_exact_pretransition_timer() {
    let mut lifecycle = lifecycle();
    lifecycle.replace_entropy(0);
    let _effects = lifecycle
        .generation_ended(
            ConnectionEpoch::from_raw(1),
            CloseReason::TransportLost(TransportFailure::Reset),
            NOW,
            true,
        )
        .unwrap_or_else(|error| panic!("suspend reconnect for refresh: {error}"));
    lifecycle
        .begin_endpoint_refresh(ConnectionEpoch::from_raw(1))
        .unwrap_or_else(|error| panic!("begin endpoint refresh: {error}"));
    let effects = lifecycle
        .fail_endpoint_refresh(ConnectionEpoch::from_raw(1), DnsFailure::Temporary, NOW)
        .unwrap_or_else(|error| panic!("schedule endpoint refresh retry: {error}"));
    let [BrokerEffect::ScheduleEndpointRefreshRetry { timer_id, .. }] = effects.as_slice() else {
        panic!("refresh failure must schedule its exact timer");
    };

    assert_eq!(
        lifecycle
            .begin_drain()
            .unwrap_or_else(|error| panic!("cancel refresh retry: {error}")),
        vec![BrokerEffect::CancelEndpointRefreshRetry {
            timer_id: *timer_id,
        }]
    );
    assert!(matches!(lifecycle.state(), BrokerState::Closed { .. }));
    assert!(
        lifecycle
            .fire_due(Moment::from_nanos(u64::MAX))
            .unwrap_or_else(|error| panic!("check cancelled refresh timer: {error}"))
            .is_none()
    );
}

fn lifecycle() -> DirectLifecycle {
    DirectLifecycle::started(
        BrokerLimits::default().backoff(),
        crate::reactor::entropy::JitterEntropy::for_value(&SocketAddr::from((
            [127, 0, 0, 1],
            9092,
        ))),
    )
    .unwrap_or_else(|error| panic!("start direct lifecycle: {error}"))
}
