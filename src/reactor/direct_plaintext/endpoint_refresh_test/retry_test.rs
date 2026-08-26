//! Retryable and terminal endpoint-refresh failure proofs.

use std::time::Duration;

use kafka_driver_core::{
    AddressRefreshState, BrokerCloseReason, BrokerState, CallFailure, CallId, Delivery, DnsFailure,
    Moment,
};
use kafka_wire::ApiVersionsRequest;

use super::support::{RefreshFixture, START};
use crate::{
    CompletionError, RequestError, reactor::causality::CausalSequence, request::erased_request,
};

#[test]
fn retryable_failures_wait_and_due_effectless_retry_reports_progress() {
    for failure in [DnsFailure::NameNotFound, DnsFailure::Temporary] {
        let mut fixture = RefreshFixture::pending(61, 5);
        let refresh = fixture.take();
        fixture.lane.lifecycle.replace_entropy(0);
        fixture
            .set
            .access(&mut fixture.lane)
            .fail_endpoint_refresh(&refresh, failure, START, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("schedule refresh retry: {error}"));
        let BrokerState::Refreshing {
            refresh:
                AddressRefreshState::Backoff {
                    deadline,
                    timer_id: _,
                    ..
                },
            ..
        } = fixture.lane.lifecycle.state()
        else {
            panic!("retryable DNS failure must enter refresh backoff");
        };
        assert_eq!(fixture.lane.lifecycle.next_deadline(), Some(deadline));
        let early = Moment::from_nanos(deadline.as_nanos() - 1);
        assert!(
            !fixture
                .set
                .access(&mut fixture.lane)
                .fire_due_reconnect(early, &mut CausalSequence::new())
                .unwrap_or_else(|error| panic!("check early refresh retry: {error}"))
        );
        assert!(
            fixture
                .set
                .access(&mut fixture.lane)
                .fire_due_reconnect(deadline, &mut CausalSequence::new())
                .unwrap_or_else(|error| panic!("fire due refresh retry: {error}"))
        );
        assert!(fixture.lane.endpoint_refresh_needed());
        assert_eq!(fixture.lane.lifecycle.next_deadline(), None);
        assert!(
            fixture
                .set
                .has_local_work(std::slice::from_ref(&fixture.lane))
        );
    }
}

#[test]
fn unusable_answers_and_timer_exhaustion_close_with_distinct_policy_reasons() {
    let mut unusable = RefreshFixture::pending(71, 6);
    let unusable_refresh = unusable.take();
    let (call, request) = erased_request(
        CallId::from_raw(701),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    );
    unusable
        .set
        .access(&mut unusable.lane)
        .submit_request(request, START, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("queue request behind endpoint refresh: {error}"));
    assert!(call.try_result().is_none());
    unusable
        .set
        .access(&mut unusable.lane)
        .fail_endpoint_refresh(
            &unusable_refresh,
            DnsFailure::NoUsableAddress,
            START,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(|error| panic!("settle unusable DNS answer: {error}"));
    assert_closed(
        &unusable,
        BrokerCloseReason::EndpointResolutionFailed(DnsFailure::NoUsableAddress),
    );
    assert_eq!(
        call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::Closed,
            delivery: Delivery::NotSent,
        })))
    );
    assert_eq!(call.try_result(), Some(Err(CompletionError::Consumed)));

    let mut exhausted = RefreshFixture::pending(72, 7);
    let exhausted_refresh = exhausted.take();
    exhausted.lane.lifecycle.exhaust_timer_ids();
    exhausted
        .set
        .access(&mut exhausted.lane)
        .fail_endpoint_refresh(
            &exhausted_refresh,
            DnsFailure::Temporary,
            START,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(|error| panic!("settle refresh timer exhaustion: {error}"));
    assert_closed(&exhausted, BrokerCloseReason::RetryResourcesUnavailable);
}

#[test]
fn shutdown_clears_pending_resolving_and_backoff_refresh_ownership() {
    let mut pending = RefreshFixture::pending(81, 8);
    shutdown(&mut pending);

    let mut resolving = RefreshFixture::pending(82, 9);
    let _refresh = resolving.take();
    shutdown(&mut resolving);

    let mut backoff = RefreshFixture::pending(83, 10);
    let refresh = backoff.take();
    backoff
        .set
        .access(&mut backoff.lane)
        .fail_endpoint_refresh(
            &refresh,
            DnsFailure::Temporary,
            START,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(|error| panic!("schedule refresh before shutdown: {error}"));
    shutdown(&mut backoff);
}

fn shutdown(fixture: &mut RefreshFixture) {
    fixture
        .set
        .access(&mut fixture.lane)
        .begin_lifecycle_drain(START)
        .unwrap_or_else(|error| panic!("shutdown refresh state: {error}"));
    assert_closed(fixture, BrokerCloseReason::Requested);
    assert!(
        !fixture
            .set
            .access(&mut fixture.lane)
            .fire_due_reconnect(Moment::from_nanos(u64::MAX), &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("check endpoint refresh after shutdown: {error}"))
    );
}

fn assert_closed(fixture: &RefreshFixture, reason: BrokerCloseReason) {
    assert_eq!(
        fixture.lane.lifecycle.state(),
        BrokerState::Closed { reason }
    );
    assert!(fixture.lane.endpoint_refresh.is_none());
    assert!(fixture.lane.is_terminal());
    assert_eq!(fixture.lane.lifecycle.next_deadline(), None);
}
