//! Host-level Direct DNS ownership, backpressure, completion, and shutdown scenarios.

use std::{
    error::Error as _,
    net::{IpAddr, SocketAddr, TcpListener},
    num::{NonZeroU16, NonZeroUsize},
    sync::{Arc, mpsc},
};

use kafka_driver_core::{
    AddressRefreshState, BootstrapSet, BrokerEndpoint, BrokerState, ConnectionEpoch, DnsFailure,
    DnsOutcome, DnsRequest, HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress,
    ResolvedAddressSet,
};

use crate::{
    BootstrapLimits, DriverLimits, ResolverLimits,
    api::CallIds,
    config::{BootstrapConfig, DirectBrokerConfig, DriverTarget},
    observation::Observation,
    reactor::{ReactorBackend, direct_plaintext::DirectBackend},
};

use super::{NameResolution, Reactor};

const NOW: Moment = Moment::from_nanos(1_000);

#[test]
fn resolver_ownership_saturation_leaves_the_direct_fence_pending() {
    let limits = limits(1);
    let (mut reactor, requests, _outcomes) = fixture(&limits);
    let expected = direct(&reactor)
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending Direct refresh owner"));

    let turn = reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("observe resolver saturation: {error}"));

    assert!(!turn.made_progress());
    assert_eq!(
        direct(&reactor).pending_endpoint_refresh_owner(),
        Some(expected)
    );
    let _bootstrap = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("initial bootstrap request: {error}"));
    assert!(requests.try_recv().is_err());
    assert_eq!(reactor.backend.selector_count(), 1);
}

#[test]
fn full_worker_queue_retains_one_direct_request_until_capacity_returns() {
    let limits = limits(2);
    let (mut reactor, requests, _outcomes) = fixture(&limits);
    let expected = direct(&reactor)
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending Direct refresh owner"));

    let first = reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("retain Direct DNS request: {error}"));

    assert!(first.made_progress());
    assert_eq!(direct(&reactor).pending_endpoint_refresh_owner(), None);
    let _bootstrap = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("initial bootstrap request: {error}"));
    let second = reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("retry retained Direct DNS request: {error}"));
    assert!(second.made_progress());
    let direct_request = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("Direct DNS request: {error}"));
    assert_eq!(direct_request.epoch(), ConnectionEpoch::from_raw(2));
    assert_eq!(direct_request.endpoint(), &resolved_endpoint());
    assert_eq!(expected.endpoint().get(), 51);
    assert_eq!(expected.lane().get(), 7);
    reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("observe in-flight Direct DNS: {error}"));
    assert!(requests.try_recv().is_err());
}

#[test]
fn matching_success_and_duplicate_outcome_settle_the_direct_lane_once() {
    let limits = limits(2);
    let (mut reactor, requests, outcomes) = fixture(&limits);
    let request = submit_direct(&mut reactor, &requests);
    let success = DnsOutcome::new(request.epoch(), request.effect_id(), Ok(addresses()));
    outcomes
        .send(success.clone())
        .unwrap_or_else(|error| panic!("queue Direct DNS success: {error}"));
    outcomes
        .send(success)
        .unwrap_or_else(|error| panic!("queue duplicate Direct DNS success: {error}"));

    let turn = reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("complete Direct DNS success: {error}"));

    assert!(turn.made_progress());
    assert!(!direct(&reactor).has_endpoint_refresh_for_test());
    assert!(matches!(
        direct(&reactor).broker_state_for_test(),
        BrokerState::Backoff { .. }
    ));
    assert!(!direct(&reactor).is_terminal());
}

#[test]
fn matching_temporary_failure_retains_one_backoff_fence() {
    let limits = limits(2);
    let (mut reactor, requests, outcomes) = fixture(&limits);
    let request = submit_direct(&mut reactor, &requests);
    outcomes
        .send(DnsOutcome::new(
            request.epoch(),
            request.effect_id(),
            Err(DnsFailure::Temporary),
        ))
        .unwrap_or_else(|error| panic!("queue Direct DNS failure: {error}"));

    reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("complete Direct DNS failure: {error}"));

    assert!(direct(&reactor).has_endpoint_refresh_for_test());
    assert!(matches!(
        direct(&reactor).broker_state_for_test(),
        BrokerState::Refreshing {
            refresh: AddressRefreshState::Backoff { .. },
            ..
        }
    ));
}

#[test]
fn matching_effect_with_wrong_epoch_terminalizes_the_direct_lane() {
    let limits = limits(2);
    let (mut reactor, requests, outcomes) = fixture(&limits);
    let request = submit_direct(&mut reactor, &requests);
    outcomes
        .send(DnsOutcome::new(
            ConnectionEpoch::from_raw(request.epoch().get() + 1),
            request.effect_id(),
            Err(DnsFailure::Temporary),
        ))
        .unwrap_or_else(|error| panic!("queue wrong-epoch Direct DNS: {error}"));

    let error = match reactor.continue_resolution(NOW) {
        Ok(_) => panic!("wrong Direct DNS epoch must fail"),
        Err(error) => error,
    };

    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("direct endpoint-refresh outcome epoch diverged")
    );
    assert!(direct(&reactor).is_terminal());
    assert!(!direct(&reactor).has_endpoint_refresh_for_test());
}

#[test]
fn closed_resolver_restores_the_exact_pending_direct_fence() {
    let limits = limits(2);
    let (mut reactor, requests, _outcomes) = fixture(&limits);
    let expected = direct(&reactor)
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending Direct refresh owner"));
    drop(requests);

    let error = match reactor.continue_resolution(NOW) {
        Ok(_) => panic!("closed resolver must fail the host turn"),
        Err(error) => error,
    };

    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("resolver worker is closed")
    );
    assert_eq!(
        direct(&reactor).pending_endpoint_refresh_owner(),
        Some(expected)
    );
    assert!(!direct(&reactor).is_terminal());
}

#[test]
fn shutdown_discards_owned_direct_dns_before_closing_the_lane() {
    let limits = limits(2);
    let (mut reactor, requests, outcomes) = fixture(&limits);
    let _request = submit_direct(&mut reactor, &requests);

    reactor
        .begin_implicit_shutdown(NOW)
        .unwrap_or_else(|error| panic!("begin Direct DNS shutdown: {error}"));

    assert!(reactor.resolution.is_none());
    assert!(reactor.resolver_shutdown.is_some());
    assert!(direct(&reactor).is_terminal());
    assert!(!direct(&reactor).has_endpoint_refresh_for_test());
    assert!(
        outcomes
            .send(DnsOutcome::new(
                ConnectionEpoch::from_raw(2),
                kafka_driver_core::EffectId::from_raw(2),
                Err(DnsFailure::Temporary),
            ))
            .is_err()
    );
}

fn fixture(
    limits: &DriverLimits,
) -> (
    Reactor,
    mpsc::Receiver<DnsRequest>,
    mpsc::SyncSender<DnsOutcome>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind Direct construction target: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read Direct construction target: {error}"));
    let target = DriverTarget::Direct(DirectBrokerConfig::plaintext(address));
    let (_, _, mut reactor) = Reactor::new(
        limits,
        target,
        Arc::new(CallIds::new()),
        Arc::new(Observation::default()),
    )
    .unwrap_or_else(|error| panic!("construct Direct host: {error}"));
    reactor.backend = ReactorBackend::Direct(Box::new(
        DirectBackend::pending_plaintext_refresh_for_test(51, 7),
    ));
    let (resolution, requests, outcomes) = NameResolution::isolated(bootstrap(), limits.resolver());
    reactor.resolution = Some(resolution);
    (reactor, requests, outcomes)
}

fn submit_direct(reactor: &mut Reactor, requests: &mpsc::Receiver<DnsRequest>) -> DnsRequest {
    reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("retain Direct DNS request: {error}"));
    let _bootstrap = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("initial bootstrap request: {error}"));
    reactor
        .continue_resolution(NOW)
        .unwrap_or_else(|error| panic!("submit retained Direct DNS request: {error}"));
    requests
        .try_recv()
        .unwrap_or_else(|error| panic!("Direct DNS request: {error}"))
}

fn direct(reactor: &Reactor) -> &DirectBackend {
    reactor
        .backend
        .direct()
        .unwrap_or_else(|| panic!("test requires Direct backend"))
}

fn limits(pending_capacity: usize) -> DriverLimits {
    let one = NonZeroUsize::MIN;
    DriverLimits::default().with_resolver_limits(
        ResolverLimits::new(one, nonzero(2), nonzero(2), nonzero(2))
            .with_pending_capacity(nonzero(pending_capacity)),
    )
}

fn bootstrap() -> BootstrapConfig {
    let endpoints = BootstrapSet::try_from_iter([bootstrap_endpoint()], BootstrapLimits::default())
        .unwrap_or_else(|error| panic!("valid bootstrap set: {error}"));
    BootstrapConfig::plaintext(endpoints)
}

fn bootstrap_endpoint() -> BrokerEndpoint {
    endpoint("bootstrap.test")
}

fn resolved_endpoint() -> BrokerEndpoint {
    endpoint("broker.test")
}

fn endpoint(host: &str) -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new(host).unwrap_or_else(|error| panic!("valid test hostname: {error}")),
        NonZeroU16::new(9092).unwrap_or_else(|| panic!("port is nonzero")),
    )
}

fn addresses() -> ResolvedAddressSet {
    let address = SocketAddr::from(([127, 0, 0, 21], 9092));
    let IpAddr::V4(ip) = address.ip() else {
        panic!("test address must be IPv4");
    };
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4(ip.octets()),
            NonZeroU16::new(address.port()).unwrap_or_else(|| panic!("port is nonzero")),
        )],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid resolved addresses: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
