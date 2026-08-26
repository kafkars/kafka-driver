//! Bootstrap membership rotation after one seed exhausts its resolved addresses.

use std::{
    io,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerCloseReason, BrokerEndpoint, BrokerState, CallFailure, CallId, ConnectionEpoch, Delivery,
    HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

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
use crate::{DriverLimits, RequestError, request::erased_request};

use super::{ClusterRuntime, SeedBootstrapState};
use crate::reactor::direct_plaintext::cluster_runtime::seed::ResolvedSeedReplacement;

pub(super) const NOW: Moment = Moment::from_nanos(1);

#[test]
fn requested_retirement_preserves_waiters_until_owned_replacement() {
    let mut runtime = runtime();
    let old = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install exhausted seed: {error}"));
    let mut causality = CausalSequence::new();
    let (first_call, first) = request(1);
    runtime
        .submit_seed(first, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain seed waiter: {error}"));

    assert!(
        runtime
            .prepare_seed_bootstrap_restart(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("retire exhausted seed: {error}"))
    );
    assert!(matches!(
        runtime.seed_bootstrap,
        SeedBootstrapState::RestartPending(slot) if slot.owner == old
    ));
    assert!(matches!(
        runtime
            .view(old)
            .unwrap_or_else(|| panic!("retired seed"))
            .lifecycle
            .state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::Requested
        }
    ));
    assert!(first_call.try_result().is_none());
    assert!(!runtime.seed_waiting_is_closed());
    assert!(!runtime.seed_replacement_blocked().unwrap_or(true));

    let ResolvedSeedReplacement::Busy(seed) = runtime
        .replace_resolved_seed(&FailedResolvedFactory, resolved_seed(2), NOW)
        .unwrap_or_else(|error| panic!("fence unowned replacement: {error}"))
    else {
        panic!("restart-pending replacement must retain raw evidence");
    };
    runtime
        .mark_seed_bootstrap_resolution_owned()
        .unwrap_or_else(|error| panic!("transfer bootstrap ownership: {error}"));
    assert!(matches!(
        runtime
            .replace_resolved_seed(&FailedResolvedFactory, resolved_seed(1), NOW)
            .unwrap_or_else(|error| panic!("ignore stale rotated seed: {error}")),
        ResolvedSeedReplacement::Stale
    ));
    assert!(matches!(
        runtime
            .replace_resolved_seed(&FailedResolvedFactory, *seed, NOW)
            .unwrap_or_else(|error| panic!("replace requested seed: {error}")),
        ResolvedSeedReplacement::Replaced
    ));

    assert_eq!(runtime.seed_bootstrap, SeedBootstrapState::Inactive);
    assert!(first_call.try_result().is_none());
    let (later_call, later) = request(2);
    runtime
        .submit_seed(later, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain post-replacement waiter: {error}"));
    assert!(later_call.try_result().is_none());
    assert!(!runtime.seed_waiting.is_empty());
}

#[test]
fn rotated_factory_failure_totalizes_external_waiters() {
    let mut runtime = runtime();
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install exhausted seed: {error}"));
    let mut causality = CausalSequence::new();
    let (call, request) = request(3);
    runtime
        .submit_seed(request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retain rotation waiter: {error}"));
    runtime
        .prepare_seed_bootstrap_restart(NOW, &mut causality)
        .unwrap_or_else(|error| panic!("retire exhausted seed: {error}"));
    runtime
        .mark_seed_bootstrap_resolution_owned()
        .unwrap_or_else(|error| panic!("transfer bootstrap ownership: {error}"));

    let error = runtime
        .replace_resolved_seed(&FatalFactory, resolved_seed(2), NOW)
        .err()
        .unwrap_or_else(|| panic!("factory failure must fail the host"));

    assert_eq!(error.to_string(), "synthetic rotated factory failure");
    assert_eq!(call.try_result(), Some(Ok(Err(closed()))));
    assert!(runtime.seed_waiting.is_empty());
    assert!(matches!(
        runtime.seed_bootstrap,
        SeedBootstrapState::ResolutionOwned(_)
    ));
}

#[test]
fn global_waiting_drain_remains_shutdown_only_and_blocks_replacement() {
    let mut runtime = runtime();
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install shutdown seed: {error}"));
    runtime
        .access(owner)
        .unwrap_or_else(|| panic!("shutdown seed access"))
        .begin_session_drain(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("retire shutdown seed: {error}"));
    runtime.begin_seed_waiting_drain();

    assert!(
        !runtime
            .prepare_seed_bootstrap_restart(NOW, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("shutdown must not rotate seed: {error}"))
    );

    assert!(matches!(
        runtime
            .replace_resolved_seed(&FailedResolvedFactory, resolved_seed(2), NOW)
            .unwrap_or_else(|error| panic!("observe shutdown replacement fence: {error}")),
        ResolvedSeedReplacement::Busy(_)
    ));
    let (call, request) = request(4);
    runtime
        .submit_seed(request, NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("reject post-shutdown waiter: {error}"));
    assert!(matches!(
        call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::Draining,
            delivery: Delivery::NotSent
        })))
    ));
}

pub(super) fn runtime() -> ClusterRuntime<TcpTransport> {
    ClusterRuntime::new(&DriverLimits::default())
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"))
}

pub(super) fn request(
    raw: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(raw),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    )
}

pub(super) fn failed_plan() -> BorneraLanePlan<TcpTransport> {
    let broker = BrokerLimits::default();
    BorneraLanePlan::new(
        crate::config::BrokerAddresses::Resolved {
            endpoint: endpoint(),
            addresses: addresses(),
        },
        broker,
        None,
        KafkaSessionPlan::new(None, broker),
        Box::new(RecoverableFailure),
    )
}

pub(super) fn resolved_seed(generation: u64) -> ResolvedSeed {
    ResolvedSeed::new(
        ConnectionEpoch::from_raw(generation),
        endpoint(),
        addresses(),
    )
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("next.kafka.test").unwrap_or_else(|error| panic!("next seed host: {error}")),
        NonZeroU16::MIN,
    )
}

fn addresses() -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            NonZeroU16::MIN,
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("next seed addresses: {error}"))
}

pub(super) fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}

struct RecoverableFailure;
struct FailedResolvedFactory;
struct FatalFactory;

impl BorneraLanePlanFactory<TcpTransport> for FailedResolvedFactory {
    fn at_resolved(
        &self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        let broker = BrokerLimits::default();
        Ok(BorneraLanePlan::new(
            crate::config::BrokerAddresses::Resolved {
                endpoint,
                addresses,
            },
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            Box::new(RecoverableFailure),
        ))
    }
}

impl BorneraLanePlanFactory<TcpTransport> for FatalFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        Err(io::Error::other("synthetic rotated factory failure"))
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
