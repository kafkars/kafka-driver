//! Sparse removal and corrupted family-state proofs.

use std::{io, net::SocketAddr, num::NonZeroU16};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerEndpoint, BrokerId, HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress,
    ResolvedAddressSet,
};

use crate::{TrafficClass, reactor::causality::CausalSequence};

use super::ClusterRuntime;
use crate::reactor::direct_plaintext::{
    attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
    endpoint_refresh::DirectRefreshOwner,
    lane_plan::{BorneraLanePlan, KafkaSessionPlan, factory::BorneraLanePlanFactory},
    owner::DirectSet,
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn sparse_removal_preflights_and_repairs_peer_indexes() {
    let mut runtime = runtime();
    let first = activate(&mut runtime, broker(7), TrafficClass::Control)
        .unwrap_or_else(|error| panic!("activate first: {error}"));
    let second = activate(&mut runtime, broker(7), TrafficClass::LongPoll)
        .unwrap_or_else(|error| panic!("activate second: {error}"));
    let retained = activate(&mut runtime, broker(8), TrafficClass::Bulk)
        .unwrap_or_else(|error| panic!("activate retained: {error}"));
    make_reclaimable(&mut runtime, first);
    assert!(
        !runtime
            .remove_terminal_family(broker(7))
            .unwrap_or_else(|error| panic!("defer removal: {error}"))
    );
    make_reclaimable(&mut runtime, second);
    assert!(
        runtime
            .remove_terminal_family(broker(7))
            .unwrap_or_else(|error| panic!("remove family: {error}"))
    );
    assert!(runtime.view(retained).is_some());
    assert!(runtime.view(first).is_none());
    assert!(runtime.view(second).is_none());
}

#[test]
fn individual_reserved_family_owner_cannot_be_removed() {
    let mut runtime = runtime();
    activate(&mut runtime, broker(7), TrafficClass::Control)
        .unwrap_or_else(|error| panic!("activate family: {error}"));
    let dormant = runtime
        .family_owner(broker(7), TrafficClass::Interactive)
        .unwrap_or_else(|| panic!("reserved dormant owner"));
    assert_eq!(
        runtime
            .remove_terminal(dormant)
            .err()
            .unwrap_or_else(|| panic!("individual removal must fail"))
            .to_string(),
        "Bornera broker-family lanes must be removed together"
    );
}

#[test]
fn missing_active_slot_blocks_activation_and_removal() {
    let mut runtime = runtime();
    let owner = activate(&mut runtime, broker(7), TrafficClass::Control)
        .unwrap_or_else(|error| panic!("activate family: {error}"));
    runtime.slots.remove(&owner);
    let factory = Factory;
    assert_diverged(runtime.activate_resolved_lane(
        broker(7),
        TrafficClass::Control,
        &factory,
        endpoint(),
        addresses(),
        NOW,
    ));
    assert_diverged(runtime.remove_terminal_family(broker(7)));
    assert_eq!(runtime.lanes.len(), 1);
}

#[test]
fn unexpected_dormant_registration_blocks_activation_and_removal() {
    let mut runtime = runtime();
    activate(&mut runtime, broker(7), TrafficClass::Control)
        .unwrap_or_else(|error| panic!("activate family: {error}"));
    let dormant = runtime
        .family_owner(broker(7), TrafficClass::Bulk)
        .unwrap_or_else(|| panic!("reserved dormant owner"));
    runtime.slots.insert(dormant, 0);
    let factory = Factory;
    assert_diverged(runtime.activate_resolved_lane(
        broker(7),
        TrafficClass::Bulk,
        &factory,
        endpoint(),
        addresses(),
        NOW,
    ));
    assert_diverged(runtime.remove_terminal_family(broker(7)));
    assert_eq!(runtime.lanes.len(), 1);
}

fn assert_diverged<T>(result: io::Result<T>) {
    assert_eq!(
        result
            .err()
            .unwrap_or_else(|| panic!("operation must report divergence"))
            .to_string(),
        "Bornera broker family lane state diverged"
    );
}

fn activate(
    runtime: &mut ClusterRuntime<TcpTransport>,
    broker: BrokerId,
    traffic: TrafficClass,
) -> io::Result<DirectRefreshOwner> {
    runtime.activate_resolved_lane(broker, traffic, &Factory, endpoint(), addresses(), NOW)
}

fn runtime() -> ClusterRuntime<TcpTransport> {
    ClusterRuntime::new(&crate::DriverLimits::default())
        .unwrap_or_else(|error| panic!("runtime: {error}"))
}

struct Factory;

impl BorneraLanePlanFactory<TcpTransport> for Factory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        let broker = crate::reactor::broker::BrokerLimits::default();
        Ok(BorneraLanePlan::new(
            crate::config::BrokerAddresses::Direct(SocketAddr::from(([127, 0, 0, 1], 9))),
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            Box::new(RecoverableFailure),
        ))
    }
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

fn make_reclaimable(runtime: &mut ClusterRuntime<TcpTransport>, owner: DirectRefreshOwner) {
    runtime
        .access(owner)
        .unwrap_or_else(|| panic!("lane access must exist"))
        .begin_session_drain(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("drain lane: {error}"));
}

fn broker(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker: {error}"))
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        NonZeroU16::new(9092).unwrap_or(NonZeroU16::MIN),
    )
}

fn addresses() -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            NonZeroU16::new(9092).unwrap_or(NonZeroU16::MIN),
        )],
        ResolutionLimits::new(std::num::NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid addresses: {error}"))
}
