//! Sparse broker-family activation and stable owner-order proofs.

use std::{
    cell::Cell,
    io,
    net::{SocketAddr, TcpListener},
    num::{NonZeroU16, NonZeroUsize},
};

use bornera::TcpTransport;
use kafka_driver_core::{
    BrokerEndpoint, BrokerId, HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress,
    ResolvedAddressSet,
};

use crate::{DriverLimits, TrafficClass};

use super::ClusterRuntime;
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        attempt::PlaintextAttempt,
        lane_plan::{BorneraLanePlan, KafkaSessionPlan, factory::BorneraLanePlanFactory},
    },
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn long_poll_first_reserves_four_owners_but_opens_one_physical_lane() {
    let listener = listener();
    let factory = LiveFactory::new(address(&listener));
    let mut runtime = runtime();
    let broker = broker_id(7);
    let endpoint = endpoint("broker.test", 9092);

    let long_poll = runtime
        .activate_resolved_lane(
            broker,
            TrafficClass::LongPoll,
            &factory,
            endpoint,
            addresses(9092),
            NOW,
        )
        .unwrap_or_else(|error| panic!("activate long-poll lane: {error}"));

    assert_eq!(long_poll.lane().get(), 4);
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.lanes.len(), 1);
    assert_eq!(runtime.slots.len(), 1);
    for (offset, traffic) in TrafficClass::ALL.into_iter().enumerate() {
        let owner = runtime
            .family_owner(broker, traffic)
            .unwrap_or_else(|| panic!("reserved family owner"));
        assert_eq!(owner.lane().get() as usize, offset + 1);
        assert_eq!(
            runtime.view(owner).is_some(),
            traffic == TrafficClass::LongPoll
        );
    }
    assert_eq!(runtime.connections.snapshot().connections.active(), 1);
    assert_eq!(runtime.connections.snapshot().poller.registrations(), 1);
    assert_exact_connections(&listener, 1);
}

#[test]
fn dormant_sibling_activates_out_of_order_and_active_repeat_is_a_noop() {
    let listener = listener();
    let factory = LiveFactory::new(address(&listener));
    let mut runtime = runtime();
    let broker = broker_id(7);
    let endpoint = endpoint("broker.test", 9092);

    let long_poll = activate(
        &mut runtime,
        broker,
        TrafficClass::LongPoll,
        &factory,
        endpoint.clone(),
    );
    let control = activate(
        &mut runtime,
        broker,
        TrafficClass::Control,
        &factory,
        endpoint.clone(),
    );
    let repeated = activate(
        &mut runtime,
        broker,
        TrafficClass::LongPoll,
        &factory,
        endpoint,
    );

    assert_eq!(repeated, long_poll);
    assert_eq!(control.lane().get(), 1);
    assert_eq!(long_poll.lane().get(), 4);
    assert_eq!(factory.attempts.get(), 2);
    assert_eq!(runtime.lanes.len(), 2);
    assert_eq!(runtime.lanes[0].refresh_owner(), long_poll);
    assert_eq!(runtime.lanes[1].refresh_owner(), control);
    assert_eq!(runtime.connections.snapshot().connections.active(), 2);
    assert_exact_connections(&listener, 2);
}

#[test]
fn changed_endpoint_is_rejected_before_factory_or_sibling_activation() {
    let listener = listener();
    let factory = LiveFactory::new(address(&listener));
    let mut runtime = runtime();
    let broker = broker_id(7);
    let original = endpoint("broker.test", 9092);
    activate(
        &mut runtime,
        broker,
        TrafficClass::Control,
        &factory,
        original,
    );

    for traffic in [TrafficClass::Control, TrafficClass::LongPoll] {
        let error = runtime
            .activate_resolved_lane(
                broker,
                traffic,
                &factory,
                endpoint("replacement.test", 9093),
                addresses(9093),
                NOW,
            )
            .err()
            .unwrap_or_else(|| panic!("changed endpoint must require replacement"));
        assert_eq!(
            error.to_string(),
            "Bornera broker family endpoint changed before replacement"
        );
    }
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.lanes.len(), 1);
    let dormant = runtime
        .family_owner(broker, TrafficClass::LongPoll)
        .unwrap_or_else(|| panic!("reserved dormant owner"));
    assert!(runtime.view(dormant).is_none());
    assert_exact_connections(&listener, 1);
}

#[test]
fn retiring_family_rejects_same_endpoint_reactivation() {
    let listener = listener();
    let factory = LiveFactory::new(address(&listener));
    let mut runtime = runtime();
    let broker = broker_id(7);
    let endpoint = endpoint("broker.test", 9092);
    activate(
        &mut runtime,
        broker,
        TrafficClass::Control,
        &factory,
        endpoint.clone(),
    );
    assert!(
        runtime
            .families
            .get_mut(&broker)
            .unwrap_or_else(|| panic!("family"))
            .begin_retirement()
    );

    for traffic in [TrafficClass::Control, TrafficClass::LongPoll] {
        let error = runtime
            .activate_resolved_lane(
                broker,
                traffic,
                &factory,
                endpoint.clone(),
                addresses(9092),
                NOW,
            )
            .err()
            .unwrap_or_else(|| panic!("retiring family must reject activation"));
        assert_eq!(
            error.to_string(),
            "Bornera broker family endpoint changed before replacement"
        );
    }
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.lanes.len(), 1);
    assert_exact_connections(&listener, 1);
}

fn activate(
    runtime: &mut ClusterRuntime<TcpTransport>,
    broker: BrokerId,
    traffic: TrafficClass,
    factory: &LiveFactory,
    endpoint: BrokerEndpoint,
) -> super::DirectRefreshOwner {
    runtime
        .activate_resolved_lane(broker, traffic, factory, endpoint, addresses(9092), NOW)
        .unwrap_or_else(|error| panic!("activate family lane: {error}"))
}

fn runtime() -> ClusterRuntime<TcpTransport> {
    ClusterRuntime::new(&DriverLimits::default())
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"))
}

struct LiveFactory {
    attempts: Cell<usize>,
    address: SocketAddr,
}

impl LiveFactory {
    const fn new(address: SocketAddr) -> Self {
        Self {
            attempts: Cell::new(0),
            address,
        }
    }
}

impl BorneraLanePlanFactory<TcpTransport> for LiveFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        self.attempts.set(self.attempts.get() + 1);
        let driver = DriverLimits::default();
        let broker = BrokerLimits::default();
        Ok(BorneraLanePlan::new(
            crate::config::BrokerAddresses::Direct(self.address),
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            Box::new(PlaintextAttempt::new(&driver, broker)),
        ))
    }
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind broker: {error}"))
}

fn address(listener: &TcpListener) -> SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("broker address: {error}"))
}

fn assert_exact_connections(listener: &TcpListener, expected: usize) {
    for _connection in 0..expected {
        let _peer = listener
            .accept()
            .unwrap_or_else(|error| panic!("accept expected broker connection: {error}"));
    }
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make broker listener nonblocking: {error}"));
    let error = listener
        .accept()
        .err()
        .unwrap_or_else(|| panic!("unexpected eager broker connection"));
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}

fn broker_id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn endpoint(host: &str, port: u16) -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}")),
        NonZeroU16::new(port).unwrap_or(NonZeroU16::MIN),
    )
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            NonZeroU16::new(port).unwrap_or(NonZeroU16::MIN),
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid address set: {error}"))
}
