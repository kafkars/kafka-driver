//! Direct-lane address selection, readiness preference, and refresh fencing.

use std::{
    io,
    net::{IpAddr, SocketAddr, TcpListener},
    sync::{Arc, Mutex},
};

use bornera::{ConnectionToken, OwnerFailure, TcpTransport};
use bornera_core::{ConnectionEpoch as BorneraEpoch, ConnectionId, EndpointId, LaneId};
use calandria::TimerOwnerId;
use kafka_driver_core::{
    AddressRefreshState, BrokerEndpoint, BrokerState, CloseReason, ConnectionEpoch, HostName,
    IpAddress, Moment, ResolutionLimits, ResolvedAddress, ResolvedAddressSet, TransportFailure,
};

use crate::{DriverLimits, config::BrokerAddresses};

use super::{
    attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt, PlaintextAttempt},
    lane_construction::start_lane,
    lane_plan::{BorneraLanePlan, KafkaSessionPlan},
    limits::DirectSetBounds,
    owner::{DirectLane, DirectPlaintextOwner, DirectSet},
    set_owner::DirectSetOwner,
};
use crate::reactor::{broker::BrokerLimits, causality::CausalSequence};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn resolved_synchronous_failures_exhaust_one_pass_then_publish_one_refresh_fence() {
    let first = SocketAddr::from(([127, 0, 0, 2], 9092));
    let second = SocketAddr::from(([127, 0, 0, 1], 9092));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (mut set, mut lane) = resolved_lane(
        [first, second],
        Box::new(RecordingFailure::new(Arc::clone(&seen))),
    );
    let deadline = backoff_deadline(&lane);

    set.access(&mut lane)
        .fire_due_reconnect(deadline, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("exhaust second resolved candidate: {error}"));

    assert_eq!(recorded(&seen), vec![first, second]);
    assert!(matches!(
        lane.lifecycle.state(),
        BrokerState::Refreshing {
            failed_epoch,
            next_epoch,
            refresh: AddressRefreshState::Pending { .. },
            ..
        } if failed_epoch == ConnectionEpoch::from_raw(2)
            && next_epoch == ConnectionEpoch::from_raw(3)
    ));
    assert!(lane.endpoint_refresh_needed());
    let refresh = lane
        .take_endpoint_refresh()
        .unwrap_or_else(|error| panic!("take endpoint refresh: {error}"))
        .unwrap_or_else(|| panic!("exhaustion must publish refresh ownership"));
    assert_eq!(refresh.endpoint(), &endpoint());
    assert_eq!(refresh.failed_epoch(), ConnectionEpoch::from_raw(2));
    assert!(!lane.endpoint_refresh_needed());
    assert_eq!(
        lane.take_endpoint_refresh()
            .unwrap_or_else(|error| panic!("reject duplicate refresh take: {error}")),
        None
    );
    set.access(&mut lane)
        .begin_lifecycle_drain(NOW)
        .unwrap_or_else(|error| panic!("stop suspended endpoint refresh: {error}"));
    assert!(lane.endpoint_refresh.is_none());
    assert!(lane.is_terminal());
    assert!(!lane.endpoint_refresh_needed());
    assert_eq!(
        lane.take_endpoint_refresh()
            .unwrap_or_else(|error| panic!("check refresh after shutdown: {error}")),
        None
    );
    assert!(
        !set.access(&mut lane)
            .fire_due_reconnect(Moment::from_nanos(u64::MAX), &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("check reconnect after refresh shutdown: {error}"))
    );
}

#[test]
fn live_failure_rotates_before_readiness_but_retries_the_ready_candidate() {
    let (unready_first, unready_second, unready_seen) = live_retry(false);
    assert_eq!(unready_seen, vec![unready_first, unready_second]);

    let (ready_first, _, ready_seen) = live_retry(true);
    assert_eq!(ready_seen, vec![ready_first, ready_first]);
}

#[test]
fn numeric_direct_retries_the_same_socket_without_requesting_refresh() {
    let address = SocketAddr::from(([127, 0, 0, 1], 9));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut owner = DirectPlaintextOwner::new_with_attempt(
        &DriverLimits::default(),
        address,
        None,
        Box::new(RecordingFailure::new(Arc::clone(&seen))),
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct numeric failure owner: {error}"));
    let deadline = backoff_deadline(&owner.lane);

    owner
        .access()
        .fire_due_reconnect(deadline, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("retry numeric direct socket: {error}"));

    assert_eq!(recorded(&seen), vec![address, address]);
    assert!(matches!(
        owner.lane.lifecycle.state(),
        BrokerState::Backoff { .. }
    ));
    assert!(!owner.lane.endpoint_refresh_needed());
}

fn live_retry(ready: bool) -> (SocketAddr, SocketAddr, Vec<SocketAddr>) {
    let first = listener();
    let second = listener();
    let first_address = local_address(&first);
    let second_address = local_address(&second);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let driver = DriverLimits::default();
    let attempt = RecordingPlaintext {
        seen: Arc::clone(&seen),
        delegate: PlaintextAttempt::new(&driver, BrokerLimits::default()),
    };
    let (mut set, mut lane) = resolved_lane([first_address, second_address], Box::new(attempt));
    if ready {
        set.access(&mut lane)
            .mark_generation_ready(ConnectionEpoch::from_raw(1))
            .unwrap_or_else(|error| panic!("mark resolved generation ready: {error}"));
    }
    detach(&mut set, &mut lane);
    let mut causality = CausalSequence::new();
    set.access(&mut lane)
        .settle_generation_lifecycle(
            ConnectionEpoch::from_raw(1),
            CloseReason::TransportLost(TransportFailure::Reset),
            NOW,
            &mut causality,
        )
        .unwrap_or_else(|error| panic!("settle resolved live failure: {error}"));
    let deadline = backoff_deadline(&lane);
    set.access(&mut lane)
        .fire_due_reconnect(deadline, &mut causality)
        .unwrap_or_else(|error| panic!("open resolved retry generation: {error}"));
    (first_address, second_address, recorded(&seen))
}

pub(super) fn resolved_lane(
    addresses: [SocketAddr; 2],
    attempt: Box<dyn DirectConnectionAttempt<TcpTransport>>,
) -> (DirectSetOwner<TcpTransport>, DirectLane<TcpTransport>) {
    let driver = DriverLimits::default();
    let broker = BrokerLimits::default();
    let mut set = DirectSetOwner::new(&driver, DirectSetBounds::direct())
        .unwrap_or_else(|error| panic!("construct resolved set: {error}"));
    let lane = start_lane(
        &mut set,
        &driver,
        BorneraLanePlan::new(
            BrokerAddresses::Resolved {
                endpoint: endpoint(),
                addresses: resolved(addresses),
            },
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            attempt,
        ),
        owner(),
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct resolved lane: {error}"));
    (set, lane)
}

fn detach(set: &mut DirectSetOwner<TcpTransport>, lane: &mut DirectLane<TcpTransport>) {
    let connection = lane.connection_for_test();
    drop(
        set.set
            .abandon(connection, OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("detach resolved generation: {error}")),
    );
    lane.connection = None;
}

fn backoff_deadline(lane: &DirectLane<TcpTransport>) -> Moment {
    let BrokerState::Backoff { deadline, .. } = lane.lifecycle.state() else {
        panic!("failed candidate must enter backoff");
    };
    deadline
}

struct RecordingFailure {
    seen: Arc<Mutex<Vec<SocketAddr>>>,
}

impl RecordingFailure {
    fn new(seen: Arc<Mutex<Vec<SocketAddr>>>) -> Self {
        Self { seen }
    }
}

impl DirectConnectionAttempt<TcpTransport> for RecordingFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: BorneraLaneOwner,
        address: SocketAddr,
        _epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        record(&self.seen, address);
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}

pub(super) struct RecordingPlaintext {
    pub(super) seen: Arc<Mutex<Vec<SocketAddr>>>,
    pub(super) delegate: PlaintextAttempt,
}

impl DirectConnectionAttempt<TcpTransport> for RecordingPlaintext {
    fn connect(
        &self,
        set: &mut DirectSet<TcpTransport>,
        owner: BorneraLaneOwner,
        address: SocketAddr,
        epoch: BorneraEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        record(&self.seen, address);
        self.delegate.connect(set, owner, address, epoch, now)
    }
}

fn record(seen: &Mutex<Vec<SocketAddr>>, address: SocketAddr) {
    seen.lock()
        .unwrap_or_else(|error| panic!("lock address recording: {error}"))
        .push(address);
}

pub(super) fn recorded(seen: &Mutex<Vec<SocketAddr>>) -> Vec<SocketAddr> {
    seen.lock()
        .unwrap_or_else(|error| panic!("read address recording: {error}"))
        .clone()
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        std::num::NonZeroU16::new(9092).unwrap_or_else(|| panic!("port is nonzero")),
    )
}

fn resolved(addresses: [SocketAddr; 2]) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        addresses.map(|address| {
            let IpAddr::V4(ip) = address.ip() else {
                panic!("test address must be IPv4");
            };
            ResolvedAddress::new(
                IpAddress::V4(ip.octets()),
                std::num::NonZeroU16::new(address.port())
                    .unwrap_or_else(|| panic!("test port is nonzero")),
            )
        }),
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid resolved addresses: {error}"))
}

fn owner() -> BorneraLaneOwner {
    BorneraLaneOwner::new(
        EndpointId::new(1),
        LaneId::new(1),
        ConnectionId::new(1),
        TimerOwnerId::new(1),
    )
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind candidate listener: {error}"))
}

fn local_address(listener: &TcpListener) -> SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read candidate address: {error}"))
}
