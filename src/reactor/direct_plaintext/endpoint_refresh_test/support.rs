//! Shared resolved-lane fixtures for endpoint-refresh policy tests.

use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::{ConnectionEpoch as BorneraEpoch, ConnectionId, EndpointId, LaneId};
use calandria::TimerOwnerId;
use kafka_driver_core::{
    BrokerEndpoint, BrokerState, HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress,
    ResolvedAddressSet,
};

use crate::{DriverLimits, config::BrokerAddresses};

use super::super::{
    attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
    backend::DirectBackend,
    endpoint_refresh::DirectEndpointRefresh,
    lane_construction::start_lane,
    lane_plan::{BorneraLanePlan, KafkaSessionPlan},
    limits::DirectSetBounds,
    owner::{DirectLane, DirectSet},
    runtime::DirectRuntime,
    set_owner::DirectSetOwner,
};
use crate::reactor::{broker::BrokerLimits, causality::CausalSequence};

pub(super) const START: Moment = Moment::from_nanos(1_000);

pub(super) struct RefreshFixture {
    pub(super) set: DirectSetOwner<TcpTransport>,
    pub(super) lane: DirectLane<TcpTransport>,
    pub(super) seen: Arc<Mutex<Vec<SocketAddr>>>,
}

impl RefreshFixture {
    pub(super) fn pending(endpoint_id: u64, lane_id: u32) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let driver = DriverLimits::default();
        let broker = BrokerLimits::default();
        let mut set = DirectSetOwner::new(&driver, DirectSetBounds::direct())
            .unwrap_or_else(|error| panic!("construct refresh set: {error}"));
        let lane = start_lane(
            &mut set,
            &driver,
            BorneraLanePlan::new(
                BrokerAddresses::Resolved {
                    endpoint: endpoint(),
                    addresses: addresses(old_addresses()),
                },
                broker,
                None,
                KafkaSessionPlan::new(None, broker),
                Box::new(RecordingFailure {
                    seen: Arc::clone(&seen),
                }),
            ),
            owner(endpoint_id, lane_id),
            START,
        )
        .unwrap_or_else(|error| panic!("construct refresh lane: {error}"));
        let mut fixture = Self { set, lane, seen };
        let first_deadline = reconnect_deadline(&fixture.lane);
        assert!(
            fixture
                .set
                .access(&mut fixture.lane)
                .fire_due_reconnect(first_deadline, &mut CausalSequence::new())
                .unwrap_or_else(|error| panic!("exhaust old address pass: {error}"))
        );
        assert!(fixture.lane.endpoint_refresh_needed());
        fixture
    }

    pub(super) fn take(&mut self) -> DirectEndpointRefresh {
        self.lane
            .take_endpoint_refresh()
            .unwrap_or_else(|error| panic!("take refresh fence: {error}"))
            .unwrap_or_else(|| panic!("refresh fence must be pending"))
    }

    pub(super) fn seen(&self) -> Vec<SocketAddr> {
        self.seen
            .lock()
            .unwrap_or_else(|error| panic!("read recorded addresses: {error}"))
            .clone()
    }
}

impl DirectBackend {
    pub(in crate::reactor) fn pending_plaintext_refresh_for_test(
        endpoint_id: u64,
        lane_id: u32,
    ) -> Self {
        let fixture = RefreshFixture::pending(endpoint_id, lane_id);
        Self::Plaintext(Box::new(DirectRuntime {
            connections: fixture.set,
            lane: fixture.lane,
        }))
    }

    pub(in crate::reactor) fn has_endpoint_refresh_for_test(&self) -> bool {
        match self {
            Self::Plaintext(runtime) => runtime.lane.endpoint_refresh.is_some(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(runtime) => runtime.lane.endpoint_refresh.is_some(),
            Self::Simulated(runtime) => runtime.lane.endpoint_refresh.is_some(),
        }
    }

    pub(in crate::reactor) fn broker_state_for_test(&self) -> BrokerState {
        match self {
            Self::Plaintext(runtime) => runtime.lane.lifecycle.state(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(runtime) => runtime.lane.lifecycle.state(),
            Self::Simulated(runtime) => runtime.lane.lifecycle.state(),
        }
    }
}

pub(super) fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid hostname: {error}")),
        std::num::NonZeroU16::new(9092).unwrap_or_else(|| panic!("port is nonzero")),
    )
}

pub(super) fn reconnect_deadline(lane: &DirectLane<TcpTransport>) -> Moment {
    match lane.lifecycle.state() {
        BrokerState::Backoff { deadline, .. } | BrokerState::Refreshing { deadline, .. } => {
            deadline
        }
        state => panic!("refresh fixture has no reconnect deadline: {state:?}"),
    }
}

pub(super) fn addresses<const N: usize>(sockets: [SocketAddr; N]) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        sockets.map(|address| {
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
    .unwrap_or_else(|error| panic!("valid resolved address set: {error}"))
}

pub(super) fn old_addresses() -> [SocketAddr; 2] {
    [
        SocketAddr::from(([127, 0, 0, 11], 9092)),
        SocketAddr::from(([127, 0, 0, 12], 9092)),
    ]
}

pub(super) fn new_addresses() -> [SocketAddr; 2] {
    [
        SocketAddr::from(([127, 0, 0, 21], 9092)),
        SocketAddr::from(([127, 0, 0, 22], 9092)),
    ]
}

struct RecordingFailure {
    seen: Arc<Mutex<Vec<SocketAddr>>>,
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
        self.seen
            .lock()
            .unwrap_or_else(|error| panic!("record refresh address: {error}"))
            .push(address);
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}

fn owner(endpoint: u64, lane: u32) -> BorneraLaneOwner {
    BorneraLaneOwner::new(
        EndpointId::new(endpoint),
        LaneId::new(lane),
        ConnectionId::new(endpoint),
        TimerOwnerId::new(endpoint),
    )
}
