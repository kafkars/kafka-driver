//! Cross-lane settlement totality when one token becomes stale during a shared turn.

use std::{net::SocketAddr, num::NonZeroUsize};

use bornera::{ConnectionToken, OwnerFailure};
use bornera_core::{ConnectionId, EndpointId, LaneId};
use bytes::BytesMut;
use calandria::{RetainedBytes, TimerOwnerId};
use kafka_driver_core::{CallFailure, CloseReason, Delivery, KafkaSessionPhase, TransportFailure};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{
    DriverLimits, RequestError,
    config::BrokerAddresses,
    reactor::{broker::BrokerLimits, causality::CausalSequence},
};

use super::shared_set_fixture_test::{NOW, request, response};
use super::{
    attempt::{BorneraLaneOwner, SimulatedAttempt, SimulatedTransport, SimulatedTransportHandle},
    lane_construction::start_lane,
    lane_plan::{BorneraLanePlan, KafkaSessionPlan},
    limits::DirectSetBounds,
    owner::DirectLane,
    set_owner::DirectSetOwner,
};

const PEER_CODE: i16 = 55;

#[test]
fn stale_lane_drain_does_not_short_circuit_peer_outcome_settlement() {
    let driver = DriverLimits::default();
    let mut connections = simulated_set(&driver);
    let controls = [
        SimulatedTransportHandle::default(),
        SimulatedTransportHandle::default(),
    ];
    let first = simulated_lane(&mut connections, &driver, controls[0].clone(), 1);
    let second = simulated_lane(&mut connections, &driver, controls[1].clone(), 2);
    let mut lanes = vec![first, second];
    let mut causality = CausalSequence::new();
    negotiate(&mut connections, &mut lanes, &controls, &mut causality);

    let (first_call, first_request) = request(505);
    let (second_call, second_request) = request(606);
    connections
        .access(&mut lanes[0])
        .submit_request(first_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit stale lane request: {error}"));
    connections
        .access(&mut lanes[1])
        .submit_request(second_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit peer lane request: {error}"));
    let correlations = capture_requests(&mut connections, &mut lanes, &controls, &mut causality);
    assert_eq!(lanes[0].contexts.snapshot().published(), 1);
    assert_eq!(lanes[1].contexts.snapshot().published(), 1);

    let first_connection = lanes[0].connection_for_test();
    let second_connection = lanes[1].connection_for_test();
    assert_eq!(pending_outcomes(&connections, second_connection), 0);
    assert!(controls[1].receive(&encoded_response(correlations[1], &response(PEER_CODE),)));
    assert_eq!(pending_outcomes(&connections, second_connection), 0);
    assert_eq!(first_call.try_result(), None);
    assert_eq!(second_call.try_result(), None);

    drop(
        connections
            .set
            .abandon(first_connection, OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("stale first shared token: {error}")),
    );
    let turns = connections.turns_for_test();
    let error = connections
        .drive(&mut lanes, NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("stale lane must fail the shared drive"));

    assert_eq!(connections.turns_for_test(), turns + 1);
    assert_eq!(
        error.to_string(),
        "stale Bornera connection violated direct ownership"
    );
    assert_eq!(
        first_call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: CloseReason::TransportLost(TransportFailure::Other),
            },
            delivery: Delivery::PossiblySent,
        })))
    );
    assert_eq!(second_call.try_result(), Some(Ok(Ok(response(PEER_CODE)))));
    assert_clean_semantics(&lanes);
    assert!(lanes[0].is_terminal());
    assert!(lanes[0].connection.is_none());
    assert!(!lanes[1].is_terminal());
    assert_eq!(lanes[1].connection, Some(second_connection));
    assert_clean_peer(&connections, second_connection);
}

fn simulated_set(driver: &DriverLimits) -> DirectSetOwner<SimulatedTransport> {
    let two = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);
    DirectSetOwner::new(driver, DirectSetBounds::new(two, two))
        .unwrap_or_else(|error| panic!("construct simulated shared set: {error}"))
}

fn simulated_lane(
    set: &mut DirectSetOwner<SimulatedTransport>,
    driver: &DriverLimits,
    control: SimulatedTransportHandle,
    id: u64,
) -> DirectLane<SimulatedTransport> {
    let broker = BrokerLimits::default();
    let port = u16::try_from(id).unwrap_or_else(|error| panic!("bound simulated port: {error}"));
    start_lane(
        set,
        driver,
        BorneraLanePlan::new(
            BrokerAddresses::Direct(SocketAddr::from(([127, 0, 0, 1], port))),
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            Box::new(SimulatedAttempt::new(driver, broker, control)),
        ),
        BorneraLaneOwner::new(
            EndpointId::new(id),
            LaneId::new(
                u32::try_from(id).unwrap_or_else(|error| panic!("bound simulated lane: {error}")),
            ),
            ConnectionId::new(id),
            TimerOwnerId::new(id),
        ),
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct simulated lane {id}: {error}"))
}

fn negotiate(
    connections: &mut DirectSetOwner<SimulatedTransport>,
    lanes: &mut [DirectLane<SimulatedTransport>],
    controls: &[SimulatedTransportHandle; 2],
    causality: &mut CausalSequence,
) {
    assert!(controls.iter().all(SimulatedTransportHandle::connect));
    let mut replied = [false; 2];
    for _ in 0..64 {
        drive(connections, lanes, causality);
        for (index, control) in controls.iter().enumerate() {
            if !replied[index] {
                if let Some(correlation) = take_correlation(control, "negotiation") {
                    assert!(
                        control.receive(&encoded_response(correlation, &negotiation_response(),))
                    );
                    replied[index] = true;
                }
            }
        }
        if lanes.iter().all(ready) {
            return;
        }
    }
    panic!("simulated shared lanes did not negotiate");
}

fn capture_requests(
    connections: &mut DirectSetOwner<SimulatedTransport>,
    lanes: &mut [DirectLane<SimulatedTransport>],
    controls: &[SimulatedTransportHandle; 2],
    causality: &mut CausalSequence,
) -> [i32; 2] {
    let mut correlations = [None; 2];
    for _ in 0..64 {
        drive(connections, lanes, causality);
        for (index, control) in controls.iter().enumerate() {
            correlations[index] =
                correlations[index].or_else(|| take_correlation(control, "public request"));
        }
        if let [Some(first), Some(second)] = correlations {
            return [first, second];
        }
    }
    panic!("simulated public requests were not written");
}

fn drive(
    connections: &mut DirectSetOwner<SimulatedTransport>,
    lanes: &mut [DirectLane<SimulatedTransport>],
    causality: &mut CausalSequence,
) {
    connections
        .drive(lanes, NOW, causality)
        .unwrap_or_else(|error| panic!("drive simulated shared set: {error}"));
}

fn ready(lane: &DirectLane<SimulatedTransport>) -> bool {
    lane.session.state().phase() == KafkaSessionPhase::Ready && lane.admission_open
}

fn take_correlation(control: &SimulatedTransportHandle, stage: &str) -> Option<i32> {
    let frames = control.take_frames();
    assert!(frames.len() <= 1, "multiple {stage} frames in one turn");
    frames.first().map(|frame| {
        i32::from_be_bytes(
            frame
                .get(8..12)
                .unwrap_or_else(|| panic!("{stage} frame has no correlation"))
                .try_into()
                .unwrap_or_else(|_| panic!("{stage} correlation must be four bytes")),
        )
    })
}

fn negotiation_response() -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    let mut api = AdvertisedApi::default();
    api.api_key = API_VERSIONS_API_DESCRIPTOR.api_key.value();
    api.min_version = 0;
    api.max_version = 0;
    response.api_keys.push(api);
    response
}

fn encoded_response(correlation: i32, response: &ApiVersionsResponse) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    header
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode simulated response header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode simulated response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("convert simulated response length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn pending_outcomes(
    connections: &DirectSetOwner<SimulatedTransport>,
    connection: ConnectionToken,
) -> usize {
    connections
        .set
        .connection_snapshot(connection)
        .unwrap_or_else(|error| panic!("snapshot simulated peer: {error}"))
        .pending_outcomes
}

fn assert_clean_semantics(lanes: &[DirectLane<SimulatedTransport>]) {
    for lane in lanes {
        let contexts = lane.contexts.snapshot();
        assert_eq!(contexts.reserved(), 0);
        assert_eq!(contexts.published(), 0);
        assert_eq!(contexts.retained_bytes(), RetainedBytes::ZERO);
        assert!(lane.pending.is_empty());
    }
}

fn assert_clean_peer(
    connections: &DirectSetOwner<SimulatedTransport>,
    connection: ConnectionToken,
) {
    let snapshot = connections.snapshot();
    assert_eq!(snapshot.connections.active(), 1);
    assert_eq!(snapshot.poller.registrations(), 1);
    let peer = connections
        .set
        .connection_snapshot(connection)
        .unwrap_or_else(|error| panic!("snapshot settled peer lane: {error}"));
    assert_eq!(peer.connection.reserved_permits, 0);
    assert_eq!(peer.connection.owned_operations, 0);
    assert_eq!(peer.connection.buffered_write_frames, 0);
    assert_eq!(
        peer.connection.buffered_write_retained_bytes,
        RetainedBytes::ZERO
    );
}
