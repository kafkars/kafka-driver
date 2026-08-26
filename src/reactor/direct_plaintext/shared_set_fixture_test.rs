//! Loopback construction and wire helpers for shared-set Kafka proofs.

use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    num::NonZeroUsize,
    sync::mpsc,
    thread,
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::{ConnectionEpoch, ConnectionId, EndpointId, LaneId};
use bytes::BytesMut;
use calandria::{Span, TimerOwnerId};
use kafka_driver_core::{CallId, KafkaSessionPhase, Moment};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{
    DriverLimits, RequestError,
    reactor::{broker::BrokerLimits, causality::CausalSequence},
    request::{ErasedRequest, erased_request},
};

use super::{
    attempt::{
        DirectConnectError, DirectConnectionAttempt, DirectConnectionOwner, PlaintextAttempt,
    },
    lane_construction::start_lane,
    limits::DirectSetBounds,
    owner::{DirectLane, DirectSet},
    session_plan::DirectSessionPlan,
    set_owner::DirectSetOwner,
};

pub(super) const NOW: Moment = Moment::from_nanos(1);
pub(super) type NegotiationGate = (mpsc::SyncSender<()>, mpsc::Receiver<()>);

pub(super) struct ResponseControl {
    pub(super) request_seen: mpsc::Receiver<()>,
    pub(super) release_response: mpsc::SyncSender<()>,
    pub(super) response_written: mpsc::Receiver<()>,
    pub(super) finish: mpsc::SyncSender<()>,
}

pub(super) fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind shared broker: {error}"))
}

pub(super) fn address(listener: &TcpListener) -> SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read shared broker address: {error}"))
}

pub(super) fn spawn_lane(
    listener: TcpListener,
    gate: Option<NegotiationGate>,
    public_error_code: i16,
) -> thread::JoinHandle<()> {
    thread::spawn(move || serve_lane(&listener, gate, public_error_code))
}

pub(super) fn spawn_controlled_lane(
    listener: TcpListener,
    public_error_code: i16,
) -> (ResponseControl, thread::JoinHandle<()>) {
    let (request_seen, await_request) = mpsc::sync_channel(1);
    let (release_response, response_release) = mpsc::sync_channel(1);
    let (response_written, await_response) = mpsc::sync_channel(1);
    let (finish, await_finish) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        serve_controlled_lane(
            &listener,
            public_error_code,
            &request_seen,
            &response_release,
            &response_written,
            &await_finish,
        );
    });
    (
        ResponseControl {
            request_seen: await_request,
            release_response,
            response_written: await_response,
            finish,
        },
        server,
    )
}

pub(super) fn shared_set(driver: &DriverLimits) -> DirectSetOwner<TcpTransport> {
    let two = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);
    DirectSetOwner::new(driver, DirectSetBounds::new(two, two))
        .unwrap_or_else(|error| panic!("construct shared direct set: {error}"))
}

pub(super) fn plaintext_lane(
    set: &mut DirectSetOwner<TcpTransport>,
    driver: &DriverLimits,
    address: SocketAddr,
    id: u64,
) -> DirectLane<TcpTransport> {
    test_lane(
        set,
        driver,
        address,
        id,
        Box::new(PlaintextAttempt::new(driver, BrokerLimits::default())),
    )
}

pub(super) fn failed_lane(
    set: &mut DirectSetOwner<TcpTransport>,
    driver: &DriverLimits,
    id: u64,
) -> DirectLane<TcpTransport> {
    test_lane(
        set,
        driver,
        SocketAddr::from(([127, 0, 0, 1], 9)),
        id,
        Box::new(ImmediateEndpointFailure),
    )
}

fn test_lane(
    set: &mut DirectSetOwner<TcpTransport>,
    driver: &DriverLimits,
    address: SocketAddr,
    id: u64,
    attempt: Box<dyn DirectConnectionAttempt<TcpTransport>>,
) -> DirectLane<TcpTransport> {
    let broker = BrokerLimits::default();
    start_lane(
        set,
        driver,
        broker,
        crate::config::BrokerAddresses::Direct(address),
        None,
        DirectSessionPlan::new(None, broker),
        attempt,
        DirectConnectionOwner::new(
            EndpointId::new(id),
            LaneId::new(
                u32::try_from(id).unwrap_or_else(|error| panic!("bound shared lane id: {error}")),
            ),
            ConnectionId::new(id),
            TimerOwnerId::new(id),
        ),
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct shared lane {id}: {error}"))
}

pub(super) fn ready(lane: &DirectLane<TcpTransport>) -> bool {
    lane.session.state().phase() == KafkaSessionPhase::Ready && lane.admission_open
}

pub(super) fn drive(
    set: &mut DirectSetOwner<TcpTransport>,
    lanes: &mut [DirectLane<TcpTransport>],
    causality: &mut CausalSequence,
) {
    set.drive(lanes, NOW, causality)
        .unwrap_or_else(|error| panic!("drive shared set: {error}"));
}

pub(super) fn wait_if_idle(
    set: &mut DirectSetOwner<TcpTransport>,
    lanes: &mut [DirectLane<TcpTransport>],
) {
    if set.has_local_work(lanes) {
        return;
    }
    let maximum = Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO);
    set.wait(lanes, maximum)
        .unwrap_or_else(|error| panic!("wait on shared set: {error}"));
}

pub(super) fn request(
    id: u64,
) -> (
    crate::Call<Result<ApiVersionsResponse, RequestError>>,
    Box<dyn ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(id),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    )
}

pub(super) fn response(error_code: i16) -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.error_code = error_code;
    response
}

struct ImmediateEndpointFailure;

impl DirectConnectionAttempt<TcpTransport> for ImmediateEndpointFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: DirectConnectionOwner,
        _address: SocketAddr,
        _epoch: ConnectionEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}

fn serve_lane(listener: &TcpListener, gate: Option<NegotiationGate>, public_error_code: i16) {
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept shared broker: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound shared broker read: {error}"));
    let negotiation = read_correlation(&mut peer);
    if let Some((seen, release)) = gate {
        seen.send(())
            .unwrap_or_else(|error| panic!("publish held negotiation: {error}"));
        release
            .recv()
            .unwrap_or_else(|error| panic!("await held negotiation release: {error}"));
    }
    write_response(&mut peer, negotiation, &negotiation_response());
    let public = read_correlation(&mut peer);
    write_response(&mut peer, public, &response(public_error_code));
}

fn serve_controlled_lane(
    listener: &TcpListener,
    public_error_code: i16,
    request_seen: &mpsc::SyncSender<()>,
    release_response: &mpsc::Receiver<()>,
    response_written: &mpsc::SyncSender<()>,
    finish: &mpsc::Receiver<()>,
) {
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept controlled shared broker: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound controlled shared broker read: {error}"));
    let negotiation = read_correlation(&mut peer);
    write_response(&mut peer, negotiation, &negotiation_response());
    let public = read_correlation(&mut peer);
    request_seen
        .send(())
        .unwrap_or_else(|error| panic!("publish controlled shared request: {error}"));
    release_response
        .recv()
        .unwrap_or_else(|error| panic!("release controlled shared response: {error}"));
    write_response(&mut peer, public, &response(public_error_code));
    response_written
        .send(())
        .unwrap_or_else(|error| panic!("publish controlled shared response: {error}"));
    finish
        .recv()
        .unwrap_or_else(|error| panic!("finish controlled shared broker: {error}"));
}

fn read_correlation(peer: &mut TcpStream) -> i32 {
    let mut prefix = [0; 4];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read shared frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("convert shared frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read shared frame body: {error}"));
    i32::from_be_bytes(
        body.get(4..8)
            .unwrap_or_else(|| panic!("shared request correlation is missing"))
            .try_into()
            .unwrap_or_else(|_| panic!("shared correlation must be four bytes")),
    )
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

fn write_response(peer: &mut TcpStream, correlation: i32, response: &ApiVersionsResponse) {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    header
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode shared response header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode shared response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("convert shared response length: {error}"));
    peer.write_all(&length.to_be_bytes())
        .and_then(|()| peer.write_all(&body))
        .unwrap_or_else(|error| panic!("write shared response: {error}"));
}
