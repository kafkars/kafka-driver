//! Loopback proofs for the direct numeric plaintext connection owner.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

use bornera::ConnectionEvent;
use bytes::BytesMut;
use calandria::Span;
use kafka_driver_core::{
    BrokerPhase, BrokerState, CallFailure, CallId, Delivery, KafkaSessionPhase, Moment,
};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{DriverLimits, RequestError, request::erased_request};

use super::{backend::DirectBackend, owner::DirectPlaintextOwner};
use crate::reactor::{ReactorBackend, causality::CausalSequence};

#[test]
fn loopback_session_round_trip_releases_every_semantic_context() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind private direct broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read private direct address: {error}"));
    let server = thread::spawn(move || serve_one(&listener));
    let now = Moment::from_nanos(1);
    let owner = DirectPlaintextOwner::new(&DriverLimits::default(), address, None, None, now)
        .unwrap_or_else(|error| panic!("construct direct owner: {error}"));
    let mut backend = ReactorBackend::Direct(Box::new(DirectBackend::Plaintext(Box::new(owner))));
    assert_eq!(backend.selector_count(), 1);
    let owner = match backend.direct_mut() {
        Some(DirectBackend::Plaintext(owner)) => owner.as_mut(),
        #[cfg(feature = "tls-rustls")]
        Some(DirectBackend::Rustls(_)) => panic!("plaintext test constructed a rustls owner"),
        None => panic!("direct construction must own only Bornera"),
    };
    assert_eq!(owner.selector_registrations(), 1);
    let mut causality = CausalSequence::new();

    for _ in 0..32 {
        drive(owner, now, &mut causality);
        if owner.lane.session.state().phase() == KafkaSessionPhase::Ready
            && owner.lane.admission_open
        {
            break;
        }
        wait_if_idle(owner);
    }
    assert_eq!(owner.lane.session.state().phase(), KafkaSessionPhase::Ready);
    assert!(owner.lane.admission_open);

    let (call, request) = erased_request(
        CallId::from_raw(7),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    );
    owner
        .submit(request, now, &mut causality)
        .unwrap_or_else(|error| panic!("submit direct request: {error}"));
    let mut result = None;
    for _ in 0..64 {
        drive(owner, now, &mut causality);
        result = call.try_result();
        if result.is_some() {
            break;
        }
        wait_if_idle(owner);
    }
    let result = result.unwrap_or_else(|| panic!("direct request did not finish within 64 turns"));

    assert_eq!(result, Ok(Ok(ApiVersionsResponse::default())));
    let contexts = owner.lane.contexts.snapshot();
    assert_eq!(contexts.reserved(), 0);
    assert_eq!(contexts.published(), 0);
    assert_eq!(contexts.retained_bytes().get(), 0);
    assert!(!contexts.is_poisoned());
    server
        .join()
        .unwrap_or_else(|_| panic!("join private direct broker"));
    for _ in 0..32 {
        if matches!(owner.lane.lifecycle.state(), BrokerState::Backoff { .. }) {
            break;
        }
        drive(owner, now, &mut causality);
        wait_if_idle(owner);
    }
    assert!(matches!(
        owner.lane.lifecycle.state(),
        BrokerState::Backoff { .. }
    ));
    assert!(!owner.is_terminal());
    assert!(!owner.has_local_work());
    assert!(owner.next_deadline().is_some());
    assert!(
        !owner
            .drive(now, &mut causality)
            .unwrap_or_else(|error| panic!("drive terminal direct owner: {error}"))
    );
}

#[test]
fn drain_rejects_pre_admission_request_as_not_sent_immediately() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind pending-drain broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read pending-drain address: {error}"));
    let now = Moment::from_nanos(1);
    let mut owner = DirectPlaintextOwner::new(&DriverLimits::default(), address, None, None, now)
        .unwrap_or_else(|error| panic!("construct pending-drain owner: {error}"));
    let mut causality = CausalSequence::new();
    let (call, request) = erased_request(
        CallId::from_raw(11),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    );
    owner
        .submit(request, now, &mut causality)
        .unwrap_or_else(|error| panic!("queue pending-drain request: {error}"));
    assert!(call.try_result().is_none());

    owner
        .begin_session_drain(now, &mut causality)
        .unwrap_or_else(|error| panic!("begin pending drain: {error}"));

    assert_eq!(
        call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::Draining,
            delivery: Delivery::NotSent,
        })))
    );
    assert!(owner.lane.pending.is_empty());
}

#[test]
fn delayed_admission_event_after_drain_does_not_reopen_admission() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind delayed-admission broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read delayed-admission address: {error}"));
    let now = Moment::from_nanos(1);
    let mut owner = DirectPlaintextOwner::new(&DriverLimits::default(), address, None, None, now)
        .unwrap_or_else(|error| panic!("construct delayed-admission owner: {error}"));
    let connection = owner.lane.connection_for_test();

    owner
        .access()
        .begin_lifecycle_drain(now)
        .unwrap_or_else(|error| panic!("begin delayed-admission drain: {error}"));
    assert!(matches!(
        owner.lane.lifecycle.phase(),
        BrokerPhase::Draining | BrokerPhase::Closed
    ));
    owner
        .access()
        .settle_event(
            ConnectionEvent::AdmissionOpened {
                sequence: 1,
                epoch: connection.epoch(),
            },
            now,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(|error| panic!("settle delayed admission: {error}"));

    assert!(!owner.lane.admission_open);
}

fn drive(owner: &mut DirectPlaintextOwner, now: Moment, causality: &mut CausalSequence) {
    owner
        .drive(now, causality)
        .unwrap_or_else(|error| panic!("drive direct owner: {error}"));
}

fn wait_if_idle(owner: &mut DirectPlaintextOwner) {
    if owner.has_local_work() {
        return;
    }
    let maximum = Span::try_from(Duration::from_millis(100))
        .unwrap_or_else(|error| panic!("convert direct wait bound: {error}"));
    owner
        .wait(maximum)
        .unwrap_or_else(|error| panic!("poll direct selector: {error}"));
}

fn serve_one(listener: &TcpListener) {
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept private direct broker: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound private direct read: {error}"));
    let negotiation = read_correlation(&mut peer);
    write_response(&mut peer, negotiation, &negotiation_body());
    let request = read_correlation(&mut peer);
    write_response(&mut peer, request, &ApiVersionsResponse::default());
}

fn read_correlation(peer: &mut TcpStream) -> i32 {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read private frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("convert private frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read private frame body: {error}"));
    i32::from_be_bytes(
        body.get(4..8)
            .unwrap_or_else(|| panic!("private request correlation is missing"))
            .try_into()
            .unwrap_or_else(|_| panic!("private correlation must be four bytes")),
    )
}

fn negotiation_body() -> ApiVersionsResponse {
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
        .unwrap_or_else(|error| panic!("encode private response header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode private response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("convert private response length: {error}"));
    peer.write_all(&length.to_be_bytes())
        .and_then(|()| peer.write_all(&body))
        .unwrap_or_else(|error| panic!("write private response: {error}"));
}
