//! Recovery totality across already-settled outcomes and nonterminal operations.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};

use bytes::BytesMut;
use calandria::Span;
use kafka_driver_core::{
    CallFailure, CallId, CloseReason, Delivery, KafkaSessionPhase, Moment, TransportFailure,
};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{DriverLimits, RequestError, request::erased_request};

use super::{failure_translation::fail_context, owner::DirectPlaintextOwner};
use crate::reactor::causality::CausalSequence;

#[test]
fn recovery_settles_missing_outcome_and_live_operation_once() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind recovery broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read recovery address: {error}"));
    let (release, hold) = mpsc::sync_channel(1);
    let server = thread::spawn(move || serve_negotiation(&listener, &hold));
    let now = Moment::from_nanos(1);
    let mut owner = DirectPlaintextOwner::new(&DriverLimits::default(), address, None, now)
        .unwrap_or_else(|error| panic!("construct recovery owner: {error}"));
    let mut causality = CausalSequence::new();
    drive_until_ready(&mut owner, now, &mut causality);
    let (settled_call, settled) = request(21);
    let (recovered_call, recovered) = request(22);
    owner
        .submit(settled, now, &mut causality)
        .unwrap_or_else(|error| panic!("submit pre-settled recovery request: {error}"));
    owner
        .submit(recovered, now, &mut causality)
        .unwrap_or_else(|error| panic!("submit live recovery request: {error}"));
    drive(&mut owner, now, &mut causality);
    assert_eq!(owner.contexts.snapshot().published(), 2);
    let (settled_key, settled_context) = owner
        .contexts
        .release_next()
        .unwrap_or_else(|| panic!("recovery test requires one published context"));
    fail_context(settled_context, RequestError::IdentityConflict);
    owner
        .set
        .cancel(owner.connection, settled_key.operation())
        .unwrap_or_else(|error| panic!("publish cancelled recovery outcome: {error}"));
    let report = owner
        .set
        .abandon(owner.connection, bornera::OwnerFailure::OwnerInvariant)
        .unwrap_or_else(|error| panic!("abandon recovery owner: {error}"));
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.operations.len(), 1);
    owner.pending_recovery = Some(report);

    assert!(
        owner
            .drive(now, &mut causality)
            .unwrap_or_else(|error| panic!("settle recovery report: {error}"))
    );
    release
        .send(())
        .unwrap_or_else(|error| panic!("release recovery broker: {error}"));
    server
        .join()
        .unwrap_or_else(|_| panic!("join recovery broker"));

    assert_eq!(
        settled_call.try_result(),
        Some(Ok(Err(RequestError::IdentityConflict)))
    );
    assert_eq!(
        recovered_call.try_result(),
        Some(Ok(Err(recovered_failure())))
    );
    let contexts = owner.contexts.snapshot();
    assert_eq!(contexts.reserved(), 0);
    assert_eq!(contexts.published(), 0);
    assert_eq!(contexts.retained_bytes().get(), 0);
    assert!(owner.is_terminal());
    assert!(!owner.has_local_work());
    assert!(owner.seed_snapshot().is_none());
}

fn drive_until_ready(
    owner: &mut DirectPlaintextOwner,
    now: Moment,
    causality: &mut CausalSequence,
) {
    for _ in 0..32 {
        drive(owner, now, causality);
        if owner.session.state().phase() == KafkaSessionPhase::Ready && owner.admission_open {
            return;
        }
        if !owner.has_local_work() {
            owner
                .wait(Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO))
                .unwrap_or_else(|error| panic!("wait for recovery readiness: {error}"));
        }
    }
    panic!("recovery owner did not become ready within 32 turns");
}

fn drive(owner: &mut DirectPlaintextOwner, now: Moment, causality: &mut CausalSequence) {
    owner
        .drive(now, causality)
        .unwrap_or_else(|error| panic!("drive recovery owner: {error}"));
}

fn request(
    call_id: u64,
) -> (
    crate::Call<Result<ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    )
}

fn recovered_failure() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: CloseReason::TransportLost(TransportFailure::Other),
        },
        delivery: Delivery::PossiblySent,
    }
}

fn serve_negotiation(listener: &TcpListener, hold: &mpsc::Receiver<()>) {
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept recovery broker: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound recovery read: {error}"));
    let correlation = read_correlation(&mut peer);
    write_negotiation(&mut peer, correlation);
    hold.recv()
        .unwrap_or_else(|error| panic!("hold recovery broker: {error}"));
}

fn read_correlation(peer: &mut TcpStream) -> i32 {
    let mut prefix = [0; 4];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read recovery length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("convert recovery length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read recovery body: {error}"));
    i32::from_be_bytes(
        body.get(4..8)
            .unwrap_or_else(|| panic!("recovery correlation is missing"))
            .try_into()
            .unwrap_or_else(|_| panic!("recovery correlation must be four bytes")),
    )
}

fn write_negotiation(peer: &mut TcpStream, correlation: i32) {
    let mut response = ApiVersionsResponse::default();
    let mut api = AdvertisedApi::default();
    api.api_key = API_VERSIONS_API_DESCRIPTOR.api_key.value();
    api.min_version = 0;
    api.max_version = 0;
    response.api_keys.push(api);
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    header
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode recovery header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode recovery response: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("convert recovery response length: {error}"));
    peer.write_all(&length.to_be_bytes())
        .and_then(|()| peer.write_all(&body))
        .unwrap_or_else(|error| panic!("write recovery response: {error}"));
}
