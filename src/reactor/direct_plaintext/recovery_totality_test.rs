//! Executable totality proofs for live and fatal Direct recovery edges.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};

use bornera_core::OperationOptions;
use bytes::BytesMut;
use calandria::{Deadline, RetainedBytes, Span};
use kafka_driver_core::{CallFailure, CallId, Delivery, KafkaSessionPhase, Moment};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{DriverLimits, RequestError, request::erased_request};

use super::{
    failure_translation::fail_context,
    operation_owner::DirectOperationContext,
    owner::{DirectPlaintextOwner, calandria_moment},
};
use crate::reactor::{bornera::correlation_id, causality::CausalSequence};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn live_missing_context_recovery_keeps_predrained_suffix() {
    let (mut owner, release, server) = ready_owner();
    let mut causality = CausalSequence::new();
    let (first, first_request) = request(101);
    let (second, second_request) = request(102);
    owner
        .submit(first_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit first suffix call: {error}"));
    owner
        .submit(second_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit second suffix call: {error}"));
    let keys = owner.lane.contexts.keys_for_test();
    assert_eq!(keys.len(), 2);
    let missing = owner
        .lane
        .contexts
        .release(keys[0])
        .unwrap_or_else(|| panic!("release missing suffix context"));
    fail_context(missing, RequestError::IdentityConflict);
    let connection = owner.lane.connection_for_test();
    owner
        .connections
        .set
        .cancel(connection, keys[0].operation())
        .unwrap_or_else(|error| panic!("cancel missing suffix operation: {error}"));
    owner
        .connections
        .set
        .cancel(connection, keys[1].operation())
        .unwrap_or_else(|error| panic!("cancel retained suffix operation: {error}"));

    let error = owner
        .drive(NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("missing live context must fail the host"));
    finish_server(&release, server);

    assert_eq!(error.to_string(), fatal_recovery());
    assert_eq!(
        first.try_result(),
        Some(Ok(Err(RequestError::IdentityConflict)))
    );
    assert!(matches!(second.try_result(), Some(Ok(Err(_)))));
    assert_total(&owner);
}

#[test]
fn causal_exhaustion_totalizes_recovered_and_pending_calls() {
    let (mut owner, release, server) = ready_owner();
    let mut causality = CausalSequence::new();
    let (first, first_request) = request(103);
    let (second, second_request) = request(104);
    owner
        .submit(first_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit first causal call: {error}"));
    owner
        .submit(second_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit second causal call: {error}"));
    let connection = owner.lane.connection_for_test();
    let report = owner
        .connections
        .set
        .abandon(connection, bornera::OwnerFailure::OwnerInvariant)
        .unwrap_or_else(|error| panic!("capture causal recovery report: {error}"));
    owner.access().capture_recovery(report);
    let (pending, pending_request) = request(105);
    owner
        .submit(pending_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("queue causal pending call: {error}"));
    causality.next = u64::MAX;

    let error = owner
        .drive(NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("causal exhaustion must fail the host"));
    finish_server(&release, server);

    assert_eq!(
        error.to_string(),
        "the reactor causal sequence is exhausted"
    );
    for call in [first, second, pending] {
        assert!(matches!(call.try_result(), Some(Ok(Err(_)))));
    }
    assert_total(&owner);
}

#[test]
fn stale_commit_with_exhausted_causality_completes_current_call() {
    let (mut owner, release, server) = ready_owner();
    let (call, request) = request(106);
    let preparation = request
        .prepare_bornera(
            ApiVersion::new(0),
            None,
            owner.lane.outbound_limits,
            owner.lane.decode_limits,
        )
        .unwrap_or_else(|error| panic!("prepare stale causal call: {error}"));
    let measure = preparation.measure();
    let retained = preparation.context_retained_bytes();
    let (encoder, context) = preparation.into_parts();
    let mut reservation = owner
        .lane
        .contexts
        .reserve(DirectOperationContext::Public(context), retained)
        .unwrap_or_else(|_| panic!("reserve stale causal context"));
    let write_retained = RetainedBytes::try_from(measure.wire_bytes)
        .unwrap_or_else(|error| panic!("measure stale causal frame: {error}"));
    let options = OperationOptions::until(Deadline::at(calandria_moment(Moment::from_nanos(10))))
        .retained_bytes(retained)
        .write_retained_bytes(write_retained);
    let connection = owner.lane.connection_for_test();
    let permit = owner
        .connections
        .set
        .reserve(connection, calandria_moment(NOW), options)
        .unwrap_or_else(|error| panic!("reserve stale causal permit: {error}"));
    let correlation = correlation_id(permit.match_key())
        .unwrap_or_else(|error| panic!("bind stale causal correlation: {error}"));
    let frame = reservation
        .bind(|context| match context {
            DirectOperationContext::Public(context) => {
                encoder.bind_and_encode(correlation, context)
            }
            _ => panic!("stale causal test reserved a session context"),
        })
        .unwrap_or_else(|error| panic!("encode stale causal frame: {error}"));
    drop(
        owner
            .connections
            .set
            .abandon(connection, bornera::OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("make stale causal token: {error}")),
    );
    let mut causality = CausalSequence { next: u64::MAX };

    let error = owner
        .access()
        .commit_public(permit, frame, reservation, NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("stale causal commit must fail the host"));
    finish_server(&release, server);

    assert_eq!(
        error.to_string(),
        "the reactor causal sequence is exhausted"
    );
    assert_eq!(call.try_result(), Some(Ok(Err(closed_not_sent()))));
    assert_total(&owner);
}

fn ready_owner() -> (
    DirectPlaintextOwner,
    mpsc::SyncSender<()>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind totality broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read totality address: {error}"));
    let (release, hold) = mpsc::sync_channel(1);
    let server = thread::spawn(move || serve(&listener, &hold));
    let mut owner = DirectPlaintextOwner::new(&DriverLimits::default(), address, None, None, NOW)
        .unwrap_or_else(|error| panic!("construct totality owner: {error}"));
    let mut causality = CausalSequence::new();
    for _ in 0..32 {
        owner
            .drive(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("drive totality owner: {error}"));
        if owner.lane.session.state().phase() == KafkaSessionPhase::Ready
            && owner.lane.admission_open
        {
            return (owner, release, server);
        }
        if !owner.has_local_work() {
            owner
                .wait(Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO))
                .unwrap_or_else(|error| panic!("wait for totality readiness: {error}"));
        }
    }
    panic!("totality owner did not become ready");
}

fn request(
    id: u64,
) -> (
    crate::Call<Result<ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(id),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    )
}

fn assert_total(owner: &DirectPlaintextOwner) {
    let contexts = owner.lane.contexts.snapshot();
    assert_eq!(contexts.reserved(), 0);
    assert_eq!(contexts.published(), 0);
    assert_eq!(contexts.retained_bytes().get(), 0);
    assert!(owner.lane.pending.is_empty());
    assert!(owner.is_terminal());
}

fn closed_not_sent() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}

fn fatal_recovery() -> &'static str {
    "fatal Bornera owner recovery cannot reuse the direct selector"
}

fn finish_server(release: &mpsc::SyncSender<()>, server: thread::JoinHandle<()>) {
    release
        .send(())
        .unwrap_or_else(|error| panic!("release totality broker: {error}"));
    server
        .join()
        .unwrap_or_else(|_| panic!("join totality broker"));
}

fn serve(listener: &TcpListener, hold: &mpsc::Receiver<()>) {
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept totality broker: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound totality read: {error}"));
    let correlation = read_correlation(&mut peer);
    write_negotiation(&mut peer, correlation);
    hold.recv()
        .unwrap_or_else(|error| panic!("hold totality broker: {error}"));
}

fn read_correlation(peer: &mut TcpStream) -> i32 {
    let mut prefix = [0; 4];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read totality length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("convert totality length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read totality body: {error}"));
    i32::from_be_bytes(
        body.get(4..8)
            .unwrap_or_else(|| panic!("totality correlation is missing"))
            .try_into()
            .unwrap_or_else(|_| panic!("totality correlation must be four bytes")),
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
        .unwrap_or_else(|error| panic!("encode totality header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode totality response: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("convert totality response length: {error}"));
    peer.write_all(&length.to_be_bytes())
        .and_then(|()| peer.write_all(&body))
        .unwrap_or_else(|error| panic!("write totality response: {error}"));
}
