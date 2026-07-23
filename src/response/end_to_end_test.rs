//! Generated request/response flow through policy, partial I/O, framing, and completion.

use std::num::NonZeroUsize;

use bytes::{Bytes, BytesMut};
use kafka_driver_core::{
    CallId, ConnectionEffect, ConnectionEpoch, ConnectionInput, ConnectionLimits,
    ConnectionMachine, ConnectionState, CorrelationId, EffectId, Moment, NegotiatedCapabilities,
    NegotiationAttempt, TimerId, TransportId,
};
use kafka_driver_sim::{
    FaultPlan, ReadRequest, ReadResult, ReadStep, ScriptedTransport,
    TransportIdentity as SimIdentity, TransportOutcome, TransportStep,
    WriteRequest as SimWriteRequest, WriteResult as SimWriteResult, WriteStep,
};
use kafka_driver_transport::{
    FrameDecoder, FrameLimits, WriteProgress, WriteQueue, WriteQueueLimits,
};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, OutboundFrameLimits, ResponseHeader, encode_request,
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaEncode};

use super::registry::ResponseRegistry;

const EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(1);
const TRANSPORT: TransportId = TransportId::from_raw(2);
const OPEN_EFFECT: EffectId = EffectId::from_raw(3);
const OPEN_TIMER: TimerId = TimerId::from_raw(8);
const CALL: CallId = CallId::from_raw(4);
const WRITE_EFFECT: EffectId = EffectId::from_raw(5);
const TIMER: TimerId = TimerId::from_raw(6);
const NEGOTIATION_EFFECT: EffectId = EffectId::from_raw(7);
const NEGOTIATION_TIMER: TimerId = TimerId::from_raw(8);

#[test]
fn generated_call_survives_partial_writes_and_fragmented_response_reads() {
    let mut machine = ready_machine();
    let correlation = submit(&mut machine);
    let mut registry = ResponseRegistry::new(nonzero(4), DecodeLimits::default());
    let Ok(call) = registry.register::<ApiVersionsRequest>(CALL, correlation, version()) else {
        panic!("generated response type must register at its supported version");
    };
    let request_frame = encode_generated_request(correlation);
    let response = ApiVersionsResponse::default();
    let response_frame = encode_generated_response(correlation, &response);
    let mut writes = WriteQueue::new(WriteQueueLimits::new(nonzero(4), nonzero(4_096)));
    assert!(
        writes
            .admit(CALL, WRITE_EFFECT, request_frame.clone())
            .is_ok()
    );
    apply(
        &mut machine,
        ConnectionInput::WriteSubmitted {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: WRITE_EFFECT,
        },
    );

    let split_write = 3;
    let split_read = 3;
    let mut transport =
        scripted_transport(&request_frame, &response_frame, split_write, split_read);
    write_once(&mut transport, &mut writes, split_write);
    write_once(
        &mut transport,
        &mut writes,
        request_frame.len() - split_write,
    );
    assert_eq!(writes.queued_frames(), 0);

    let mut frames = FrameDecoder::new(frame_limits());
    read_once(&mut transport, &mut frames);
    assert_eq!(frames.next_frame(), Ok(None));
    read_once(&mut transport, &mut frames);
    let Ok(Some(frame)) = frames.next_frame() else {
        panic!("fragmented generated response must form one complete frame");
    };
    assert!(transport.is_complete());

    let Ok(envelope) = registry.inspect_front(frame) else {
        panic!("generated response header must be inspectable");
    };
    let transition = apply(
        &mut machine,
        ConnectionInput::ResponseReceived {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            correlation_id: envelope.correlation_id(),
        },
    );
    assert_eq!(
        transition.effects(),
        &[
            ConnectionEffect::CancelDeadline { timer_id: TIMER },
            ConnectionEffect::CompleteResponse {
                call_id: CALL,
                correlation_id: correlation,
            },
        ]
    );
    assert!(
        registry
            .complete_verified(
                CALL,
                correlation,
                envelope,
                kafka_driver_core::OutcomeStamp::from_raw(9),
            )
            .is_ok()
    );

    assert_eq!(call.wait(), Ok(Ok(response)));
    assert_eq!(registry.pending(), 0);
    assert!(matches!(
        machine.state(),
        ConnectionState::Ready { pending: 0, .. }
    ));
}

fn ready_machine() -> ConnectionMachine {
    let mut machine = ConnectionMachine::new(EPOCH, ConnectionLimits::default());
    apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
            deadline_timer: OPEN_TIMER,
            deadline: Moment::from_nanos(50),
        },
    );
    apply(
        &mut machine,
        ConnectionInput::TransportOpened {
            epoch: EPOCH,
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
            negotiation: NegotiationAttempt::new(
                NEGOTIATION_EFFECT,
                NEGOTIATION_TIMER,
                Moment::ORIGIN,
                Moment::from_nanos(100),
            ),
        },
    );
    apply(
        &mut machine,
        ConnectionInput::ApiVersionsNegotiated {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            capabilities: NegotiatedCapabilities::try_from_iter([], NonZeroUsize::MIN)
                .unwrap_or_else(|error| panic!("test capabilities must be valid: {error}")),
        },
    );
    machine
}

fn submit(machine: &mut ConnectionMachine) -> CorrelationId {
    let transition = apply(
        machine,
        ConnectionInput::Submit {
            call_id: CALL,
            write_effect: WRITE_EFFECT,
            deadline_timer: TIMER,
            now: Moment::ORIGIN,
            deadline: Moment::from_nanos(1_000),
        },
    );
    let Some(ConnectionEffect::WriteRequest { correlation_id, .. }) = transition.effects().get(1)
    else {
        panic!("accepted call must emit an ordered write effect");
    };
    *correlation_id
}

fn scripted_transport(
    request: &Bytes,
    response: &[u8],
    split_write: usize,
    split_read: usize,
) -> ScriptedTransport {
    let identity = SimIdentity::new(EPOCH, TRANSPORT);
    let first_write = write_step(
        SimWriteRequest::new(identity, request.to_vec()),
        split_write,
    );
    let second_write = write_step(
        SimWriteRequest::new(identity, request[split_write..].to_vec()),
        request.len() - split_write,
    );
    let read_request = ReadRequest::new(identity, nonzero(4_096));
    let first_read = read_step(read_request, response[..split_read].to_vec());
    let second_read = read_step(read_request, response[split_read..].to_vec());
    ScriptedTransport::new(FaultPlan::new([
        TransportStep::Write(first_write),
        TransportStep::Write(second_write),
        TransportStep::Read(first_read),
        TransportStep::Read(second_read),
    ]))
}

fn write_once(transport: &mut ScriptedTransport, queue: &mut WriteQueue, expected: usize) {
    let Some(front) = queue.front(nonzero(4_096)) else {
        panic!("queued frame must expose writable bytes");
    };
    let effect_id = front.effect_id();
    let request = SimWriteRequest::new(SimIdentity::new(EPOCH, TRANSPORT), front.bytes().to_vec());
    let Ok(outcome) = transport.write(request) else {
        panic!("offered bytes must match scripted partial write");
    };
    let SimWriteResult::Written(written) = outcome.into_result() else {
        panic!("scenario transport step must make write progress");
    };
    assert_eq!(written, expected);
    assert!(matches!(
        queue.advance(effect_id, written),
        Ok(WriteProgress::Pending { .. } | WriteProgress::Complete { .. })
    ));
}

fn read_once(transport: &mut ScriptedTransport, decoder: &mut FrameDecoder) {
    let request = ReadRequest::new(SimIdentity::new(EPOCH, TRANSPORT), nonzero(4_096));
    let Ok(outcome) = transport.read(request) else {
        panic!("bounded read must match scripted response fragment");
    };
    let ReadResult::Bytes(bytes) = outcome.into_result() else {
        panic!("scenario transport step must return response bytes");
    };
    assert!(decoder.feed(&bytes).is_ok());
}

fn write_step(request: SimWriteRequest, written: usize) -> WriteStep {
    let outcome = TransportOutcome::new(request.identity(), SimWriteResult::Written(written));
    let Ok(step) = WriteStep::new(request, outcome) else {
        panic!("scripted partial write must fit offered bytes");
    };
    step
}

fn read_step(request: ReadRequest, bytes: Vec<u8>) -> ReadStep {
    let outcome = TransportOutcome::new(request.identity(), ReadResult::Bytes(bytes));
    let Ok(step) = ReadStep::new(request, outcome) else {
        panic!("scripted response fragment must fit read bound");
    };
    step
}

fn encode_generated_request(correlation: CorrelationId) -> Bytes {
    let mut frame = BytesMut::new();
    assert!(
        encode_request(
            &mut frame,
            correlation.get(),
            None,
            &ApiVersionsRequest::default(),
            version(),
            OutboundFrameLimits::new(4_092),
        )
        .is_ok(),
        "generated request must encode at its supported version"
    );
    frame.freeze()
}

fn encode_generated_response(
    correlation: CorrelationId,
    response: &ApiVersionsResponse,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation.get();
    let Ok(header_version) = response_header_version_for::<ApiVersionsRequest>(version()) else {
        panic!("supported test response must have header policy");
    };
    let header_version = ApiVersion::new(header_version);
    assert!(header.encode_into(&mut body, header_version).is_ok());
    assert!(response.encode_into(&mut body, version()).is_ok());
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("test response must fit Kafka frame length");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn frame_limits() -> FrameLimits {
    let Ok(limits) = FrameLimits::new(nonzero(4_092), nonzero(4_096)) else {
        panic!("test frame bounds must fit one maximum frame");
    };
    limits
}

fn apply(
    machine: &mut ConnectionMachine,
    input: ConnectionInput,
) -> kafka_driver_core::ConnectionTransition {
    let Ok(transition) = machine.apply(input) else {
        panic!("scenario input must satisfy connection invariants");
    };
    transition
}

const fn version() -> ApiVersion {
    ApiVersion::new(3)
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test bound must be nonzero");
    };
    value
}
