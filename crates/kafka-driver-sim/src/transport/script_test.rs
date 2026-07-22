//! Transport scenarios for partial progress, faults, mismatches, and staleness.

use std::num::NonZeroUsize;

use kafka_driver_core::{ConnectionEpoch, TransportId};

use super::{
    FaultPlan, ReadRequest, ReadResult, ReadStep, ScriptedTransport, TransportFault,
    TransportIdentity, TransportOperationKind, TransportOutcome, TransportScriptError,
    TransportStep, WriteRequest, WriteResult, WriteStep,
};

const TRANSPORT: TransportId = TransportId::from_raw(9);
const CURRENT: ConnectionEpoch = ConnectionEpoch::from_raw(7);
const STALE: ConnectionEpoch = ConnectionEpoch::from_raw(6);

#[test]
fn consecutive_reads_can_fragment_one_frame_and_then_would_block() {
    let first = read_step(4, ReadResult::Bytes(vec![0, 0]));
    let second = read_step(4, ReadResult::Bytes(vec![0, 3, 1, 2]));
    let blocked = read_step(4, ReadResult::WouldBlock);
    let mut transport = ScriptedTransport::new(FaultPlan::new([
        TransportStep::Read(first),
        TransportStep::Read(second),
        TransportStep::Read(blocked),
    ]));
    let request = ReadRequest::new(identity(CURRENT), nonzero(4));

    assert_eq!(
        read(&mut transport, request).into_result(),
        ReadResult::Bytes(vec![0, 0])
    );
    assert_eq!(
        read(&mut transport, request).into_result(),
        ReadResult::Bytes(vec![0, 3, 1, 2])
    );
    assert_eq!(
        read(&mut transport, request).into_result(),
        ReadResult::WouldBlock
    );
    assert!(transport.is_complete());
}

#[test]
fn writes_report_exact_partial_progress_before_a_fault() {
    let bytes = vec![0, 0, 0, 3, 1, 2, 3];
    let first_request = WriteRequest::new(identity(CURRENT), bytes.clone());
    let second_request = WriteRequest::new(identity(CURRENT), bytes[2..].to_vec());
    let first = write_step(first_request.clone(), WriteResult::Written(2));
    let failed = write_step(
        second_request.clone(),
        WriteResult::Failed(TransportFault::BrokenPipe),
    );
    let mut transport = ScriptedTransport::new(FaultPlan::new([
        TransportStep::Write(first),
        TransportStep::Write(failed),
    ]));

    assert_eq!(
        write(&mut transport, first_request).into_result(),
        WriteResult::Written(2)
    );
    assert_eq!(
        write(&mut transport, second_request).into_result(),
        WriteResult::Failed(TransportFault::BrokenPipe)
    );
}

#[test]
fn wrong_operation_and_payload_do_not_consume_the_plan_front() {
    let expected = WriteRequest::new(identity(CURRENT), vec![1, 2]);
    let step = write_step(expected.clone(), WriteResult::Written(1));
    let mut transport = ScriptedTransport::new(FaultPlan::new([TransportStep::Write(step)]));
    let read_request = ReadRequest::new(identity(CURRENT), nonzero(2));

    assert_eq!(
        transport.read(read_request),
        Err(TransportScriptError::UnexpectedOperation {
            expected: TransportOperationKind::Write,
            received: TransportOperationKind::Read,
        })
    );
    let received = WriteRequest::new(identity(CURRENT), vec![1, 3]);
    assert_eq!(
        transport.write(received.clone()),
        Err(TransportScriptError::UnexpectedWrite {
            expected: expected.clone(),
            received,
        })
    );
    assert_eq!(transport.remaining_steps(), 1);
    assert_eq!(
        write(&mut transport, expected).into_result(),
        WriteResult::Written(1)
    );
}

#[test]
fn transport_outcome_can_intentionally_carry_a_stale_identity() {
    let request = ReadRequest::new(identity(CURRENT), nonzero(8));
    let outcome = TransportOutcome::new(identity(STALE), ReadResult::Closed);
    let Ok(step) = ReadStep::new(request, outcome) else {
        panic!("closed result must be a valid read step");
    };
    let mut transport = ScriptedTransport::new(FaultPlan::new([TransportStep::Read(step)]));

    let observed = read(&mut transport, request);

    assert_eq!(observed.identity(), identity(STALE));
    assert_eq!(observed.result(), &ReadResult::Closed);
}

#[test]
fn exhausted_plan_returns_the_unexpected_call() {
    let request = ReadRequest::new(identity(CURRENT), nonzero(1));
    let mut transport = ScriptedTransport::new(FaultPlan::default());

    assert_eq!(
        transport.read(request),
        Err(TransportScriptError::ReadPlanExhausted { received: request })
    );
}

fn read_step(max_bytes: usize, result: ReadResult) -> ReadStep {
    let request = ReadRequest::new(identity(CURRENT), nonzero(max_bytes));
    let outcome = TransportOutcome::new(identity(CURRENT), result);
    let Ok(step) = ReadStep::new(request, outcome) else {
        panic!("test read step must fit its buffer");
    };
    step
}

fn write_step(request: WriteRequest, result: WriteResult) -> WriteStep {
    let outcome = TransportOutcome::new(identity(CURRENT), result);
    let Ok(step) = WriteStep::new(request, outcome) else {
        panic!("test write progress must fit its offered bytes");
    };
    step
}

fn read(transport: &mut ScriptedTransport, request: ReadRequest) -> TransportOutcome<ReadResult> {
    let Ok(outcome) = transport.read(request) else {
        panic!("test read must match the fault plan");
    };
    outcome
}

fn write(
    transport: &mut ScriptedTransport,
    request: WriteRequest,
) -> TransportOutcome<WriteResult> {
    let Ok(outcome) = transport.write(request) else {
        panic!("test write must match the fault plan");
    };
    outcome
}

const fn identity(epoch: ConnectionEpoch) -> TransportIdentity {
    TransportIdentity::new(epoch, TRANSPORT)
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test bound must be nonzero");
    };
    value
}
