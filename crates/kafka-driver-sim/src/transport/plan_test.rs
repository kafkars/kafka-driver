//! Fault-plan validation scenarios for impossible read and write progress.

use super::{
    ReadRequest, ReadResult, ReadStep, TransportIdentity, TransportOutcome, TransportPlanError,
    WriteRequest, WriteResult, WriteStep,
};

#[test]
fn read_step_rejects_bytes_larger_than_the_expected_buffer() {
    let request = ReadRequest::new(identity(1), nonzero(2));
    let outcome = TransportOutcome::new(identity(1), ReadResult::Bytes(vec![1, 2, 3]));

    assert_eq!(
        ReadStep::new(request, outcome),
        Err(TransportPlanError::ReadExceedsRequest {
            returned: 3,
            maximum: 2,
        })
    );
}

#[test]
fn write_step_rejects_progress_larger_than_the_offered_slice() {
    let request = WriteRequest::new(identity(1), vec![1, 2]);
    let outcome = TransportOutcome::new(identity(1), WriteResult::Written(3));

    assert_eq!(
        WriteStep::new(request, outcome),
        Err(TransportPlanError::WriteExceedsRequest {
            written: 3,
            offered: 2,
        })
    );
}

fn identity(epoch: u64) -> TransportIdentity {
    TransportIdentity::new(
        kafka_driver_core::ConnectionEpoch::from_raw(epoch),
        kafka_driver_core::TransportId::from_raw(9),
    )
}

fn nonzero(value: usize) -> std::num::NonZeroUsize {
    let Some(value) = std::num::NonZeroUsize::new(value) else {
        panic!("test bound must be nonzero");
    };
    value
}
