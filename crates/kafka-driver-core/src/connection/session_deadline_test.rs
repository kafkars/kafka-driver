//! Session deadline and protocol-failure scenarios.

use crate::{Moment, NegotiationFailure};

use super::{
    KafkaSessionCloseReason, KafkaSessionDeadline, KafkaSessionEffect, KafkaSessionInput,
    KafkaSessionLimits, KafkaSessionMachine, KafkaSessionProtocolFailure,
};

#[test]
fn early_negotiation_deadline_reschedules_and_elapsed_deadline_closes() {
    let mut session = KafkaSessionMachine::new(KafkaSessionLimits::default());
    let deadline = Moment::from_nanos(100);
    let _ = session.apply(KafkaSessionInput::TransportOpened {
        deadline: KafkaSessionDeadline::new(Moment::ORIGIN, deadline),
    });

    let early = session.apply(KafkaSessionInput::DeadlineElapsed {
        now: Moment::from_nanos(99),
    });
    let elapsed = session.apply(KafkaSessionInput::DeadlineElapsed { now: deadline });

    assert_eq!(
        early.effects(),
        [KafkaSessionEffect::RescheduleDeadline { at: deadline }]
    );
    assert_eq!(
        elapsed.effects(),
        [KafkaSessionEffect::CloseSession {
            reason: KafkaSessionCloseReason::NegotiationFailed(NegotiationFailure::Timeout),
        }]
    );
}

#[test]
fn protocol_failure_closes_without_transport_or_operation_identity() {
    let mut session = KafkaSessionMachine::new(KafkaSessionLimits::default());
    let _ = session.apply(KafkaSessionInput::TransportOpened {
        deadline: KafkaSessionDeadline::new(Moment::ORIGIN, Moment::from_nanos(100)),
    });

    let failed = session.apply(KafkaSessionInput::ProtocolFailed {
        failure: KafkaSessionProtocolFailure::Malformed,
    });

    assert_eq!(
        failed.effects(),
        [
            KafkaSessionEffect::CloseSession {
                reason: KafkaSessionCloseReason::ProtocolFailed(
                    KafkaSessionProtocolFailure::Malformed,
                ),
            },
            KafkaSessionEffect::CancelDeadline,
        ]
    );
}
