//! Focused ownership scenarios for the extracted Kafka session policy.

use crate::Moment;

use super::scenario_support_test::{
    EPOCH, NEGOTIATION_EFFECT, NEGOTIATION_TIMER, TRANSPORT, capabilities,
};
use super::{
    CloseReason, ConnectionEffect, ConnectionLimits, ConnectionPhase, KafkaSessionMachine,
    NegotiationAttempt,
};

#[test]
fn session_machine_owns_negotiation_readiness_and_empty_drain() {
    let mut session = KafkaSessionMachine::new(EPOCH, ConnectionLimits::default());
    let attempt = NegotiationAttempt::new(
        NEGOTIATION_EFFECT,
        NEGOTIATION_TIMER,
        Moment::ORIGIN,
        Moment::from_nanos(100),
    );

    let negotiation = session.begin_negotiation(EPOCH, TRANSPORT, attempt);
    let ready =
        session.api_versions_negotiated(EPOCH, TRANSPORT, NEGOTIATION_EFFECT, capabilities());
    let drain = session.begin_drain();

    assert!(matches!(
        negotiation.effects.as_slice(),
        [
            ConnectionEffect::ScheduleNegotiationDeadline { .. },
            ConnectionEffect::NegotiateApiVersions { .. }
        ]
    ));
    assert_eq!(
        ready.effects,
        vec![ConnectionEffect::CancelDeadline {
            timer_id: NEGOTIATION_TIMER,
        }]
    );
    assert_eq!(session.phase(), ConnectionPhase::Closing);
    assert_eq!(
        drain.effects,
        vec![ConnectionEffect::CloseTransport {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            reason: CloseReason::Drained,
        }]
    );
}
