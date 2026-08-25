//! Plaintext session ownership scenarios without transport or operation identities.

use crate::Moment;

use super::scenario_support_test::capabilities;
use super::{
    KafkaSessionCloseReason, KafkaSessionDeadline, KafkaSessionEffect, KafkaSessionInput,
    KafkaSessionLimits, KafkaSessionMachine, KafkaSessionPhase,
};

#[test]
fn transport_open_negotiates_then_exposes_ready_and_drain_signals() {
    let mut session = KafkaSessionMachine::new(KafkaSessionLimits::default());
    let deadline = Moment::from_nanos(100);

    let negotiation = session.apply(KafkaSessionInput::TransportOpened {
        deadline: KafkaSessionDeadline::new(Moment::ORIGIN, deadline),
    });
    let ready = session.apply(KafkaSessionInput::ApiVersionsSucceeded {
        capabilities: capabilities(),
    });
    let drain = session.apply(KafkaSessionInput::BeginDrain);
    let drained = session.apply(KafkaSessionInput::Drained);

    assert_eq!(
        negotiation.effects(),
        [KafkaSessionEffect::StartApiVersions { deadline }]
    );
    assert_eq!(
        ready.effects(),
        [
            KafkaSessionEffect::CancelDeadline,
            KafkaSessionEffect::SessionReady,
        ]
    );
    assert_eq!(drain.effects(), [KafkaSessionEffect::BeginDrain]);
    assert_eq!(
        drained.effects(),
        [KafkaSessionEffect::CloseSession {
            reason: KafkaSessionCloseReason::Drained,
        }]
    );
    assert_eq!(session.state().phase(), KafkaSessionPhase::Closing);
}
