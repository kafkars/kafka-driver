//! Admission after terminal authentication failure retains its exact cause.

use crate::{AuthenticationFailure, AuthenticationInput, Delivery, ExchangeOutcome, Moment};

use super::{
    CallFailure, CloseReason, ConnectionEffect, ConnectionInput,
    authentication_test::{AUTHENTICATION_EFFECT, exchanging_machine, round},
    scenario_support_test::{EPOCH, TRANSPORT, apply, call, timer, write_effect},
};

#[test]
fn call_after_authentication_rejection_retains_the_terminal_reason() {
    // Given: invalid credentials have already made this epoch terminal.
    let mut machine = exchanging_machine();
    let reason = CloseReason::AuthenticationFailed(AuthenticationFailure::Rejected);
    apply(
        &mut machine,
        ConnectionInput::Authentication {
            input: AuthenticationInput::ExchangeCompleted {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                effect_id: AUTHENTICATION_EFFECT,
                round: round(1),
                outcome: ExchangeOutcome::Failed(AuthenticationFailure::Rejected),
            },
        },
    );

    // When: a later public call asks the closed connection for admission.
    let transition = apply(
        &mut machine,
        ConnectionInput::Submit {
            call_id: call(1),
            write_effect: write_effect(1),
            deadline_timer: timer(1),
            now: Moment::from_nanos(300),
            deadline: Moment::from_nanos(400),
        },
    );

    // Then: the caller can distinguish credential rejection from generic closure.
    assert_eq!(
        transition.effects(),
        [ConnectionEffect::FailCall {
            call_id: call(1),
            failure: CallFailure::ConnectionClosed { reason },
            delivery: Delivery::NotSent,
        }]
    );
}
