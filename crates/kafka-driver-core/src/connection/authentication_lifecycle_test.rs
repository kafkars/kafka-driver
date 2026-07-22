//! Deadline and shutdown scenarios while the connection owns authentication.

use crate::{AuthenticationEffect, AuthenticationFailure};

use super::{CloseReason, ConnectionEffect, ConnectionInput};
use super::{
    authentication_test::{AUTHENTICATION_TIMER, authenticating_machine, authentication_deadline},
    scenario_support_test::{EPOCH, apply},
};

#[test]
fn authentication_deadline_and_shutdown_share_terminal_cleanup_ordering() {
    // Given
    let mut timed_out = authenticating_machine();
    let mut draining = authenticating_machine();

    // When
    let timeout = apply(
        &mut timed_out,
        ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            timer_id: AUTHENTICATION_TIMER,
            now: authentication_deadline(),
        },
    );
    let shutdown = apply(&mut draining, ConnectionInput::BeginDrain);

    // Then
    assert!(matches!(
        timeout.effects(),
        [
            ConnectionEffect::CloseTransport {
                reason: CloseReason::AuthenticationFailed(AuthenticationFailure::Timeout),
                ..
            },
            ConnectionEffect::Authentication {
                effect: AuthenticationEffect::CancelDeadline {
                    timer_id: AUTHENTICATION_TIMER
                }
            }
        ]
    ));
    assert!(matches!(
        shutdown.effects(),
        [
            ConnectionEffect::CloseTransport {
                reason: CloseReason::Requested,
                ..
            },
            ConnectionEffect::Authentication {
                effect: AuthenticationEffect::CancelDeadline {
                    timer_id: AUTHENTICATION_TIMER
                }
            }
        ]
    ));
}
