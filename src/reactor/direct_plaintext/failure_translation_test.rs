//! Platform-neutral translation of established transport loss.

use std::io;

use bornera::{TransportDiagnostic, TransportFailureKind, TransportFailurePhase};
use bornera_core::CloseReason as BorneraCloseReason;
use kafka_driver_core::{CloseReason, TransportFailure};

use super::failure_translation::{close_reason, connection_close_reason};

#[test]
fn disconnected_and_reset_transports_share_the_stable_reset_category() {
    let expected = CloseReason::TransportLost(TransportFailure::Reset);
    assert_eq!(close_reason(BorneraCloseReason::TransportLost), expected);

    let reset = TransportDiagnostic::new(
        TransportFailurePhase::Read,
        TransportFailureKind::Io,
        io::ErrorKind::ConnectionReset,
        None,
    );
    assert_eq!(
        connection_close_reason(BorneraCloseReason::TransportLost, Some(reset)),
        expected
    );
}

#[test]
fn possible_send_deadline_without_a_diagnostic_remains_other() {
    assert_eq!(
        close_reason(BorneraCloseReason::DeadlineAfterPossibleSend),
        CloseReason::TransportLost(TransportFailure::Other)
    );
}
