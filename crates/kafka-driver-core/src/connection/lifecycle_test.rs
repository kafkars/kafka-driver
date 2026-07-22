//! Scenarios proving opening, draining, and terminal lifecycle ownership.

use crate::{ConnectionEpoch, EffectId, TransportId};

use super::scenario_support_test::{
    EPOCH, OPEN_EFFECT, STALE_EPOCH, TRANSPORT, apply, ready_machine, transport_opened,
};
use super::{
    CloseReason, ConnectionEffect, ConnectionInput, ConnectionLimits, ConnectionMachine,
    ConnectionPhase, ConnectionState, TransitionDisposition, TransportFailure,
};

#[test]
fn start_reserves_one_transport_and_enters_opening() {
    let mut machine = ConnectionMachine::new(EPOCH, ConnectionLimits::default());

    let transition = apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
        },
    );

    assert_eq!(
        transition.effects(),
        &[ConnectionEffect::OpenTransport {
            epoch: EPOCH,
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
        }]
    );
    assert_eq!(transition.record().from(), ConnectionPhase::Dormant);
    assert_eq!(transition.record().to(), ConnectionPhase::Opening);
    assert_eq!(
        machine.state(),
        ConnectionState::Opening {
            epoch: EPOCH,
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
        }
    );
}

#[test]
fn stale_open_result_cannot_claim_the_reserved_transport() {
    let mut machine = ConnectionMachine::new(EPOCH, ConnectionLimits::default());
    apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
        },
    );

    let transition = apply(
        &mut machine,
        transport_opened(STALE_EPOCH, OPEN_EFFECT, TRANSPORT),
    );

    assert!(transition.effects().is_empty());
    assert_eq!(
        transition.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Opening);
}

#[test]
fn matching_open_failure_is_terminal_and_sanitized() {
    let mut machine = ConnectionMachine::new(EPOCH, ConnectionLimits::default());
    apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
        },
    );

    let transition = apply(
        &mut machine,
        ConnectionInput::TransportOpenFailed {
            epoch: EPOCH,
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
            failure: TransportFailure::Refused,
        },
    );

    assert!(transition.effects().is_empty());
    assert_eq!(
        machine.state(),
        ConnectionState::Closed {
            epoch: EPOCH,
            reason: CloseReason::OpenFailed(TransportFailure::Refused),
        }
    );
}

#[test]
fn empty_ready_connection_drains_through_explicit_close() {
    let mut machine = ready_machine();

    let drain = apply(&mut machine, ConnectionInput::BeginDrain);

    assert_eq!(
        drain.effects(),
        &[ConnectionEffect::CloseTransport {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            reason: CloseReason::Drained,
        }]
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);

    apply(
        &mut machine,
        ConnectionInput::TransportClosed {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            failure: TransportFailure::Other,
        },
    );
    assert_eq!(
        machine.state(),
        ConnectionState::Closed {
            epoch: EPOCH,
            reason: CloseReason::Drained,
        }
    );
}

#[test]
fn dormant_drain_needs_no_external_resource() {
    let epoch = ConnectionEpoch::from_raw(21);
    let mut machine = ConnectionMachine::new(epoch, ConnectionLimits::default());

    let transition = apply(&mut machine, ConnectionInput::BeginDrain);

    assert!(transition.effects().is_empty());
    assert_eq!(
        machine.state(),
        ConnectionState::Closed {
            epoch,
            reason: CloseReason::Requested,
        }
    );
}

#[test]
fn mismatched_open_identities_are_stale() {
    let mut machine = ConnectionMachine::new(EPOCH, ConnectionLimits::default());
    apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
        },
    );

    let transition = apply(
        &mut machine,
        transport_opened(EPOCH, EffectId::from_raw(99), TransportId::from_raw(100)),
    );

    assert_eq!(
        transition.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Opening);
}
