//! Shared deterministic fixtures for connection-machine scenario modules.

use std::num::NonZeroUsize;

use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{
    CallId, ConnectionEpoch, EffectId, Moment, NegotiatedApi, NegotiatedCapabilities, TimerId,
    TransportId,
};

use super::{
    ConnectionEffect, ConnectionInput, ConnectionLimits, ConnectionMachine, ConnectionTransition,
    CorrelationId, NegotiationAttempt,
};

pub(super) const EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(7);
pub(super) const STALE_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(6);
pub(super) const TRANSPORT: TransportId = TransportId::from_raw(11);
pub(super) const OPEN_EFFECT: EffectId = EffectId::from_raw(13);
pub(super) const NEGOTIATION_EFFECT: EffectId = EffectId::from_raw(14);
pub(super) const NEGOTIATION_TIMER: TimerId = TimerId::from_raw(15);

pub(super) fn ready_machine() -> ConnectionMachine {
    ready_machine_with(ConnectionLimits::default())
}

pub(super) fn ready_machine_with(limits: ConnectionLimits) -> ConnectionMachine {
    let mut machine = ConnectionMachine::new(EPOCH, limits);
    apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
        },
    );
    apply(
        &mut machine,
        transport_opened(EPOCH, OPEN_EFFECT, TRANSPORT),
    );
    apply(
        &mut machine,
        ConnectionInput::ApiVersionsNegotiated {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            capabilities: capabilities(),
        },
    );
    machine
}

pub(super) const fn transport_opened(
    epoch: ConnectionEpoch,
    effect_id: EffectId,
    transport_id: TransportId,
) -> ConnectionInput {
    ConnectionInput::TransportOpened {
        epoch,
        effect_id,
        transport_id,
        negotiation: NegotiationAttempt::new(
            NEGOTIATION_EFFECT,
            NEGOTIATION_TIMER,
            Moment::ORIGIN,
            Moment::from_nanos(100),
        ),
    }
}

pub(super) fn capabilities() -> NegotiatedCapabilities {
    let entries = [NegotiatedApi::new(ApiKey::new(18), ApiVersion::new(4))];
    NegotiatedCapabilities::try_from_iter(entries, NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("scenario capabilities must be valid: {error}"))
}

pub(super) fn submit(machine: &mut ConnectionMachine, raw: u64) -> ConnectionTransition {
    apply(
        machine,
        ConnectionInput::Submit {
            call_id: call(raw),
            write_effect: write_effect(raw),
            deadline_timer: timer(raw),
            now: Moment::from_nanos(10),
            deadline: Moment::from_nanos(1_000 + raw),
        },
    )
}

pub(super) fn mark_submitted(machine: &mut ConnectionMachine, raw: u64) {
    apply(
        machine,
        ConnectionInput::WriteSubmitted {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: write_effect(raw),
        },
    );
}

pub(super) fn correlation(transition: &ConnectionTransition) -> CorrelationId {
    let Some(ConnectionEffect::WriteRequest { correlation_id, .. }) = transition.effects().get(1)
    else {
        panic!("a successful submit emits schedule then write effects");
    };
    *correlation_id
}

pub(super) fn apply(
    machine: &mut ConnectionMachine,
    input: ConnectionInput,
) -> ConnectionTransition {
    let Ok(transition) = machine.apply(input) else {
        panic!("scenario input must produce a transition");
    };
    transition
}

pub(super) const fn call(raw: u64) -> CallId {
    CallId::from_raw(raw)
}

pub(super) const fn write_effect(raw: u64) -> EffectId {
    EffectId::from_raw(1_000 + raw)
}

pub(super) const fn timer(raw: u64) -> TimerId {
    TimerId::from_raw(2_000 + raw)
}
