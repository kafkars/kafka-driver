//! Transport-establishment identity, deadline, and outcome transitions.

use crate::{ConnectionEpoch, EffectId, Moment, TimerId, TransportId};

use super::{
    CloseReason, ConnectionEffect, ConnectionMachine, Decision, NegotiationAttempt, StateData,
    TransportFailure,
};

impl ConnectionMachine {
    pub(super) fn start(
        &mut self,
        effect_id: EffectId,
        transport_id: TransportId,
        deadline_timer: TimerId,
        deadline: Moment,
    ) -> Decision {
        let StateData::Dormant { epoch } = self.session.state else {
            return Decision::ignored();
        };
        self.session.state = StateData::Opening {
            epoch,
            effect_id,
            transport_id,
            deadline_timer,
            deadline,
        };
        Decision::applied(vec![
            ConnectionEffect::ScheduleOpenDeadline {
                epoch,
                timer_id: deadline_timer,
                at: deadline,
            },
            ConnectionEffect::OpenTransport {
                epoch,
                effect_id,
                transport_id,
            },
        ])
    }

    pub(super) fn transport_opened(
        &mut self,
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        transport_id: TransportId,
        negotiation: NegotiationAttempt,
    ) -> Decision {
        let StateData::Opening {
            epoch: expected_epoch,
            effect_id: expected_effect,
            transport_id: expected_transport,
            deadline_timer,
            ..
        } = self.session.state
        else {
            return Decision::stale();
        };
        if epoch != expected_epoch
            || effect_id != expected_effect
            || transport_id != expected_transport
        {
            return Decision::stale();
        }
        let mut decision = self
            .session
            .begin_negotiation(epoch, transport_id, negotiation);
        decision.effects.insert(
            0,
            ConnectionEffect::CancelDeadline {
                timer_id: deadline_timer,
            },
        );
        decision
    }

    pub(super) fn transport_open_failed(
        &mut self,
        epoch: ConnectionEpoch,
        effect_id: EffectId,
        transport_id: TransportId,
        failure: TransportFailure,
    ) -> Decision {
        let StateData::Opening {
            epoch: expected_epoch,
            effect_id: expected_effect,
            transport_id: expected_transport,
            deadline_timer,
            ..
        } = self.session.state
        else {
            return Decision::stale();
        };
        if epoch != expected_epoch
            || effect_id != expected_effect
            || transport_id != expected_transport
        {
            return Decision::stale();
        }
        self.session.state = StateData::Closed {
            epoch,
            reason: CloseReason::OpenFailed(failure),
        };
        Decision::applied(vec![ConnectionEffect::CancelDeadline {
            timer_id: deadline_timer,
        }])
    }
}
