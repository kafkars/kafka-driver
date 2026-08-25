//! Initial API negotiation transitions and capability ownership.

use crate::{
    AuthenticationFailure, ConnectionEpoch, EffectId, Moment, NegotiatedCapabilities, TimerId,
    TransportId,
};

use super::{
    ActiveConnection, ActiveMode, CloseReason, ConnectionEffect, ConnectionMachine, Decision,
    KafkaSessionDeadline, KafkaSessionDisposition, KafkaSessionInput, NegotiationFailure,
    StateData,
};

const NEGOTIATION_CORRELATION: super::CorrelationId = super::CorrelationId::from_raw(0);

/// Reserved identities and driver-relative timing for one negotiation attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiationAttempt {
    pub(super) effect_id: EffectId,
    pub(super) deadline_timer: TimerId,
    pub(super) now: Moment,
    pub(super) deadline: Moment,
}

impl NegotiationAttempt {
    /// Creates one bounded initial negotiation attempt.
    pub const fn new(
        effect_id: EffectId,
        deadline_timer: TimerId,
        now: Moment,
        deadline: Moment,
    ) -> Self {
        Self {
            effect_id,
            deadline_timer,
            now,
            deadline,
        }
    }
}

impl ConnectionMachine {
    pub(super) fn begin_negotiation(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        attempt: NegotiationAttempt,
    ) -> Decision {
        let session = self.session.apply(KafkaSessionInput::TransportOpened {
            deadline: KafkaSessionDeadline::new(attempt.now, attempt.deadline),
        });
        if session.disposition() == KafkaSessionDisposition::IgnoredStale {
            return Decision::stale();
        }
        if attempt.deadline <= attempt.now {
            let reason = CloseReason::NegotiationFailed(NegotiationFailure::Timeout);
            self.state = StateData::Closing {
                epoch,
                transport_id,
                reason,
            };
            return Decision::applied(vec![ConnectionEffect::CloseTransport {
                epoch,
                transport_id,
                reason,
            }]);
        }
        self.state = StateData::Negotiating {
            epoch,
            transport_id,
            effect_id: attempt.effect_id,
            deadline_timer: attempt.deadline_timer,
            deadline: attempt.deadline,
        };
        Decision::applied(vec![
            ConnectionEffect::ScheduleNegotiationDeadline {
                epoch,
                timer_id: attempt.deadline_timer,
                at: attempt.deadline,
            },
            ConnectionEffect::NegotiateApiVersions {
                epoch,
                transport_id,
                effect_id: attempt.effect_id,
                correlation_id: NEGOTIATION_CORRELATION,
            },
        ])
    }

    pub(super) fn api_versions_negotiated(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
        capabilities: NegotiatedCapabilities,
    ) -> Decision {
        let StateData::Negotiating {
            epoch: expected_epoch,
            transport_id: expected_transport,
            effect_id: expected_effect,
            deadline_timer,
            ..
        } = &self.state
        else {
            return Decision::stale();
        };
        if epoch != *expected_epoch
            || transport_id != *expected_transport
            || effect_id != *expected_effect
        {
            return Decision::stale();
        }
        let deadline_timer = *deadline_timer;
        let session = self.session.apply(KafkaSessionInput::ApiVersionsSucceeded {
            capabilities: capabilities.clone(),
        });
        if session.disposition() == KafkaSessionDisposition::IgnoredStale {
            return Decision::stale();
        }
        if capabilities.len() > self.limits.max_capabilities().get() {
            return self.finish_negotiation_failure(NegotiationFailure::Capacity);
        }
        if self.authentication.is_some() {
            return self.finish_authentication_setup_failure(AuthenticationFailure::Protocol);
        }
        self.state = StateData::Active {
            mode: ActiveMode::Ready,
            connection: ActiveConnection::new(epoch, transport_id, capabilities, self.limits),
        };
        Decision::applied(vec![ConnectionEffect::CancelDeadline {
            timer_id: deadline_timer,
        }])
    }

    pub(super) fn api_versions_failed(
        &mut self,
        epoch: ConnectionEpoch,
        transport_id: TransportId,
        effect_id: EffectId,
        failure: NegotiationFailure,
    ) -> Decision {
        let StateData::Negotiating {
            epoch: expected_epoch,
            transport_id: expected_transport,
            effect_id: expected_effect,
            ..
        } = self.state
        else {
            return Decision::stale();
        };
        if epoch != expected_epoch
            || transport_id != expected_transport
            || effect_id != expected_effect
        {
            return Decision::stale();
        }
        let session = self
            .session
            .apply(KafkaSessionInput::ApiVersionsFailed { failure });
        if session.disposition() == KafkaSessionDisposition::IgnoredStale {
            return Decision::stale();
        }
        self.finish_negotiation_failure(failure)
    }

    pub(super) fn finish_negotiation_failure(&mut self, failure: NegotiationFailure) -> Decision {
        let StateData::Negotiating {
            epoch,
            transport_id,
            deadline_timer,
            ..
        } = self.state
        else {
            return Decision::stale();
        };
        let reason = CloseReason::NegotiationFailed(failure);
        self.state = StateData::Closing {
            epoch,
            transport_id,
            reason,
        };
        Decision::applied(vec![
            ConnectionEffect::CloseTransport {
                epoch,
                transport_id,
                reason,
            },
            ConnectionEffect::CancelDeadline {
                timer_id: deadline_timer,
            },
        ])
    }
}
