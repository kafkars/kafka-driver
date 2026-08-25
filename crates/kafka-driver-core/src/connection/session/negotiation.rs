//! Transport-open observation and `ApiVersions` session transitions.

use crate::{AuthenticationFailure, NegotiatedCapabilities, NegotiationFailure, SaslProtocol};

use super::{
    AuthenticationStage, Decision, KafkaSessionCloseReason, KafkaSessionDeadline,
    KafkaSessionEffect, KafkaSessionMachine, StateData,
};

impl KafkaSessionMachine {
    pub(super) fn transport_opened(&mut self, attempt: KafkaSessionDeadline) -> Decision {
        if !matches!(self.state, StateData::AwaitingTransport) {
            return Decision::stale();
        }
        if attempt.deadline <= attempt.now {
            return self.close_negotiation(NegotiationFailure::Timeout, false);
        }
        self.state = StateData::Negotiating {
            deadline: attempt.deadline,
        };
        Decision::applied(vec![KafkaSessionEffect::StartApiVersions {
            deadline: attempt.deadline,
        }])
    }

    pub(super) fn api_versions_succeeded(
        &mut self,
        capabilities: NegotiatedCapabilities,
    ) -> Decision {
        if !matches!(self.state, StateData::Negotiating { .. }) {
            return Decision::stale();
        }
        if capabilities.len() > self.limits.max_capabilities().get() {
            return self.close_negotiation(NegotiationFailure::Capacity, true);
        }
        if self.authentication.is_some() {
            return self.close_authentication(AuthenticationFailure::Protocol, true);
        }
        self.state = StateData::Ready { capabilities };
        Decision::applied(vec![
            KafkaSessionEffect::CancelDeadline,
            KafkaSessionEffect::SessionReady,
        ])
    }

    pub(super) fn api_versions_succeeded_with_authentication(
        &mut self,
        capabilities: NegotiatedCapabilities,
        attempt: KafkaSessionDeadline,
    ) -> Decision {
        if !matches!(self.state, StateData::Negotiating { .. }) {
            return Decision::stale();
        }
        if capabilities.len() > self.limits.max_capabilities().get() {
            return self.close_negotiation(NegotiationFailure::Capacity, true);
        }
        let Some(policy) = self.authentication else {
            return self.close_authentication(AuthenticationFailure::Protocol, true);
        };
        let Some(handshake_version) = capabilities.version(policy.handshake_api_key()) else {
            return self.close_authentication(AuthenticationFailure::Protocol, true);
        };
        let Some(authenticate_version) = capabilities.version(policy.authenticate_api_key()) else {
            return self.close_authentication(AuthenticationFailure::Protocol, true);
        };
        if attempt.deadline <= attempt.now {
            return self.close_authentication(AuthenticationFailure::Timeout, true);
        }
        let protocol =
            SaslProtocol::new(policy.mechanism(), handshake_version, authenticate_version);
        self.state = StateData::Authenticating {
            capabilities,
            protocol,
            stage: AuthenticationStage::Handshaking {
                deadline: attempt.deadline,
            },
        };
        Decision::applied(vec![
            KafkaSessionEffect::CancelDeadline,
            KafkaSessionEffect::StartAuthenticationHandshake {
                mechanism: policy.mechanism(),
                version: handshake_version,
                deadline: attempt.deadline,
            },
        ])
    }

    pub(super) fn api_versions_failed(&mut self, failure: NegotiationFailure) -> Decision {
        if !matches!(self.state, StateData::Negotiating { .. }) {
            return Decision::stale();
        }
        self.close_negotiation(failure, true)
    }

    pub(super) fn close_negotiation(
        &mut self,
        failure: NegotiationFailure,
        cancel_deadline: bool,
    ) -> Decision {
        let reason = KafkaSessionCloseReason::NegotiationFailed(failure);
        self.close(reason, cancel_deadline, false)
    }

    pub(super) fn close_authentication(
        &mut self,
        failure: AuthenticationFailure,
        cancel_deadline: bool,
    ) -> Decision {
        let reason = KafkaSessionCloseReason::AuthenticationFailed(failure);
        self.close(reason, cancel_deadline, false)
    }
}
