//! Session-stage deadline fencing, rescheduling, and timeout policy.

use crate::{AuthenticationFailure, Moment, NegotiationFailure};

use super::{AuthenticationStage, Decision, KafkaSessionEffect, KafkaSessionMachine, StateData};

impl KafkaSessionMachine {
    pub(super) fn deadline_elapsed(&mut self, now: Moment) -> Decision {
        match &self.state {
            StateData::Negotiating { deadline } => {
                let deadline = *deadline;
                if now < deadline {
                    Decision::applied(vec![KafkaSessionEffect::RescheduleDeadline {
                        at: deadline,
                    }])
                } else {
                    self.close_negotiation(NegotiationFailure::Timeout, false)
                }
            }
            StateData::Authenticating { stage, .. } => {
                let deadline = match stage {
                    AuthenticationStage::Handshaking { deadline }
                    | AuthenticationStage::Exchanging { deadline, .. } => *deadline,
                };
                if now < deadline {
                    Decision::applied(vec![KafkaSessionEffect::RescheduleDeadline {
                        at: deadline,
                    }])
                } else {
                    self.close_authentication(AuthenticationFailure::Timeout, true)
                }
            }
            StateData::AwaitingTransport
            | StateData::Ready { .. }
            | StateData::Draining { .. }
            | StateData::Closing { .. }
            | StateData::Closed { .. } => Decision::stale(),
        }
    }
}
