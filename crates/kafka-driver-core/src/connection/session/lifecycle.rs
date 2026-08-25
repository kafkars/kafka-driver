//! Ready, drain, protocol-failure, and terminal session transitions.

use std::mem;

use super::{
    Decision, KafkaSessionCloseReason, KafkaSessionEffect, KafkaSessionMachine,
    KafkaSessionProtocolFailure, StateData,
};

impl KafkaSessionMachine {
    pub(super) fn begin_drain(&mut self) -> Decision {
        match self.state {
            StateData::AwaitingTransport => {
                self.close(KafkaSessionCloseReason::Requested, false, false)
            }
            StateData::Negotiating { .. } | StateData::Authenticating { .. } => {
                self.close(KafkaSessionCloseReason::Requested, true, false)
            }
            StateData::Ready { .. } => {
                let placeholder = StateData::Closing {
                    reason: KafkaSessionCloseReason::Requested,
                };
                let previous = mem::replace(&mut self.state, placeholder);
                let StateData::Ready { capabilities } = previous else {
                    return Decision::stale();
                };
                self.state = StateData::Draining { capabilities };
                Decision::applied(vec![KafkaSessionEffect::BeginDrain])
            }
            StateData::Draining { .. } | StateData::Closing { .. } | StateData::Closed { .. } => {
                Decision::ignored()
            }
        }
    }

    pub(super) fn drained(&mut self) -> Decision {
        if !matches!(self.state, StateData::Draining { .. }) {
            return Decision::stale();
        }
        self.close(KafkaSessionCloseReason::Drained, false, false)
    }

    pub(super) fn protocol_failed(&mut self, failure: KafkaSessionProtocolFailure) -> Decision {
        if matches!(
            self.state,
            StateData::Closing { .. } | StateData::Closed { .. }
        ) {
            return Decision::stale();
        }
        let cancel_deadline = matches!(
            self.state,
            StateData::Negotiating { .. } | StateData::Authenticating { .. }
        );
        let reason = KafkaSessionCloseReason::ProtocolFailed(failure);
        self.close(reason, cancel_deadline, true)
    }

    pub(super) fn closed(&mut self) -> Decision {
        let reason = match self.state {
            StateData::Closed { .. } => return Decision::ignored(),
            StateData::Closing { reason } => reason,
            StateData::AwaitingTransport
            | StateData::Negotiating { .. }
            | StateData::Authenticating { .. }
            | StateData::Ready { .. }
            | StateData::Draining { .. } => KafkaSessionCloseReason::TransportClosed,
        };
        self.state = StateData::Closed { reason };
        Decision::applied(Vec::new())
    }

    pub(super) fn close(
        &mut self,
        reason: KafkaSessionCloseReason,
        cancel_deadline: bool,
        fault: bool,
    ) -> Decision {
        self.state = StateData::Closing { reason };
        let mut effects = vec![KafkaSessionEffect::CloseSession { reason }];
        if cancel_deadline {
            effects.push(KafkaSessionEffect::CancelDeadline);
        }
        if fault {
            Decision::fault(effects)
        } else {
            Decision::applied(effects)
        }
    }
}
