//! Session-machine settlement for owner-requested and engine-observed closure.

use kafka_driver_core::{
    CallFailure, KafkaSessionCloseReason, KafkaSessionEffect, KafkaSessionInput, KafkaSessionState,
    Moment,
};

use super::{failure_translation::not_sent, owner::DirectPlaintextOwner};

impl DirectPlaintextOwner {
    pub(in crate::reactor) fn begin_session_drain(&mut self, now: Moment) -> std::io::Result<()> {
        self.admission_open = false;
        self.fail_pending(&not_sent(CallFailure::Draining), None)?;
        self.apply_session(KafkaSessionInput::BeginDrain, now)
    }

    pub(super) fn session_closed(&mut self, now: Moment) -> std::io::Result<()> {
        self.apply_session(KafkaSessionInput::Closed, now)?;
        if let KafkaSessionState::Closed { reason } = self.session.state()
            && reason != KafkaSessionCloseReason::TransportClosed
        {
            self.record_session_close(reason);
        }
        Ok(())
    }

    pub(super) fn session_drained_by_engine(&mut self) -> std::io::Result<()> {
        let effects = self
            .session
            .apply(KafkaSessionInput::Drained)
            .into_effects();
        for effect in effects {
            match effect {
                // Bornera already began the authoritative Drained closure.
                KafkaSessionEffect::CloseSession {
                    reason: KafkaSessionCloseReason::Drained,
                } => self.record_session_close(KafkaSessionCloseReason::Drained),
                KafkaSessionEffect::CancelDeadline => {}
                _ => {
                    return Err(std::io::Error::other(
                        "drained Bornera connection produced an unexpected session effect",
                    ));
                }
            }
        }
        Ok(())
    }
}
