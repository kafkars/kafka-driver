//! Ordered session-policy effects and one owner-local establishment deadline.

use bornera::{ConnectionAccessError, RegisteredTransport};
use bornera_core::CloseReason;
use calandria::Deadline;
use kafka_driver_core::{KafkaSessionCloseReason, KafkaSessionEffect, KafkaSessionInput, Moment};

use super::owner::{DirectLaneAccess, add, calandria_moment, message};

const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn apply_session(
        &mut self,
        input: KafkaSessionInput,
        now: Moment,
    ) -> std::io::Result<()> {
        let effects = self.session.apply(input).into_effects();
        for effect in effects {
            self.apply_session_effect(effect, now)?;
        }
        Ok(())
    }

    fn apply_session_effect(
        &mut self,
        effect: KafkaSessionEffect,
        now: Moment,
    ) -> std::io::Result<()> {
        match effect {
            KafkaSessionEffect::StartApiVersions { deadline } => {
                self.session_deadline = Some(deadline);
                self.start_negotiation(now, deadline)
            }
            KafkaSessionEffect::StartAuthenticationHandshake {
                mechanism,
                version,
                deadline,
            } => {
                self.session_deadline = Some(deadline);
                self.start_authentication_handshake(mechanism, version, now, deadline)
            }
            KafkaSessionEffect::StartAuthenticationExchange {
                round,
                version,
                deadline,
            } => {
                self.session_deadline = Some(deadline);
                self.start_authentication_exchange(round, version, now, deadline)
            }
            KafkaSessionEffect::RescheduleDeadline { at } => {
                self.session_deadline = Some(at);
                Ok(())
            }
            KafkaSessionEffect::CancelDeadline => {
                self.session_deadline = None;
                Ok(())
            }
            KafkaSessionEffect::SessionReady => {
                let connection = self.live_connection()?;
                self.clear_authentication_ownership();
                self.session_deadline = None;
                self.mark_generation_ready(super::reconnect::core_epoch(connection.epoch()))?;
                match self.set.open_admission(connection) {
                    Ok(_) => {}
                    Err(ConnectionAccessError::StaleConnection) => {
                        return self.stale_generation_fatal(now, None);
                    }
                    Err(ConnectionAccessError::Owner(_)) => {
                        let report = self.recover_failed_generation(connection, now, None)?;
                        self.capture_recovery(report);
                        return Ok(());
                    }
                    Err(error) => return Err(message(error)),
                }
                self.mark_runnable();
                Ok(())
            }
            KafkaSessionEffect::BeginDrain => {
                let connection = self.live_connection()?;
                self.admission_open = false;
                let deadline = add(now, DRAIN_TIMEOUT)?;
                match self
                    .set
                    .begin_drain(connection, Deadline::at(calandria_moment(deadline)))
                {
                    Ok(_) => {}
                    Err(ConnectionAccessError::StaleConnection) => {
                        return self.stale_generation_fatal(now, None);
                    }
                    Err(ConnectionAccessError::Owner(_)) => {
                        let report = self.recover_failed_generation(connection, now, None)?;
                        self.capture_recovery(report);
                        return Ok(());
                    }
                    Err(error) => return Err(message(error)),
                }
                self.mark_runnable();
                Ok(())
            }
            KafkaSessionEffect::CloseSession { reason } => {
                let connection = self.live_connection()?;
                self.admission_open = false;
                self.clear_authentication_ownership();
                self.session_deadline = None;
                self.record_generation_close(reason);
                match self.set.finalize(connection, close_reason(reason)) {
                    Ok(_) => {}
                    Err(ConnectionAccessError::StaleConnection) => {
                        return self.stale_generation_fatal(now, None);
                    }
                    Err(ConnectionAccessError::Owner(_)) => {
                        let report = self.recover_failed_generation(connection, now, None)?;
                        self.capture_recovery(report);
                        return Ok(());
                    }
                    Err(error) => return Err(message(error)),
                }
                self.mark_runnable();
                Ok(())
            }
        }
    }

    pub(in crate::reactor) fn fire_due_session_deadline(
        &mut self,
        now: Moment,
    ) -> std::io::Result<bool> {
        if self.terminal || self.session_deadline.is_none_or(|deadline| deadline > now) {
            return Ok(false);
        }
        self.session_deadline = None;
        self.apply_session(KafkaSessionInput::DeadlineElapsed { now }, now)?;
        Ok(true)
    }
}

const fn close_reason(reason: KafkaSessionCloseReason) -> CloseReason {
    match reason {
        KafkaSessionCloseReason::Drained => CloseReason::Drained,
        KafkaSessionCloseReason::ProtocolFailed(_) => CloseReason::MalformedReply,
        KafkaSessionCloseReason::Requested
        | KafkaSessionCloseReason::NegotiationFailed(_)
        | KafkaSessionCloseReason::AuthenticationFailed(_)
        | KafkaSessionCloseReason::TransportClosed => CloseReason::Requested,
    }
}
