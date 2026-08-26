//! Exact outcome, lifecycle, and fatal-recovery settlement for direct operations.

use bornera::{ConnectionEvent, ConnectionRetireError, EngineOutcome, RegisteredTransport};
use bornera_core::{CloseReason as BorneraCloseReason, OperationOutcome};
use kafka_driver_core::{AuthenticationFailure, CloseReason, Delivery, Moment, NegotiationFailure};

use crate::{RequestError, reactor::causality::CausalSequence};

use super::{
    failure_translation::{negotiation, recovery},
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, message},
    reconnect::core_epoch,
};
use crate::reactor::bornera::OperationContextKey;

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn settle_event(
        &mut self,
        event: ConnectionEvent,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        let connection = self.live_connection()?;
        match event {
            ConnectionEvent::TransportOpened { epoch, .. } if epoch == connection.epoch() => {
                self.transport_opened(now)?;
            }
            ConnectionEvent::AdmissionOpened { epoch, .. } if epoch == connection.epoch() => {
                self.lifecycle.ready(core_epoch(epoch))?;
                self.admission_open = true;
            }
            ConnectionEvent::Closing { epoch, reason, .. } if epoch == connection.epoch() => {
                self.admission_open = false;
                if reason == BorneraCloseReason::Drained {
                    self.session_drained_by_engine()?;
                }
            }
            ConnectionEvent::Closed { epoch, reason, .. } if epoch == connection.epoch() => {
                self.admission_open = false;
                if reason == BorneraCloseReason::Drained {
                    self.session_drained_by_engine()?;
                }
                let diagnostic = self
                    .set
                    .connection_snapshot(connection)
                    .ok()
                    .and_then(|snapshot| snapshot.transport_diagnostic);
                let recovered =
                    super::failure_translation::connection_close_reason(reason, diagnostic);
                self.session_closed(now)?;
                let effective = self.generation_close_reason.unwrap_or(recovered);
                self.generation_close_reason = None;
                self.last_close_reason = Some(effective);
                let semantic_diverged = self.fail_remaining(
                    &recovery(effective, Delivery::PossiblySent),
                    Some(causality),
                    None,
                )?;
                match self.set.retire(connection) {
                    Ok(()) => {}
                    Err(ConnectionRetireError::StaleConnection) => {
                        self.generation_close_reason = Some(effective);
                        return self.stale_generation_fatal(now, Some(causality));
                    }
                    Err(error) => return Err(message(error)),
                }
                self.connection = None;
                self.last_turn = calandria::Turn::waiting();
                if semantic_diverged != 0 {
                    self.fail_pending(&recovery(effective, Delivery::NotSent), Some(causality))?;
                    self.terminal = true;
                    return Err(std::io::Error::other(
                        "direct retirement lost semantic outcome ownership",
                    ));
                }
                self.settle_generation_lifecycle(core_epoch(epoch), effective, now, causality)?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn settle_outcome(
        &mut self,
        outcome: EngineOutcome<crate::reactor::bornera::KafkaFrame>,
        now: Moment,
        causality: &mut CausalSequence,
        session_live: bool,
        recovery_reason: Option<CloseReason>,
    ) -> std::io::Result<bool> {
        let key = OperationContextKey::new(outcome.epoch(), outcome.operation());
        let Some(context) = self.contexts.release(key) else {
            if session_live {
                let connection = self.live_connection()?;
                let report = self.abandon_generation(
                    connection,
                    bornera::OwnerFailure::OwnerInvariant,
                    now,
                    Some(causality),
                )?;
                self.capture_diverged_recovery(report);
            }
            return Ok(false);
        };
        match (context, outcome.into_outcome()) {
            (
                DirectOperationContext::Negotiation(Some(exchange)),
                OperationOutcome::Reply(frame),
            ) if session_live => {
                self.finish_negotiation(exchange, frame, now)?;
            }
            (DirectOperationContext::Negotiation(_), OperationOutcome::Failed { failure, .. })
                if session_live =>
            {
                self.negotiation_failed(negotiation(failure), now)?;
            }
            (
                DirectOperationContext::Authentication(Some(exchange)),
                OperationOutcome::Reply(frame),
            ) if session_live => self.settle_authentication_reply(exchange, frame, now)?,
            (
                DirectOperationContext::Authentication(exchange),
                OperationOutcome::Failed { failure, .. },
            ) if session_live => self.settle_authentication_failure(exchange, failure, now)?,
            (DirectOperationContext::Authentication(_), _) if session_live => {
                self.fail_active_authentication(AuthenticationFailure::Malformed, now)?;
            }
            (DirectOperationContext::Public(context), outcome) => {
                self.settle_public_outcome(
                    context,
                    outcome,
                    now,
                    causality,
                    session_live,
                    recovery_reason,
                )?;
            }
            (DirectOperationContext::Negotiation(_), _) => {
                if session_live {
                    self.negotiation_failed(NegotiationFailure::Malformed, now)?;
                }
            }
            _ => {}
        }
        Ok(true)
    }

    pub(super) fn fail_released(
        &self,
        key: OperationContextKey,
        failure: RequestError,
        causality: &mut CausalSequence,
    ) -> std::io::Result<bool> {
        let Some(context) = self.contexts.release(key) else {
            return Ok(false);
        };
        if let DirectOperationContext::Public(context) = context {
            match causality.outcome() {
                Ok(observed) => {
                    let _ = context.fail_observed(failure, observed);
                }
                Err(error) => {
                    let _ = context.fail(failure);
                    return Err(message(error));
                }
            }
        }
        Ok(true)
    }

    pub(super) fn fail_remaining(
        &self,
        failure: &RequestError,
        mut causality: Option<&mut CausalSequence>,
        unobserved: Option<()>,
    ) -> std::io::Result<usize> {
        let mut released = 0;
        let mut first_error = None;
        while let Some((_key, context)) = self.contexts.release_next() {
            released += 1;
            if let DirectOperationContext::Public(context) = context {
                if unobserved.is_some() {
                    let _ = context.fail(failure.clone());
                } else if let Some(sequence) = causality.as_deref_mut() {
                    match sequence.outcome() {
                        Ok(observed) => {
                            let _ = context.fail_observed(failure.clone(), observed);
                        }
                        Err(error) => {
                            let _ = context.fail(failure.clone());
                            first_error.get_or_insert_with(|| message(error));
                        }
                    }
                } else {
                    let _ = context.fail(failure.clone());
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(released),
        }
    }

    pub(super) fn fail_pending(
        &mut self,
        failure: &RequestError,
        mut causality: Option<&mut CausalSequence>,
    ) -> std::io::Result<()> {
        let mut first_error = None;
        while let Some(request) = self.pending.pop() {
            if let Some(sequence) = causality.as_deref_mut() {
                match sequence.outcome() {
                    Ok(observed) => request.fail_observed(failure.clone(), observed),
                    Err(error) => {
                        request.fail(failure.clone());
                        first_error.get_or_insert_with(|| message(error));
                    }
                }
            } else {
                request.fail(failure.clone());
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
