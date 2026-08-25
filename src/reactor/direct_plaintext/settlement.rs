//! Exact outcome, lifecycle, and fatal-recovery settlement for direct operations.

use bornera::{ConnectionEvent, EngineOutcome, RegisteredTransport};
use bornera_core::{CloseReason as BorneraCloseReason, OperationOutcome};
use kafka_driver_core::{AuthenticationFailure, Delivery, Moment, NegotiationFailure};

use crate::{RequestError, reactor::causality::CausalSequence};

use super::{
    failure_translation::{negotiation, recovery},
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, message},
};
use crate::reactor::bornera::OperationContextKey;

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn settle_event(
        &mut self,
        event: ConnectionEvent,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        match event {
            ConnectionEvent::TransportOpened { epoch, .. } if epoch == self.connection.epoch() => {
                self.transport_opened(now)?;
            }
            ConnectionEvent::AdmissionOpened { epoch, .. } if epoch == self.connection.epoch() => {
                self.admission_open = true;
            }
            ConnectionEvent::Closing { epoch, reason, .. } if epoch == self.connection.epoch() => {
                self.admission_open = false;
                if reason == BorneraCloseReason::Drained {
                    self.session_drained_by_engine()?;
                }
            }
            ConnectionEvent::Closed { epoch, reason, .. } if epoch == self.connection.epoch() => {
                self.admission_open = false;
                if reason == BorneraCloseReason::Drained {
                    self.session_drained_by_engine()?;
                }
                let diagnostic = self
                    .set
                    .connection_snapshot(self.connection)
                    .ok()
                    .and_then(|snapshot| snapshot.transport_diagnostic);
                let recovered =
                    super::failure_translation::connection_close_reason(reason, diagnostic);
                let effective = self.last_close_reason.unwrap_or(recovered);
                self.last_close_reason = Some(effective);
                self.session_closed(now)?;
                self.fail_pending(&recovery(effective, Delivery::NotSent), Some(causality))?;
                self.fail_remaining(
                    &recovery(effective, Delivery::PossiblySent),
                    Some(causality),
                    None,
                )?;
                self.terminal = true;
                self.latch_retired_seed();
                self.set.retire(self.connection).map_err(message)?;
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
    ) -> std::io::Result<()> {
        let key = OperationContextKey::new(outcome.epoch(), outcome.operation());
        let Some(context) = self.contexts.release(key) else {
            if session_live {
                return Err(std::io::Error::other(
                    "Bornera outcome has no semantic context",
                ));
            }
            return Ok(());
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
                self.settle_public_outcome(context, outcome, now, causality, session_live)?;
            }
            (DirectOperationContext::Negotiation(_), _) => {
                if session_live {
                    self.negotiation_failed(NegotiationFailure::Malformed, now)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn fail_released(
        &self,
        key: OperationContextKey,
        failure: RequestError,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        let Some(context) = self.contexts.release(key) else {
            return Ok(());
        };
        if let DirectOperationContext::Public(context) = context {
            let observed = causality.outcome().map_err(message)?;
            let _ = context.fail_observed(failure, observed);
        }
        Ok(())
    }

    pub(super) fn fail_remaining(
        &self,
        failure: &RequestError,
        mut causality: Option<&mut CausalSequence>,
        unobserved: Option<()>,
    ) -> std::io::Result<()> {
        while let Some((_key, context)) = self.contexts.release_next() {
            if let DirectOperationContext::Public(context) = context {
                if unobserved.is_some() {
                    let _ = context.fail(failure.clone());
                } else if let Some(sequence) = causality.as_deref_mut() {
                    let observed = sequence.outcome().map_err(message)?;
                    let _ = context.fail_observed(failure.clone(), observed);
                } else {
                    let _ = context.fail(failure.clone());
                }
            }
        }
        Ok(())
    }

    pub(super) fn fail_pending(
        &mut self,
        failure: &RequestError,
        mut causality: Option<&mut CausalSequence>,
    ) -> std::io::Result<()> {
        while let Some(request) = self.pending.pop() {
            if let Some(sequence) = causality.as_deref_mut() {
                let observed = sequence.outcome().map_err(message)?;
                request.fail_observed(failure.clone(), observed);
            } else {
                request.fail(failure.clone());
            }
        }
        Ok(())
    }
}
