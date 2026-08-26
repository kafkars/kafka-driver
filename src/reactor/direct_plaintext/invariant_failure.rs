//! Total settlement when Bornera generation ownership becomes impossible.

use bornera::{ConnectionRecoveryError, ConnectionToken, OwnerFailure, RegisteredTransport};
use kafka_driver_core::{CloseReason, Delivery, Moment, TransportFailure};

use super::{
    failure_translation::recovery,
    operation_owner::DirectOperationContext,
    owner::{DirectLaneAccess, DirectRecoveryReport},
};
use crate::{
    RequestError,
    reactor::{
        bornera::{OperationContextKey, driver_delivery},
        causality::CausalSequence,
    },
};

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn totalize_duplicate_recovery(&mut self, report: DirectRecoveryReport) {
        let reason = self.recovery_fallback_reason();
        let bornera::RecoveryReport {
            epoch,
            operations,
            unmatched_writes,
            outcomes,
            ..
        } = report;
        let fallback = recovery(reason, Delivery::PossiblySent);
        for outcome in outcomes {
            let key = OperationContextKey::new(outcome.epoch(), outcome.operation());
            self.fail_context_unobserved(key, fallback.clone());
        }
        for recovered in operations {
            let key = OperationContextKey::new(epoch, recovered.operation);
            let failure = recovery(reason, driver_delivery(recovered.delivery));
            self.fail_context_unobserved(key, failure);
        }
        for discarded in unmatched_writes {
            let key = OperationContextKey::new(epoch, discarded.operation);
            let failure = recovery(reason, driver_delivery(discarded.delivery));
            self.fail_context_unobserved(key, failure);
        }
        let _ = self.fail_remaining(&fallback, None, Some(()));
    }

    pub(super) fn capture_context_divergence(
        &mut self,
        now: Moment,
        causality: Option<&mut CausalSequence>,
    ) -> std::io::Result<()> {
        let connection = self.live_connection()?;
        let report =
            self.abandon_generation(connection, OwnerFailure::OwnerInvariant, now, causality)?;
        self.capture_diverged_recovery(report);
        Ok(())
    }

    pub(super) fn recover_failed_generation(
        &mut self,
        connection: ConnectionToken,
        now: Moment,
        causality: Option<&mut CausalSequence>,
    ) -> std::io::Result<DirectRecoveryReport> {
        match self.set.try_recover(connection) {
            Ok(report) => Ok(report),
            Err(error) => {
                let message = recovery_error_message(error);
                let _ = self.generation_invariant_fatal(now, causality, message);
                Err(std::io::Error::other(message))
            }
        }
    }

    pub(super) fn abandon_generation(
        &mut self,
        connection: ConnectionToken,
        reason: OwnerFailure,
        now: Moment,
        causality: Option<&mut CausalSequence>,
    ) -> std::io::Result<DirectRecoveryReport> {
        match self.set.abandon(connection, reason) {
            Ok(report) => Ok(report),
            Err(error) => {
                let message = recovery_error_message(error);
                let _ = self.generation_invariant_fatal(now, causality, message);
                Err(std::io::Error::other(message))
            }
        }
    }

    pub(super) fn stale_generation_fatal(
        &mut self,
        now: Moment,
        causality: Option<&mut CausalSequence>,
    ) -> std::io::Result<()> {
        self.generation_invariant_fatal(
            now,
            causality,
            "stale Bornera connection violated direct ownership",
        )
    }

    pub(super) fn generation_invariant_fatal(
        &mut self,
        now: Moment,
        mut causality: Option<&mut CausalSequence>,
        message: &'static str,
    ) -> std::io::Result<()> {
        let reason = self.recovery_fallback_reason();
        self.admission_open = false;
        self.clear_authentication_ownership();
        self.session_deadline = None;
        let _ = self.session_closed(now);
        self.connection = None;
        self.last_close_reason = Some(reason);
        self.mark_waiting();
        let _ = self.fail_remaining(
            &recovery(reason, Delivery::PossiblySent),
            causality.as_deref_mut(),
            None,
        );
        let _ = self.fail_pending(&recovery(reason, Delivery::NotSent), causality);
        self.terminal = true;
        Err(std::io::Error::other(message))
    }

    fn fail_context_unobserved(&self, key: OperationContextKey, failure: RequestError) {
        let Some(context) = self.contexts.release(key) else {
            return;
        };
        if let DirectOperationContext::Public(context) = context {
            let _ = context.fail(failure);
        }
    }

    fn recovery_fallback_reason(&self) -> CloseReason {
        self.generation_close_reason
            .or(self.last_close_reason)
            .unwrap_or(CloseReason::TransportLost(TransportFailure::Other))
    }
}

fn recovery_error_message(error: ConnectionRecoveryError) -> &'static str {
    match error {
        ConnectionRecoveryError::StaleConnection => {
            "stale Bornera connection violated direct ownership"
        }
        ConnectionRecoveryError::OwnerRunning => {
            "failed Bornera recovery unexpectedly remained live"
        }
        _ => "Bornera recovery violated direct ownership",
    }
}
