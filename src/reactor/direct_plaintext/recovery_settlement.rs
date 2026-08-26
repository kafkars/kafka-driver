//! Terminal semantic settlement of every owner transferred by Bornera recovery.

use std::io;

use bornera::{ConnectionEvent, OwnerFailure, RegisteredTransport, TransportDiagnostic};
use kafka_driver_core::{
    BrokerPhase, CloseReason, Delivery, KafkaSessionState, Moment, TransportFailure,
};

use crate::reactor::causality::CausalSequence;

use super::{
    failure_translation::{connection_close_reason, diagnostic_close_reason, recovery},
    owner::{DirectLaneAccess, DirectRecovery},
    recovery_owners::RecoveredOwners,
};

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn settle_recovery(
        &mut self,
        recovery_report: DirectRecovery,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        self.clear_authentication_ownership();
        self.session_deadline = None;
        let DirectRecovery {
            report,
            mut semantic_diverged,
        } = recovery_report;
        let mut first_error = None;
        let bornera::RecoveryReport {
            epoch,
            reason: owner_failure,
            operations,
            unmatched_writes,
            outcomes,
            events,
            transport_diagnostic,
            transport_pressure: _transport_pressure,
            transport_retained_limit: _transport_retained_limit,
            transport_retained_ceiling: _transport_retained_ceiling,
            ownership_diverged,
            ..
        } = report;
        let recovered_admission = events.iter().any(|event| {
            matches!(event, ConnectionEvent::AdmissionOpened { epoch: opened, .. } if *opened == epoch)
        });
        if !semantic_diverged
            && recovered_admission
            && self.lifecycle.phase() == BrokerPhase::Connecting
        {
            record_error(
                &mut first_error,
                self.mark_generation_ready(super::reconnect::core_epoch(epoch)),
            );
        }
        let semantic_reason = self.generation_close_reason;
        let recovered_reason = recovered_close_reason(
            self.session.state(),
            epoch,
            &events,
            transport_diagnostic,
            owner_failure,
        );
        let effective_reason = semantic_reason.unwrap_or(recovered_reason);
        self.last_close_reason = Some(effective_reason);

        let owner_settlement = self.settle_recovered_owners(
            RecoveredOwners {
                epoch,
                operations,
                unmatched_writes,
                outcomes,
            },
            effective_reason,
            now,
            causality,
        );
        semantic_diverged |= owner_settlement.semantic_diverged;
        if let Some(error) = owner_settlement.first_error {
            keep_first(&mut first_error, error);
        }
        self.admission_open = false;
        if recovered_reason == CloseReason::Drained
            && matches!(self.session.state(), KafkaSessionState::Draining { .. })
        {
            record_error(&mut first_error, self.session_drained_by_engine());
        }
        record_error(&mut first_error, self.session_closed(now));
        self.generation_close_reason = None;
        self.last_close_reason = Some(effective_reason);
        self.mark_waiting();
        self.connection = None;

        let contexts = self.contexts.snapshot();
        let set_owner_failed = self.set.snapshot().owner_failure.is_some();
        if first_error.is_none()
            && recovery_can_reconnect(
                owner_failure,
                ownership_diverged,
                set_owner_failed,
                contexts,
                semantic_diverged,
            )
        {
            match self.settle_generation_lifecycle(
                super::reconnect::core_epoch(epoch),
                effective_reason,
                now,
                causality,
            ) {
                Ok(()) => return Ok(()),
                Err(error) => keep_first(&mut first_error, error),
            }
        }

        let pending = recovery(effective_reason, Delivery::NotSent);
        record_error(
            &mut first_error,
            self.fail_pending(&pending, Some(causality)),
        );
        self.terminal = true;
        Err(first_error.unwrap_or_else(|| {
            io::Error::other("fatal Bornera owner recovery cannot reuse the direct selector")
        }))
    }
}

fn record_error(first: &mut Option<io::Error>, result: io::Result<()>) {
    if let Err(error) = result {
        keep_first(first, error);
    }
}

fn keep_first(first: &mut Option<io::Error>, error: io::Error) {
    if first.is_none() {
        *first = Some(error);
    }
}

fn recovery_can_reconnect(
    failure: OwnerFailure,
    ownership_diverged: bool,
    set_owner_failed: bool,
    contexts: crate::reactor::bornera::OperationContextsSnapshot,
    semantic_diverged: bool,
) -> bool {
    matches!(failure, OwnerFailure::Core | OwnerFailure::OwnerInvariant)
        && !ownership_diverged
        && !set_owner_failed
        && contexts.reserved() == 0
        && contexts.published() == 0
        && contexts.retained_bytes() == calandria::RetainedBytes::ZERO
        && !contexts.is_poisoned()
        && !semantic_diverged
}

fn recovered_close_reason(
    session: KafkaSessionState,
    epoch: bornera_core::ConnectionEpoch,
    events: &[ConnectionEvent],
    diagnostic: Option<TransportDiagnostic>,
    _owner_failure: OwnerFailure,
) -> CloseReason {
    let lifecycle = events
        .iter()
        .filter(|event| event.epoch() == epoch)
        .filter_map(|event| match *event {
            ConnectionEvent::Closing {
                sequence, reason, ..
            }
            | ConnectionEvent::Closed {
                sequence, reason, ..
            } => Some((sequence, reason)),
            _ => None,
        })
        .max_by_key(|(sequence, _)| *sequence)
        .map(|(_, reason)| reason);
    if let Some(reason) = lifecycle {
        return connection_close_reason(reason, diagnostic);
    }
    diagnostic.map_or_else(
        || owner_failure_fallback(session),
        |diagnostic| {
            diagnostic_close_reason(
                matches!(session, KafkaSessionState::AwaitingTransport),
                diagnostic,
            )
        },
    )
}

fn owner_failure_fallback(session: KafkaSessionState) -> CloseReason {
    if matches!(session, KafkaSessionState::AwaitingTransport) {
        CloseReason::OpenFailed(TransportFailure::Other)
    } else {
        CloseReason::TransportLost(TransportFailure::Other)
    }
}
