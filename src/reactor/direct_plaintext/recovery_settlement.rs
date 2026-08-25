//! Terminal semantic settlement of every owner transferred by Bornera recovery.

use bornera::{ConnectionEvent, OwnerFailure, RegisteredTransport, TransportDiagnostic};
use kafka_driver_core::{CloseReason, Delivery, KafkaSessionState, Moment, TransportFailure};

use crate::reactor::{bornera::OperationContextKey, causality::CausalSequence};

use super::{
    failure_translation::{connection_close_reason, diagnostic_close_reason, recovery},
    owner::{DirectOwner, DirectRecovery},
};
use crate::reactor::bornera::driver_delivery;

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn settle_recovery(
        &mut self,
        report: DirectRecovery,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        let bornera::RecoveryReport {
            epoch,
            reason,
            operations,
            unmatched_writes,
            outcomes,
            events,
            transport_diagnostic,
            transport_pressure: _transport_pressure,
            transport_retained_limit: _transport_retained_limit,
            transport_retained_ceiling: _transport_retained_ceiling,
            ownership_diverged: _ownership_diverged,
            ..
        } = report;
        let semantic_reason = self.last_close_reason;
        let recovered_reason = recovered_close_reason(
            self.session.state(),
            epoch,
            events,
            transport_diagnostic,
            reason,
        );
        let effective_reason = semantic_reason.unwrap_or(recovered_reason);
        self.last_close_reason = Some(effective_reason);

        for outcome in outcomes {
            self.settle_outcome(outcome, Moment::from_nanos(0), causality, false)?;
        }
        for recovered in operations {
            let key = OperationContextKey::new(epoch, recovered.operation);
            let failure = recovery(effective_reason, driver_delivery(recovered.delivery));
            self.fail_released(key, failure, causality)?;
        }
        for discarded in unmatched_writes {
            let key = OperationContextKey::new(epoch, discarded.operation);
            let failure = recovery(effective_reason, driver_delivery(discarded.delivery));
            self.fail_released(key, failure, causality)?;
        }
        let fallback = recovery(effective_reason, Delivery::PossiblySent);
        self.fail_remaining(&fallback, Some(causality), None)?;
        let pending = recovery(effective_reason, Delivery::NotSent);
        self.fail_pending(&pending, Some(causality))?;
        self.admission_open = false;
        if recovered_reason == CloseReason::Drained
            && matches!(self.session.state(), KafkaSessionState::Draining { .. })
        {
            self.session_drained_by_engine()?;
        }
        self.session_closed(Moment::from_nanos(0))?;
        self.last_close_reason = Some(effective_reason);
        self.terminal = true;
        self.latch_recovered_seed(epoch.get());
        Ok(())
    }
}

fn recovered_close_reason(
    session: KafkaSessionState,
    epoch: bornera_core::ConnectionEpoch,
    events: Vec<ConnectionEvent>,
    diagnostic: Option<TransportDiagnostic>,
    _owner_failure: OwnerFailure,
) -> CloseReason {
    let lifecycle = events
        .into_iter()
        .filter(|event| event.epoch() == epoch)
        .filter_map(|event| match event {
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
