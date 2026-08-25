//! Atomic public frame commit and semantic-context publication.

use std::time::Instant;

use bornera::{ConnectionCommitError, EngineCommitError};
use kafka_driver_core::CallFailure;

use crate::{RequestError, reactor::bornera::OperationContextKey};

use super::{
    failure_translation::{fail_context, not_sent},
    operation_owner::DirectOperationContext,
    owner::{DirectPlaintextOwner, message},
};
use crate::reactor::causality::CausalSequence;

impl DirectPlaintextOwner {
    pub(super) fn commit_public(
        &mut self,
        permit: bornera_core::OperationPermit,
        frame: bornera::OutboundFrame,
        reservation: crate::reactor::bornera::ContextReservation<DirectOperationContext>,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        match self.set.commit(self.connection, permit, frame) {
            Ok(operation) => {
                self.publish_public(operation, reservation)?;
                self.mark_runnable();
                Ok(())
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::AcceptedOwnerFailure {
                operation,
                ..
            })) => {
                self.publish_public(operation, reservation)?;
                if self.pending_recovery.is_none() {
                    self.pending_recovery =
                        Some(self.set.try_recover(self.connection).map_err(message)?);
                }
                Ok(())
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::Rejected(error))) => {
                drop(error.into_parts());
                fail_context(reservation.abort(), not_sent(CallFailure::LocallyRejected));
                Ok(())
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::OwnerFailed { .. })) => {
                fail_context(reservation.abort(), not_sent(CallFailure::LocallyRejected));
                self.pending_recovery =
                    Some(self.set.try_recover(self.connection).map_err(message)?);
                Ok(())
            }
            Err(ConnectionCommitError::StaleConnection { .. }) => {
                if let DirectOperationContext::Public(context) = reservation.abort() {
                    let observed = causality.outcome().map_err(message)?;
                    let _ = context.fail_observed(not_sent(CallFailure::Closed), observed);
                }
                let reason = super::failure_translation::close_reason(
                    bornera_core::CloseReason::TransportLost,
                );
                self.fail_remaining(
                    &super::failure_translation::recovery(
                        reason,
                        kafka_driver_core::Delivery::PossiblySent,
                    ),
                    Some(causality),
                    None,
                )?;
                self.fail_pending(
                    &super::failure_translation::recovery(
                        reason,
                        kafka_driver_core::Delivery::NotSent,
                    ),
                    Some(causality),
                )?;
                self.admission_open = false;
                self.session_closed(kafka_driver_core::Moment::from_nanos(0))?;
                self.last_close_reason = Some(reason);
                self.terminal = true;
                Ok(())
            }
            Err(_) => {
                fail_context(reservation.abort(), RequestError::IdentityConflict);
                Ok(())
            }
        }
    }

    fn publish_public(
        &mut self,
        operation: bornera_core::OperationId,
        mut reservation: crate::reactor::bornera::ContextReservation<DirectOperationContext>,
    ) -> std::io::Result<()> {
        reservation.bind(|context| {
            if let DirectOperationContext::Public(context) = context {
                context.mark_writer(Instant::now());
            }
        });
        let key = OperationContextKey::new(self.connection.epoch(), operation);
        if let Err(error) = reservation.publish(key) {
            fail_context(error.into_context(), RequestError::IdentityConflict);
            self.pending_recovery = Some(
                self.set
                    .abandon(self.connection, bornera::OwnerFailure::OwnerInvariant)
                    .map_err(message)?,
            );
        }
        Ok(())
    }
}
