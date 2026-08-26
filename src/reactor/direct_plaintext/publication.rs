//! Atomic public frame commit and semantic-context publication.

use std::time::Instant;

use bornera::{ConnectionCommitError, EngineCommitError, RegisteredTransport};
use kafka_driver_core::CallFailure;

use crate::{RequestError, reactor::bornera::OperationContextKey};

use super::{
    failure_translation::{fail_context, not_sent},
    operation_owner::DirectOperationContext,
    owner::{DirectLaneAccess, message},
};
use crate::reactor::causality::CausalSequence;

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn commit_public(
        &mut self,
        permit: bornera_core::OperationPermit,
        frame: bornera::OutboundFrame,
        reservation: crate::reactor::bornera::ContextReservation<DirectOperationContext>,
        now: kafka_driver_core::Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        let connection = self.live_connection()?;
        match self.set.commit(connection, permit, frame) {
            Ok(operation) => {
                self.publish_public(operation, reservation, now, causality)?;
                if self.pending_recovery.is_none() {
                    self.mark_runnable();
                }
                Ok(())
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::AcceptedOwnerFailure {
                operation,
                ..
            })) => {
                self.publish_public(operation, reservation, now, causality)?;
                if self.pending_recovery.is_none() {
                    let report =
                        self.recover_failed_generation(connection, now, Some(causality))?;
                    self.capture_recovery(report);
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
                let report = self.recover_failed_generation(connection, now, Some(causality))?;
                self.capture_recovery(report);
                Ok(())
            }
            Err(ConnectionCommitError::StaleConnection { permit, frame }) => {
                drop((permit, frame));
                if let DirectOperationContext::Public(context) = reservation.abort() {
                    match causality.outcome() {
                        Ok(observed) => {
                            let _ = context.fail_observed(not_sent(CallFailure::Closed), observed);
                        }
                        Err(error) => {
                            let _ = context.fail(not_sent(CallFailure::Closed));
                            let _ = self.stale_generation_fatal(now, Some(causality));
                            return Err(message(error));
                        }
                    }
                }
                self.stale_generation_fatal(now, Some(causality))
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
        now: kafka_driver_core::Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        reservation.bind(|context| {
            if let DirectOperationContext::Public(context) = context {
                context.mark_writer(Instant::now());
            }
        });
        let connection = self.live_connection()?;
        let key = OperationContextKey::new(connection.epoch(), operation);
        if let Err(error) = reservation.publish(key) {
            fail_context(error.into_context(), RequestError::IdentityConflict);
            let report = self.abandon_generation(
                connection,
                bornera::OwnerFailure::OwnerInvariant,
                now,
                Some(causality),
            )?;
            self.capture_diverged_recovery(report);
        }
        Ok(())
    }
}
