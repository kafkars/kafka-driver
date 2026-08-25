//! Atomic SASL commit and semantic-context publication.

use bornera::{ConnectionCommitError, EngineCommitError, OutboundFrame, RegisteredTransport};
use kafka_driver_core::{AuthenticationFailure, Moment};

use crate::reactor::bornera::{ContextReservation, OperationContextKey};

use super::{
    authentication_settlement::AuthenticationStageOwner,
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, message},
};

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn commit_authentication(
        &mut self,
        permit: bornera_core::OperationPermit,
        frame: OutboundFrame,
        reservation: ContextReservation<DirectOperationContext>,
        stage: AuthenticationStageOwner,
        now: Moment,
    ) -> std::io::Result<()> {
        match self.set.commit(self.connection, permit, frame) {
            Ok(operation) => {
                self.publish_authentication(operation, reservation)?;
                self.mark_runnable();
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::AcceptedOwnerFailure {
                operation,
                ..
            })) => {
                self.publish_authentication(operation, reservation)?;
                if self.pending_recovery.is_none() {
                    self.pending_recovery =
                        Some(self.set.try_recover(self.connection).map_err(message)?);
                }
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::Rejected(error))) => {
                drop(error.into_parts());
                drop(reservation.abort());
                self.fail_authentication_stage(stage, AuthenticationFailure::LocalCapacity, now)?;
            }
            Err(
                ConnectionCommitError::Connection(EngineCommitError::OwnerFailed { .. })
                | ConnectionCommitError::StaleConnection { .. },
            ) => {
                drop(reservation.abort());
                if self.pending_recovery.is_none() {
                    self.pending_recovery =
                        Some(self.set.try_recover(self.connection).map_err(message)?);
                }
            }
            Err(_) => {
                drop(reservation.abort());
                self.fail_authentication_stage(stage, AuthenticationFailure::LocalCapacity, now)?;
            }
        }
        Ok(())
    }

    fn publish_authentication(
        &mut self,
        operation: bornera_core::OperationId,
        reservation: ContextReservation<DirectOperationContext>,
    ) -> std::io::Result<()> {
        let key = OperationContextKey::new(self.connection.epoch(), operation);
        if let Err(error) = reservation.publish(key) {
            drop(error.into_context());
            self.pending_recovery = Some(
                self.set
                    .abandon(self.connection, bornera::OwnerFailure::OwnerInvariant)
                    .map_err(message)?,
            );
        }
        Ok(())
    }
}
