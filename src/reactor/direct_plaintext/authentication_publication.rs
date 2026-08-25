//! Atomic SASL commit and semantic-context publication.

use bornera::{ConnectionCommitError, EngineCommitError, OutboundFrame, RegisteredTransport};
use bornera_core::{CommitErrorKind, FrameCommitFailure, WriteAdmissionFailure};
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
                self.recover_authentication_owner()?;
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::Rejected(error))) => {
                let disposition = authentication_commit_disposition(error.failure());
                drop(error.into_parts());
                drop(reservation.abort());
                match disposition {
                    AuthenticationCommitDisposition::Fail(failure) => {
                        self.fail_authentication_stage(stage, failure, now)?;
                    }
                    AuthenticationCommitDisposition::Lifecycle => {}
                    AuthenticationCommitDisposition::Recover => {
                        self.recover_authentication_owner()?;
                    }
                    AuthenticationCommitDisposition::Abandon => {
                        self.abandon_authentication_owner()?;
                    }
                }
            }
            Err(ConnectionCommitError::Connection(EngineCommitError::OwnerFailed {
                permit,
                frame,
                ..
            })) => {
                drop((permit, frame));
                drop(reservation.abort());
                self.recover_authentication_owner()?;
            }
            Err(ConnectionCommitError::StaleConnection { permit, frame }) => {
                drop((permit, frame));
                drop(reservation.abort());
                return Err(std::io::Error::other(
                    "stale connection rejected an authentication commit",
                ));
            }
            Err(error) => {
                drop(error);
                drop(reservation.abort());
                return Err(std::io::Error::other(
                    "unknown Bornera authentication commit failure",
                ));
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

    fn recover_authentication_owner(&mut self) -> std::io::Result<()> {
        if self.pending_recovery.is_none() {
            self.pending_recovery = Some(self.set.try_recover(self.connection).map_err(message)?);
        }
        Ok(())
    }

    fn abandon_authentication_owner(&mut self) -> std::io::Result<()> {
        if self.pending_recovery.is_none() {
            self.pending_recovery = Some(
                self.set
                    .abandon(self.connection, bornera::OwnerFailure::OwnerInvariant)
                    .map_err(message)?,
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AuthenticationCommitDisposition {
    Fail(AuthenticationFailure),
    Lifecycle,
    Recover,
    Abandon,
}

pub(super) const fn authentication_commit_disposition(
    failure: FrameCommitFailure,
) -> AuthenticationCommitDisposition {
    match failure {
        FrameCommitFailure::Policy(CommitErrorKind::AdmissionClosed) => {
            AuthenticationCommitDisposition::Lifecycle
        }
        FrameCommitFailure::Policy(CommitErrorKind::FrameTooLarge) => {
            AuthenticationCommitDisposition::Fail(AuthenticationFailure::PolicyLimitExceeded)
        }
        FrameCommitFailure::Policy(CommitErrorKind::OwnerPoisoned) => {
            AuthenticationCommitDisposition::Recover
        }
        FrameCommitFailure::Writer(
            WriteAdmissionFailure::FrameCapacityReached { .. }
            | WriteAdmissionFailure::RetainedByteCapacity { .. },
        ) => AuthenticationCommitDisposition::Fail(AuthenticationFailure::LocalCapacity),
        FrameCommitFailure::Policy(CommitErrorKind::ForeignPermit)
        | FrameCommitFailure::Writer(
            WriteAdmissionFailure::StaleEpoch { .. } | WriteAdmissionFailure::IdentityInUse(_),
        ) => AuthenticationCommitDisposition::Abandon,
        FrameCommitFailure::Policy(_) | FrameCommitFailure::Writer(_) | _ => {
            AuthenticationCommitDisposition::Abandon
        }
    }
}
