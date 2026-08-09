//! Authentication-frame admission and broker-local interest-failure containment.

use kafka_driver_core::{CallId, EffectId, TransportFailure};
use kafka_driver_transport::WriteAdmissionFailure;
use kafka_wire_core::Bytes;

use crate::reactor::{PollInterest, Poller, resource::ResourceIdentity};

use super::super::{BrokerError, owner::SingleBroker};

const AUTHENTICATION_CALL: CallId = CallId::from_raw(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AuthenticationWriteOutcome {
    Admitted,
    CapacityReached,
    ConnectionLost,
}

impl SingleBroker {
    pub(super) fn admit_authentication_write(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effect_id: EffectId,
        frame: Bytes,
    ) -> Result<AuthenticationWriteOutcome, BrokerError> {
        let token = self.resource_token.ok_or(BrokerError::MissingEffect)?;
        let (observed, connection) = self
            .resources
            .get_mut(token)
            .ok_or(BrokerError::MissingEffect)?;
        if observed != identity {
            return Err(BrokerError::MissingEffect);
        }
        let admitted = match connection.admit_write(AUTHENTICATION_CALL, effect_id, frame) {
            Ok(_) => AuthenticationWriteOutcome::Admitted,
            Err(error) => match error.failure() {
                WriteAdmissionFailure::FrameCapacityReached { .. }
                | WriteAdmissionFailure::ByteCapacityReached { .. } => {
                    AuthenticationWriteOutcome::CapacityReached
                }
                failure @ (WriteAdmissionFailure::FrameTooShort { .. }
                | WriteAdmissionFailure::IdentityInUse(_)) => {
                    return Err(BrokerError::AuthenticationWrite(failure));
                }
            },
        };
        if admitted == AuthenticationWriteOutcome::Admitted
            && self
                .resources
                .reregister(poller, token, PollInterest::ReadWrite)
                .is_err()
        {
            self.transport_lost(poller, identity, TransportFailure::Other)?;
            return Ok(AuthenticationWriteOutcome::ConnectionLost);
        }
        Ok(admitted)
    }
}
