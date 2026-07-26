//! Request preparation and bounded ordered-writer admission for one write effect.

use std::time::Instant;

use kafka_driver_core::{ConnectionEffect, ConnectionInput};

use crate::{
    reactor::{PollInterest, Poller, resource::ResourceIdentity},
    request::ErasedRequest,
};

use super::{BrokerError, owner::SingleBroker, terminal::expect_no_effects};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriteRequestOutcome {
    Accepted,
    Settled,
}

impl SingleBroker {
    pub(super) fn interpret_write_request(
        &mut self,
        poller: &Poller,
        request: &mut Option<Box<dyn ErasedRequest>>,
        version: Option<kafka_wire_core::ApiVersion>,
        effect: ConnectionEffect,
    ) -> Result<WriteRequestOutcome, BrokerError> {
        let ConnectionEffect::WriteRequest {
            epoch,
            transport_id,
            call_id,
            correlation_id,
            effect_id,
        } = effect
        else {
            return Err(BrokerError::UnexpectedEffect(effect));
        };
        let Some(owned) = request.take() else {
            return Err(BrokerError::MissingEffect);
        };
        if owned.call_id() != call_id {
            return Err(BrokerError::RequestOwnership {
                expected: owned.call_id(),
                observed: call_id,
            });
        }
        let Some(version) = version else {
            return Err(BrokerError::MissingEffect);
        };
        let Ok(frame) = owned.prepare(
            correlation_id,
            version,
            self.outbound_frame,
            &mut self.responses,
        ) else {
            self.abort_unsent_call(call_id, effect_id, Some(call_id))?;
            return Ok(WriteRequestOutcome::Settled);
        };
        let Some(token) = self.resource_token else {
            self.abort_write(poller, effect_id, None)?;
            return Ok(WriteRequestOutcome::Settled);
        };
        let identity = ResourceIdentity::new(transport_id, epoch);
        let admission = self
            .resources
            .get_mut(token)
            .and_then(|(observed, connection)| {
                (observed == identity).then(|| connection.admit_write(call_id, effect_id, frame))
            });
        match admission {
            Some(Ok(_)) => {
                if !self.responses.mark_writer_admitted(call_id, Instant::now()) {
                    return Err(BrokerError::MissingEffect);
                }
            }
            Some(Err(error)) => {
                self.observe_write_rejection(error.failure());
                self.abort_unsent_call(call_id, effect_id, None)?;
                return Ok(WriteRequestOutcome::Settled);
            }
            None => {
                self.abort_write(poller, effect_id, None)?;
                return Ok(WriteRequestOutcome::Settled);
            }
        }
        if self
            .resources
            .reregister(poller, token, PollInterest::ReadWrite)
            .is_err()
        {
            self.abort_write(poller, effect_id, None)?;
            return Ok(WriteRequestOutcome::Settled);
        }
        let transition = self.connection.apply(ConnectionInput::WriteSubmitted {
            epoch,
            transport_id,
            effect_id,
        })?;
        expect_no_effects(&transition.into_effects())?;
        // Admission is local work; the socket's prior writable edge may already be consumed.
        self.retry_write = true;
        Ok(WriteRequestOutcome::Accepted)
    }
}
