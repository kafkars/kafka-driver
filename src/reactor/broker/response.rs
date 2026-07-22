//! Frame inspection followed by machine-approved typed FIFO completion.

use kafka_driver_core::{ConnectionEffect, ConnectionInput, ResponseFault};
use kafka_driver_transport::FrameBody;

use crate::{
    reactor::{Poller, resource::ResourceIdentity},
    response::{ResponseDispatchError, ResponseEnvelope, ResponseInspectError},
};

use super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(super) fn process_frames(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
    ) -> Result<bool, BrokerError> {
        let frames = std::mem::take(&mut self.frames);
        let mut processed = false;
        for frame in frames.iter().cloned() {
            if self.resource_token.is_none() {
                break;
            }
            processed = true;
            self.process_frame(poller, identity, frame)?;
        }
        self.frames = frames;
        self.frames.clear();
        Ok(processed)
    }

    fn process_frame(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        frame: FrameBody,
    ) -> Result<(), BrokerError> {
        let envelope = match self.responses.inspect_front(frame) {
            Ok(envelope) => envelope,
            Err(ResponseInspectError::NoPendingResponse { .. }) => {
                return self.reject_frame(poller, identity, ResponseFault::Unexpected);
            }
            Err(ResponseInspectError::HeaderDecode { .. }) => {
                return self.reject_frame(poller, identity, ResponseFault::Malformed);
            }
        };
        let transition = self.machine.apply(ConnectionInput::ResponseReceived {
            epoch: identity.epoch(),
            transport_id: identity.transport_id(),
            correlation_id: envelope.correlation_id(),
        })?;
        self.interpret_response(poller, transition.into_effects(), envelope)
    }

    fn reject_frame(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        fault: ResponseFault,
    ) -> Result<(), BrokerError> {
        let transition = self.machine.apply(ConnectionInput::ResponseRejected {
            epoch: identity.epoch(),
            transport_id: identity.transport_id(),
            fault,
        })?;
        self.interpret_close(poller, transition.into_effects(), None)
    }

    fn interpret_response(
        &mut self,
        poller: &Poller,
        effects: Vec<ConnectionEffect>,
        envelope: ResponseEnvelope,
    ) -> Result<(), BrokerError> {
        let mut envelope = Some(envelope);
        let mut terminal = Vec::new();
        for effect in effects {
            match effect {
                ConnectionEffect::CancelDeadline { timer_id } if terminal.is_empty() => {
                    self.timers.cancel(timer_id);
                }
                ConnectionEffect::CompleteResponse {
                    call_id,
                    correlation_id,
                } if terminal.is_empty() => {
                    let Some(owned) = envelope.take() else {
                        return Err(BrokerError::MissingEffect);
                    };
                    match self
                        .responses
                        .complete_verified(call_id, correlation_id, owned)
                    {
                        Ok(_) | Err(ResponseDispatchError::BodyDecode { .. }) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                other => terminal.push(other),
            }
        }
        if !terminal.is_empty() {
            self.interpret_close(poller, terminal, None)?;
        }
        Ok(())
    }
}
