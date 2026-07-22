//! Ordered transport close, timer cancellation, and FIFO call failure effects.

use kafka_driver_core::{CallId, ConnectionEffect, ConnectionInput, TransportFailure};

use crate::{
    RequestError,
    reactor::{Poller, resource::ResourceIdentity},
};

use super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(super) fn transport_lost(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        failure: TransportFailure,
    ) -> Result<(), BrokerError> {
        self.close_resource(poller, identity)?;
        let transition = self.machine.apply(ConnectionInput::TransportClosed {
            epoch: identity.epoch(),
            transport_id: identity.transport_id(),
            failure,
        })?;
        self.interpret_close(poller, transition.into_effects(), None)
    }

    pub(super) fn interpret_close(
        &mut self,
        poller: &Poller,
        effects: Vec<ConnectionEffect>,
        settled_call: Option<CallId>,
    ) -> Result<(), BrokerError> {
        for effect in effects {
            match effect {
                ConnectionEffect::CloseTransport {
                    epoch,
                    transport_id,
                    ..
                } => {
                    let identity = ResourceIdentity::new(transport_id, epoch);
                    self.close_resource(poller, identity)?;
                    let transition = self.machine.apply(ConnectionInput::TransportClosed {
                        epoch,
                        transport_id,
                        failure: TransportFailure::Other,
                    })?;
                    expect_no_effects(&transition.into_effects())?;
                }
                ConnectionEffect::CancelDeadline { timer_id } => {
                    self.timers.cancel(timer_id);
                }
                ConnectionEffect::FailCall { call_id, .. } if settled_call == Some(call_id) => {}
                ConnectionEffect::FailCall {
                    call_id,
                    failure,
                    delivery,
                } => {
                    self.responses
                        .fail_verified(call_id, RequestError::Rejected { failure, delivery })?;
                }
                unexpected => return Err(BrokerError::UnexpectedEffect(unexpected)),
            }
        }
        Ok(())
    }
}

pub(super) fn expect_no_effects(effects: &[ConnectionEffect]) -> Result<(), BrokerError> {
    match effects.first().copied() {
        Some(effect) => Err(BrokerError::UnexpectedEffect(effect)),
        None => Ok(()),
    }
}
