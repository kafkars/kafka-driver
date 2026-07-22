//! Exact machine, timer, and completion cleanup for one locally unsent call.

use kafka_driver_core::{CallId, ConnectionEffect, ConnectionInput, ConnectionState, EffectId};

use crate::RequestError;

use super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(super) fn abort_unsent_call(
        &mut self,
        call_id: CallId,
        effect_id: EffectId,
        settled_call: Option<CallId>,
    ) -> Result<(), BrokerError> {
        let ConnectionState::Ready {
            epoch,
            transport_id,
            ..
        } = self.connection.state()
        else {
            return Err(BrokerError::MissingEffect);
        };
        let transition = self.connection.apply(ConnectionInput::AbortUnsentCall {
            epoch,
            transport_id,
            call_id,
            effect_id,
        })?;
        for effect in transition.into_effects() {
            match effect {
                ConnectionEffect::CancelDeadline { timer_id } => {
                    self.timers.cancel(timer_id);
                }
                ConnectionEffect::FailCall {
                    call_id: failed, ..
                } if settled_call == Some(failed) => {}
                ConnectionEffect::FailCall {
                    call_id,
                    failure,
                    delivery,
                } => {
                    self.responses.fail_locally_rejected(
                        call_id,
                        RequestError::Rejected { failure, delivery },
                    )?;
                }
                unexpected => return Err(BrokerError::UnexpectedEffect(unexpected)),
            }
        }
        Ok(())
    }
}
