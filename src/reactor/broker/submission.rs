//! Ordered interpretation of one call admission and its emitted effects.

use kafka_driver_core::{
    CallId, ConnectionEffect, ConnectionInput, ConnectionPhase, ConnectionState, Moment,
    TransportFailure,
};

use crate::{
    RequestError,
    reactor::{PollInterest, Poller, resource::ResourceIdentity, timer::DeadlineTimer},
    request::ErasedRequest,
};

use super::{BrokerError, owner::SingleBroker, terminal::expect_no_effects};

impl SingleBroker {
    pub(in crate::reactor) fn submit(
        &mut self,
        poller: &Poller,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let call_id = request.call_id();
        let api_key = request.api_key();
        let version = self.connection.negotiated_version(api_key);
        if self.connection.state().phase() == ConnectionPhase::Ready && version.is_none() {
            request.fail(RequestError::ApiUnavailable { api_key });
            return Ok(());
        }
        let Some(deadline) = now.checked_add(request.timeout()) else {
            request.fail(RequestError::DeadlineOverflow);
            return Ok(());
        };
        let Some(ids) = self.ids.reserve_submission() else {
            request.fail(RequestError::IdentityConflict);
            return Err(BrokerError::IdentityExhausted);
        };
        let transition = self.connection.apply(ConnectionInput::Submit {
            call_id,
            write_effect: ids.write_effect,
            deadline_timer: ids.deadline_timer,
            now,
            deadline,
        })?;
        self.interpret_submission(poller, request, version, transition.into_effects())?;
        self.reconcile_connection(poller, now)
    }

    fn interpret_submission(
        &mut self,
        poller: &Poller,
        request: Box<dyn ErasedRequest>,
        version: Option<kafka_wire_core::ApiVersion>,
        effects: Vec<ConnectionEffect>,
    ) -> Result<(), BrokerError> {
        let submitted_call = request.call_id();
        let mut request = Some(request);
        for effect in effects {
            match effect {
                ConnectionEffect::ScheduleDeadline {
                    epoch,
                    call_id,
                    timer_id,
                    at,
                } => {
                    if call_id != submitted_call {
                        return Err(ownership_error(submitted_call, call_id));
                    }
                    let deadline = DeadlineTimer::for_call(timer_id, epoch, call_id, at);
                    if self.timers.schedule(deadline).is_err() {
                        fail_unprepared(&mut request, RequestError::IdentityConflict);
                        let Some(pending) = self.connection.pending_call(call_id) else {
                            return Err(BrokerError::MissingEffect);
                        };
                        self.abort_write(poller, pending.write_effect(), Some(submitted_call))?;
                        return Ok(());
                    }
                }
                ConnectionEffect::WriteRequest {
                    epoch,
                    transport_id,
                    call_id,
                    correlation_id,
                    effect_id,
                } => {
                    if call_id != submitted_call {
                        return Err(ownership_error(submitted_call, call_id));
                    }
                    let Some(owned) = request.take() else {
                        return Err(BrokerError::MissingEffect);
                    };
                    let Some(version) = version else {
                        return Err(BrokerError::MissingEffect);
                    };
                    let Ok(frame) = owned.prepare(
                        correlation_id,
                        version,
                        self.outbound_frame,
                        &mut self.responses,
                    ) else {
                        self.abort_write(poller, effect_id, Some(call_id))?;
                        return Ok(());
                    };
                    let Some(token) = self.resource_token else {
                        self.abort_write(poller, effect_id, None)?;
                        return Ok(());
                    };
                    let identity = ResourceIdentity::new(transport_id, epoch);
                    let admitted =
                        self.resources
                            .get_mut(token)
                            .is_some_and(|(observed, connection)| {
                                observed == identity
                                    && connection.admit_write(call_id, effect_id, frame).is_ok()
                            });
                    if !admitted {
                        self.abort_write(poller, effect_id, None)?;
                        return Ok(());
                    }
                    if self
                        .resources
                        .reregister(poller, token, PollInterest::ReadWrite)
                        .is_err()
                    {
                        self.abort_write(poller, effect_id, None)?;
                        return Ok(());
                    }
                    let transition = self.connection.apply(ConnectionInput::WriteSubmitted {
                        epoch,
                        transport_id,
                        effect_id,
                    })?;
                    expect_no_effects(&transition.into_effects())?;
                }
                ConnectionEffect::FailCall {
                    call_id,
                    failure,
                    delivery,
                } => {
                    let Some(owned) = request.take() else {
                        return Err(BrokerError::MissingEffect);
                    };
                    if owned.call_id() != call_id {
                        return Err(ownership_error(owned.call_id(), call_id));
                    }
                    owned.fail(RequestError::Rejected { failure, delivery });
                }
                unexpected => return Err(BrokerError::UnexpectedEffect(unexpected)),
            }
        }
        if request.is_some() {
            return Err(BrokerError::MissingEffect);
        }
        Ok(())
    }

    fn abort_write(
        &mut self,
        poller: &Poller,
        effect_id: kafka_driver_core::EffectId,
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
        let transition = self.connection.apply(ConnectionInput::WriteFailed {
            epoch,
            transport_id,
            effect_id,
            failure: TransportFailure::Other,
        })?;
        self.interpret_close(poller, transition.into_effects(), settled_call)
    }

    #[cfg(test)]
    pub(super) fn admitted_counts(&mut self) -> (usize, usize, usize) {
        let queued = self
            .resource_token
            .and_then(|token| self.resources.get_mut(token))
            .map_or(0, |(_, connection)| connection.queued_write_frames());
        (self.responses.pending(), self.timers.len(), queued)
    }
}

fn fail_unprepared(request: &mut Option<Box<dyn ErasedRequest>>, failure: RequestError) {
    if let Some(request) = request.take() {
        request.fail(failure);
    }
}

fn ownership_error(expected: CallId, observed: CallId) -> BrokerError {
    BrokerError::RequestOwnership { expected, observed }
}
