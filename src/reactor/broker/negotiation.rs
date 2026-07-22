//! Interpretation of the machine-owned initial `ApiVersions` exchange.

use kafka_driver_core::{
    CallId, ConnectionEffect, ConnectionInput, EffectId, Moment, NegotiationAttempt,
    NegotiationFailure, TransportFailure,
};
use kafka_driver_transport::FrameBody;

use crate::{
    negotiation::{NegotiationExchange, negotiate},
    reactor::{PollInterest, Poller, resource::ResourceIdentity, timer::DeadlineTimer},
};

use super::{BrokerError, owner::SingleBroker};

const NEGOTIATION_CALL: CallId = CallId::from_raw(0);

impl SingleBroker {
    pub(super) fn begin_negotiation(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        open_effect: EffectId,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let Some(ids) = self.ids.reserve_negotiation() else {
            return Err(BrokerError::IdentityExhausted);
        };
        let Some(deadline) = now.checked_add(self.negotiation_timeout) else {
            return Err(BrokerError::DeadlineOverflow);
        };
        let transition = self.connection.apply(ConnectionInput::TransportOpened {
            epoch: identity.epoch(),
            effect_id: open_effect,
            transport_id: identity.transport_id(),
            negotiation: NegotiationAttempt::new(ids.effect_id, ids.deadline_timer, now, deadline),
        })?;
        let effects = transition.into_effects();
        let [
            ConnectionEffect::ScheduleNegotiationDeadline {
                epoch,
                timer_id,
                at,
            },
            ConnectionEffect::NegotiateApiVersions {
                epoch: exchange_epoch,
                transport_id,
                effect_id,
                correlation_id,
            },
        ] = effects.as_slice()
        else {
            return Err(unexpected_or_missing(&effects));
        };
        if *epoch != identity.epoch()
            || *exchange_epoch != identity.epoch()
            || *transport_id != identity.transport_id()
            || *timer_id != ids.deadline_timer
            || *effect_id != ids.effect_id
        {
            return Err(BrokerError::MissingEffect);
        }
        if self
            .timers
            .schedule(DeadlineTimer::for_negotiation(*timer_id, *epoch, *at))
            .is_err()
        {
            return self.fail_negotiation(
                poller,
                identity,
                *effect_id,
                NegotiationFailure::Capacity,
            );
        }
        let (exchange, frame) = match NegotiationExchange::start(
            *effect_id,
            *correlation_id,
            self.outbound_frame,
            self.negotiation_limits.decode_limits(),
        ) {
            Ok(exchange) => exchange,
            Err(error) => {
                return self.fail_negotiation(poller, identity, *effect_id, error.failure());
            }
        };
        let Some(token) = self.resource_token else {
            return self.fail_negotiation(
                poller,
                identity,
                *effect_id,
                NegotiationFailure::Malformed,
            );
        };
        let admitted = self
            .resources
            .get_mut(token)
            .is_some_and(|(observed, connection)| {
                observed == identity
                    && connection
                        .admit_write(NEGOTIATION_CALL, *effect_id, frame)
                        .is_ok()
            });
        if !admitted {
            return self.fail_negotiation(
                poller,
                identity,
                *effect_id,
                NegotiationFailure::Capacity,
            );
        }
        self.negotiation_exchange = Some(exchange);
        if self
            .resources
            .reregister(poller, token, PollInterest::ReadWrite)
            .is_err()
        {
            self.transport_lost(poller, identity, TransportFailure::Other)?;
        }
        Ok(())
    }

    pub(super) fn process_negotiation_frame(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        frame: FrameBody,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let Some(exchange) = self.negotiation_exchange.take() else {
            return Err(BrokerError::MissingEffect);
        };
        let effect_id = exchange.effect_id();
        let response = match exchange.finish(frame) {
            Ok(response) => response,
            Err(error) => {
                return self.fail_negotiation(poller, identity, effect_id, error.failure());
            }
        };
        let capabilities = match negotiate(response, self.negotiation_limits) {
            Ok(capabilities) => capabilities,
            Err(error) => {
                return self.fail_negotiation(poller, identity, effect_id, error.failure());
            }
        };
        if self.sasl.is_some() {
            return self.begin_authentication(poller, identity, effect_id, capabilities, now);
        }
        let transition = self
            .connection
            .apply(ConnectionInput::ApiVersionsNegotiated {
                epoch: identity.epoch(),
                transport_id: identity.transport_id(),
                effect_id,
                capabilities,
            })?;
        let effects = transition.into_effects();
        let [ConnectionEffect::CancelDeadline { timer_id }] = effects.as_slice() else {
            return Err(unexpected_or_missing(&effects));
        };
        self.timers.cancel(*timer_id);
        self.mark_connection_ready(identity.epoch())?;
        let Some(token) = self.resource_token else {
            return Err(BrokerError::MissingEffect);
        };
        if self
            .resources
            .reregister(poller, token, PollInterest::Readable)
            .is_err()
        {
            self.transport_lost(poller, identity, TransportFailure::Other)?;
        }
        Ok(())
    }

    fn fail_negotiation(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effect_id: EffectId,
        failure: NegotiationFailure,
    ) -> Result<(), BrokerError> {
        self.negotiation_exchange = None;
        let transition = self.connection.apply(ConnectionInput::ApiVersionsFailed {
            epoch: identity.epoch(),
            transport_id: identity.transport_id(),
            effect_id,
            failure,
        })?;
        self.interpret_close(poller, transition.into_effects(), None)
    }
}

fn unexpected_or_missing(effects: &[ConnectionEffect]) -> BrokerError {
    effects
        .first()
        .copied()
        .map_or(BrokerError::MissingEffect, BrokerError::UnexpectedEffect)
}
