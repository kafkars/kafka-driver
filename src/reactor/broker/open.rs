//! Admission of one broker connection generation into timer and transport ownership.

use kafka_driver_core::{ConnectionEffect, ConnectionEpoch, ConnectionInput, Moment};

use crate::reactor::{Poller, resource::ResourceIdentity, timer::DeadlineTimer};

use super::{BrokerError, failure::open_failure, owner::SingleBroker};

impl SingleBroker {
    pub(super) fn open_connection(
        &mut self,
        poller: &Poller,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> Result<(), BrokerError> {
        if self.resource_token.is_some() {
            return Err(BrokerError::MissingEffect);
        }
        self.connection = Self::connection_machine(
            epoch,
            self.connection_limits,
            self.sasl.as_ref(),
            self.authentication_limits,
        );
        self.negotiation_exchange = None;
        self.authentication_session = None;
        self.authentication_exchange = None;
        self.frames.clear();
        self.completed_writes.clear();
        self.retry_read = false;
        self.retry_write = false;

        let Some(deadline) = now.checked_add(self.connect_timeout) else {
            return Err(BrokerError::DeadlineOverflow);
        };
        let Some(open) = self.ids.reserve_open() else {
            return Err(BrokerError::IdentityExhausted);
        };
        let transition = self.connection.apply(ConnectionInput::Start {
            effect_id: open.effect_id,
            transport_id: open.transport_id,
            deadline_timer: open.deadline_timer,
            deadline,
        })?;
        let effects = transition.into_effects();
        let [
            ConnectionEffect::ScheduleOpenDeadline {
                epoch: deadline_epoch,
                timer_id,
                at,
            },
            ConnectionEffect::OpenTransport {
                epoch: opened_epoch,
                effect_id,
                transport_id,
            },
        ] = effects.as_slice()
        else {
            return Err(unexpected_connection_effect(&effects));
        };
        if *deadline_epoch != epoch
            || *opened_epoch != epoch
            || *effect_id != open.effect_id
            || *transport_id != open.transport_id
            || *timer_id != open.deadline_timer
            || *at != deadline
        {
            return Err(BrokerError::MissingEffect);
        }
        if self
            .timers
            .schedule(DeadlineTimer::for_open(*timer_id, epoch, *at))
            .is_err()
        {
            self.apply_open_failed(
                epoch,
                *effect_id,
                *transport_id,
                kafka_driver_core::TransportFailure::Other,
            )?;
            return Ok(());
        }
        let identity = ResourceIdentity::new(*transport_id, epoch);
        let Some(address) = self.addresses.next() else {
            return Err(BrokerError::MissingEffect);
        };
        match self.resources.open(poller, identity, address) {
            Ok(token) => self.resource_token = Some(token),
            Err(error) => {
                self.apply_open_failed(epoch, *effect_id, *transport_id, open_failure(&error))?;
            }
        }
        Ok(())
    }
}

fn unexpected_connection_effect(effects: &[ConnectionEffect]) -> BrokerError {
    effects
        .first()
        .copied()
        .map_or(BrokerError::MissingEffect, BrokerError::UnexpectedEffect)
}
