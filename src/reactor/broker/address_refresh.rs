//! Suspended reconnect ownership while newer endpoint evidence is resolved.

use kafka_driver_core::{
    AddressRefreshState, BrokerDisposition, BrokerEndpoint, BrokerInput, BrokerState,
    ConnectionEpoch, DnsFailure, EndpointRefreshSchedule, Moment, ResolvedAddressSet, TimerId,
};

use crate::reactor::Poller;

use super::{BrokerError, owner::SingleBroker};

#[derive(Debug)]
pub(super) struct AddressRefresh {
    endpoint: BrokerEndpoint,
    failed_epoch: ConnectionEpoch,
}

impl SingleBroker {
    pub(in crate::reactor) fn address_refresh_needed(&self) -> bool {
        matches!(
            (self.address_refresh.as_ref(), self.broker.state()),
            (
                Some(AddressRefresh { failed_epoch, .. }),
                BrokerState::Refreshing {
                    failed_epoch: current,
                    refresh: AddressRefreshState::Pending { .. },
                    ..
                },
            ) if *failed_epoch == current
        )
    }

    pub(in crate::reactor) fn take_address_refresh(
        &mut self,
    ) -> Result<Option<BrokerEndpoint>, BrokerError> {
        let Some(refresh) = self.address_refresh.as_ref() else {
            return Ok(None);
        };
        let failed_epoch = refresh.failed_epoch;
        let endpoint = refresh.endpoint.clone();
        match self.broker.state() {
            BrokerState::Refreshing {
                failed_epoch: current,
                refresh: AddressRefreshState::Pending { .. },
                ..
            } if current == failed_epoch => {}
            BrokerState::Refreshing {
                failed_epoch: current,
                refresh: AddressRefreshState::Resolving { .. } | AddressRefreshState::Backoff { .. },
                ..
            } if current == failed_epoch => return Ok(None),
            _ => return Err(BrokerError::MissingEffect),
        }
        let transition = self
            .broker
            .apply(BrokerInput::EndpointRefreshStarted { failed_epoch });
        require_refresh_applied(transition.disposition())?;
        expect_no_refresh_effects(&transition.into_effects())?;
        Ok(Some(endpoint))
    }

    pub(in crate::reactor) fn restore_address_refresh(&mut self) -> Result<(), BrokerError> {
        let failed_epoch = self.refresh_epoch()?;
        let transition = self
            .broker
            .apply(BrokerInput::EndpointRefreshDeferred { failed_epoch });
        require_refresh_applied(transition.disposition())?;
        expect_no_refresh_effects(&transition.into_effects())
    }

    pub(in crate::reactor) fn fail_address_refresh(
        &mut self,
        failure: DnsFailure,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let failed_epoch = self.refresh_epoch()?;
        let retry = match failure {
            DnsFailure::NoUsableAddress => None,
            DnsFailure::NameNotFound | DnsFailure::Temporary => self.reserve_endpoint_refresh(now),
        };
        let transition = self.broker.apply(BrokerInput::EndpointRefreshFailed {
            failed_epoch,
            failure,
            retry,
        });
        require_refresh_applied(transition.disposition())?;
        if matches!(self.broker.state(), BrokerState::Closed { .. }) {
            self.address_refresh = None;
        }
        self.interpret_broker_effects(poller, transition.into_effects(), now)
    }

    pub(in crate::reactor) fn finish_address_refresh(
        &mut self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let failed_epoch = self.refresh_epoch_for(&endpoint)?;
        let transition = self
            .broker
            .apply(BrokerInput::EndpointRefreshed { failed_epoch, now });
        require_refresh_applied(transition.disposition())?;
        self.addresses.replace(endpoint, addresses);
        self.address_refresh = None;
        self.interpret_broker_effects(poller, transition.into_effects(), now)?;
        self.reconcile_connection(poller, now)
    }

    pub(super) fn resume_after_refresh(
        &mut self,
        failed_epoch: ConnectionEpoch,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let transition = self
            .broker
            .apply(BrokerInput::EndpointRefreshed { failed_epoch, now });
        require_refresh_applied(transition.disposition())?;
        self.interpret_broker_effects(poller, transition.into_effects(), now)?;
        self.reconcile_connection(poller, now)
    }

    pub(super) fn begin_address_refresh(
        &mut self,
        endpoint: BrokerEndpoint,
        failed_epoch: ConnectionEpoch,
    ) -> Result<(), BrokerError> {
        if !matches!(
            self.broker.state(),
            BrokerState::Refreshing {
                failed_epoch: current,
                refresh: AddressRefreshState::Pending { .. },
                ..
            } if current == failed_epoch
        ) {
            return Err(BrokerError::MissingEffect);
        }
        self.address_refresh = Some(AddressRefresh {
            endpoint,
            failed_epoch,
        });
        Ok(())
    }

    pub(super) fn refresh_epoch(&self) -> Result<ConnectionEpoch, BrokerError> {
        let refresh = self
            .address_refresh
            .as_ref()
            .ok_or(BrokerError::MissingEffect)?;
        matches!(
            self.broker.state(),
            BrokerState::Refreshing {
                failed_epoch,
                refresh: AddressRefreshState::Resolving { .. },
                ..
            } if failed_epoch == refresh.failed_epoch
        )
        .then_some(refresh.failed_epoch)
        .ok_or(BrokerError::MissingEffect)
    }

    fn refresh_epoch_for(&self, endpoint: &BrokerEndpoint) -> Result<ConnectionEpoch, BrokerError> {
        let refresh = self
            .address_refresh
            .as_ref()
            .ok_or(BrokerError::MissingEffect)?;
        if &refresh.endpoint != endpoint {
            return Err(BrokerError::MissingEffect);
        }
        self.refresh_epoch()
    }

    pub(super) fn deliver_address_refresh_retry(
        &mut self,
        poller: &Poller,
        failed_epoch: ConnectionEpoch,
        timer_id: TimerId,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let transition = self.broker.apply(BrokerInput::EndpointRefreshRetryElapsed {
            failed_epoch,
            timer_id,
            now,
        });
        self.interpret_broker_effects(poller, transition.into_effects(), now)
    }

    fn reserve_endpoint_refresh(&mut self, now: Moment) -> Option<EndpointRefreshSchedule> {
        let timer_id = self.ids.reserve_policy_timer()?;
        Some(EndpointRefreshSchedule::new(
            timer_id,
            now,
            self.entropy.next_sample(),
        ))
    }
}

fn require_refresh_applied(disposition: BrokerDisposition) -> Result<(), BrokerError> {
    match disposition {
        BrokerDisposition::Applied => Ok(()),
        BrokerDisposition::Ignored | BrokerDisposition::IgnoredStale => {
            Err(BrokerError::MissingEffect)
        }
    }
}

fn expect_no_refresh_effects(
    effects: &[kafka_driver_core::BrokerEffect],
) -> Result<(), BrokerError> {
    match effects.first().copied() {
        Some(effect) => Err(BrokerError::UnexpectedBrokerEffect(effect)),
        None => Ok(()),
    }
}
