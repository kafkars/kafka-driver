//! Suspended reconnect ownership while newer endpoint evidence is resolved.

use kafka_driver_core::{
    BrokerDisposition, BrokerEndpoint, BrokerInput, ConnectionEpoch, Moment, ResolvedAddressSet,
};

use crate::reactor::Poller;

use super::{BrokerError, owner::SingleBroker};

#[derive(Debug)]
pub(super) enum AddressRefresh {
    Pending {
        endpoint: BrokerEndpoint,
        failed_epoch: ConnectionEpoch,
    },
    InFlight {
        endpoint: BrokerEndpoint,
        failed_epoch: ConnectionEpoch,
    },
}

impl SingleBroker {
    pub(in crate::reactor) const fn address_refresh_needed(&self) -> bool {
        matches!(self.address_refresh, Some(AddressRefresh::Pending { .. }))
    }

    pub(in crate::reactor) fn take_address_refresh(&mut self) -> Option<BrokerEndpoint> {
        match self.address_refresh.take()? {
            AddressRefresh::Pending {
                endpoint,
                failed_epoch,
            } => {
                self.address_refresh = Some(AddressRefresh::InFlight {
                    endpoint: endpoint.clone(),
                    failed_epoch,
                });
                Some(endpoint)
            }
            refresh @ AddressRefresh::InFlight { .. } => {
                self.address_refresh = Some(refresh);
                None
            }
        }
    }

    pub(in crate::reactor) fn restore_address_refresh(&mut self) -> Result<(), BrokerError> {
        match self
            .address_refresh
            .take()
            .ok_or(BrokerError::MissingEffect)?
        {
            AddressRefresh::InFlight {
                endpoint,
                failed_epoch,
            } => {
                self.address_refresh = Some(AddressRefresh::Pending {
                    endpoint,
                    failed_epoch,
                });
                Ok(())
            }
            refresh @ AddressRefresh::Pending { .. } => {
                self.address_refresh = Some(refresh);
                Err(BrokerError::MissingEffect)
            }
        }
    }

    pub(in crate::reactor) fn fail_address_refresh(&mut self) -> Result<(), BrokerError> {
        self.restore_address_refresh()
    }

    pub(in crate::reactor) fn finish_address_refresh(
        &mut self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let failed_epoch = self.refresh_epoch_for(&endpoint)?;
        self.addresses.replace(endpoint, addresses);
        self.address_refresh = None;
        self.resume_after_refresh(failed_epoch, poller, now)
    }

    pub(super) fn begin_address_refresh(
        &mut self,
        endpoint: BrokerEndpoint,
        failed_epoch: ConnectionEpoch,
    ) {
        self.address_refresh = Some(AddressRefresh::Pending {
            endpoint,
            failed_epoch,
        });
    }

    pub(super) fn refresh_epoch(&self) -> Result<ConnectionEpoch, BrokerError> {
        match self.address_refresh {
            Some(AddressRefresh::InFlight { failed_epoch, .. }) => Ok(failed_epoch),
            Some(AddressRefresh::Pending { .. }) | None => Err(BrokerError::MissingEffect),
        }
    }

    fn refresh_epoch_for(&self, endpoint: &BrokerEndpoint) -> Result<ConnectionEpoch, BrokerError> {
        match &self.address_refresh {
            Some(AddressRefresh::InFlight {
                endpoint: current,
                failed_epoch,
            }) if current == endpoint => Ok(*failed_epoch),
            Some(AddressRefresh::Pending { .. } | AddressRefresh::InFlight { .. }) | None => {
                Err(BrokerError::MissingEffect)
            }
        }
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
        if transition.disposition() != BrokerDisposition::Applied {
            return Err(BrokerError::MissingEffect);
        }
        self.interpret_broker_effects(poller, transition.into_effects(), now)?;
        self.reconcile_connection(poller, now)
    }
}
