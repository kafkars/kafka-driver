//! Pre-reserved DNS ownership for one route into a discovered broker lane.

use kafka_driver_core::BrokerRoute;

use crate::{
    RequestError,
    reactor::{broker_set::BrokerSetError, resolver::ResolutionOwner},
    request::ErasedRequest,
};

use super::{Reactor, ReactorError, resolution::ResolutionPermit};

impl Reactor {
    pub(super) fn submit_broker_route(
        &mut self,
        route: BrokerRoute,
        request: Box<dyn ErasedRequest>,
        now: kafka_driver_core::Moment,
    ) -> Result<(), ReactorError> {
        let Some(resolution) = &mut self.resolution else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let resolution_lane = self.brokers.resolution_lane(route, request.traffic_class());
        let permit = if let Some(lane) = resolution_lane {
            let Some(permit) = resolution
                .try_reserve_broker(lane)
                .map_err(|error| ReactorError::host(std::io::Error::other(error)))?
            else {
                request.fail(RequestError::NameResolutionCapacityReached {
                    limit: resolution.capacity(),
                });
                return Ok(());
            };
            Some(permit)
        } else {
            None
        };
        let effect_id = permit.as_ref().map(ResolutionPermit::effect_id);
        let dns = self
            .brokers
            .submit_route(&self.poller, route, effect_id, request, now);
        let dns = match dns {
            Ok(dns) => dns,
            Err(error) => {
                if let Some(permit) = permit {
                    resolution.cancel(permit);
                }
                return Err(ReactorError::broker_set(error));
            }
        };
        let Some((lane, dns)) = dns else {
            if let Some(permit) = permit {
                resolution.cancel(permit);
            }
            return Ok(());
        };
        let Some(permit) = permit else {
            return Err(ReactorError::broker_set(
                BrokerSetError::ResolutionPermitMissing,
            ));
        };
        debug_assert_eq!(permit.owner(), ResolutionOwner::Broker(lane));
        resolution
            .submit(permit, dns)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        Ok(())
    }
}
