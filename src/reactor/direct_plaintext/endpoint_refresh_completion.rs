//! Resolver completion settlement for one exact Direct refresh fence.

#![allow(
    dead_code,
    reason = "the resolver host consumes this seam in the next bounded cutover"
)]

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{DnsFailure, Moment, ResolvedAddressSet};

use crate::reactor::causality::CausalSequence;

use super::{endpoint_refresh::DirectEndpointRefresh, owner::DirectLaneAccess};

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(in crate::reactor) fn fail_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
        failure: DnsFailure,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        let result = self.require_endpoint_refresh(refresh).and_then(|()| {
            let effects =
                self.lifecycle
                    .fail_endpoint_refresh(refresh.failed_epoch(), failure, now)?;
            if self.lifecycle.is_closed() {
                self.endpoint_refresh = None;
            }
            self.interpret_lifecycle_effects(effects, now, Some(causality))?;
            self.settle_refresh_policy_close(causality)
        });
        result.map_err(|error| self.host_fatal(error))
    }

    pub(in crate::reactor) fn finish_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
        addresses: ResolvedAddressSet,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        let result = self.require_endpoint_refresh(refresh).and_then(|()| {
            let effects = self
                .lifecycle
                .finish_endpoint_refresh(refresh.failed_epoch(), now)?;
            self.addresses
                .replace(refresh.endpoint().clone(), addresses);
            self.endpoint_refresh = None;
            self.interpret_lifecycle_effects(effects, now, Some(causality))
        });
        result.map_err(|error| self.host_fatal(error))
    }
}
