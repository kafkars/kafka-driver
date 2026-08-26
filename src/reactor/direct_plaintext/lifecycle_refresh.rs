//! DNS policy transitions for a suspended Direct reconnect.

use std::io;

use kafka_driver_core::{BrokerEffect, BrokerInput, DnsFailure, Moment};

use super::lifecycle::{DirectLifecycle, invariant};

impl DirectLifecycle {
    pub(super) fn defer_endpoint_refresh(
        &mut self,
        failed_epoch: kafka_driver_core::ConnectionEpoch,
    ) -> io::Result<()> {
        let effects = self.apply(BrokerInput::EndpointRefreshDeferred { failed_epoch })?;
        expect_no_effects(&effects)
    }

    pub(super) fn fail_endpoint_refresh(
        &mut self,
        failed_epoch: kafka_driver_core::ConnectionEpoch,
        failure: DnsFailure,
        now: Moment,
    ) -> io::Result<Vec<BrokerEffect>> {
        let retry = match failure {
            DnsFailure::NoUsableAddress => None,
            DnsFailure::NameNotFound | DnsFailure::Temporary => self.reserve_endpoint_refresh(now),
        };
        self.apply(BrokerInput::EndpointRefreshFailed {
            failed_epoch,
            failure,
            retry,
        })
    }

    pub(super) fn finish_endpoint_refresh(
        &mut self,
        failed_epoch: kafka_driver_core::ConnectionEpoch,
        now: Moment,
    ) -> io::Result<Vec<BrokerEffect>> {
        self.apply(BrokerInput::EndpointRefreshed { failed_epoch, now })
    }
}

fn expect_no_effects(effects: &[BrokerEffect]) -> io::Result<()> {
    if effects.is_empty() {
        Ok(())
    } else {
        Err(invariant(
            "direct endpoint-refresh deferral emitted an unexpected effect",
        ))
    }
}
