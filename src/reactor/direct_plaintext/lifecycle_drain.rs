//! Exact shutdown ownership across live, timed, and resolving Direct states.

use std::io;

use kafka_driver_core::{
    AddressRefreshState, BrokerDisposition, BrokerEffect, BrokerInput, BrokerState,
};

use super::lifecycle::{DirectLifecycle, invariant};

impl DirectLifecycle {
    pub(super) fn begin_drain(&mut self) -> io::Result<Vec<BrokerEffect>> {
        let before = self.state();
        let transition = self.broker.apply(BrokerInput::BeginDrain);
        let disposition = transition.disposition();
        let effects = transition.into_effects();
        match disposition {
            BrokerDisposition::Applied | BrokerDisposition::Ignored => {
                validate_drain(before, &effects)?;
                Ok(effects)
            }
            BrokerDisposition::IgnoredStale => Err(invariant("direct drain was rejected as stale")),
        }
    }
}

fn validate_drain(before: BrokerState, effects: &[BrokerEffect]) -> io::Result<()> {
    let exact = match before {
        BrokerState::Dormant { .. }
        | BrokerState::Refreshing {
            refresh: AddressRefreshState::Pending { .. } | AddressRefreshState::Resolving { .. },
            ..
        }
        | BrokerState::Draining { .. }
        | BrokerState::Closed { .. } => effects.is_empty(),
        BrokerState::Connecting { epoch, .. } | BrokerState::Available { epoch } => {
            effects == [BrokerEffect::DrainConnection { epoch }]
        }
        BrokerState::Backoff { timer_id, .. } => {
            effects == [BrokerEffect::CancelReconnect { timer_id }]
        }
        BrokerState::Refreshing {
            refresh: AddressRefreshState::Backoff { timer_id, .. },
            ..
        } => effects == [BrokerEffect::CancelEndpointRefreshRetry { timer_id }],
    };
    exact
        .then_some(())
        .ok_or_else(|| invariant("direct drain effect diverged from prior lifecycle state"))
}
