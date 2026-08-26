//! Route-local failure policy and host-fatal waiter totality.

use std::io;

use bornera::RegisteredTransport;
#[cfg(test)]
use kafka_driver_core::OutcomeStamp;
use kafka_driver_core::{BrokerRoute, BrokerState, ConnectionEpoch, DnsFailure};

use crate::{RequestError, reactor::BrokerLane};

use super::{ClusterRuntime, route_resolution::RouteResolutionProgress};

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn sync_route_failures(
        &mut self,
        causality: &mut crate::reactor::causality::CausalSequence,
    ) -> io::Result<bool> {
        let probes = self
            .routes
            .iter()
            .map(|(&lane, state)| {
                let broker = state
                    .installed
                    .as_ref()
                    .and_then(|installed| self.slots.get(&installed.owner))
                    .and_then(|&index| self.lanes.get(index))
                    .map(|physical| physical.lifecycle.state());
                (lane, broker)
            })
            .collect::<Vec<_>>();
        let mut progress = false;
        for (lane, broker) in probes {
            let Some(state) = self.routes.get_mut(&lane) else {
                continue;
            };
            if let Some(epoch) = broker.and_then(failed_epoch) {
                if state.last_connection_failure_epoch != Some(epoch) {
                    state.last_connection_failure_epoch = Some(epoch);
                    state.route_failure_at = Some(
                        causality
                            .outcome()
                            .map_err(|error| io::Error::other(error.to_string()))?,
                    );
                    progress = true;
                }
            } else if matches!(broker, Some(BrokerState::Available { .. }))
                && state.waiting.is_empty()
                && state.route_failure_at.take().is_some()
            {
                progress = true;
            }
        }
        Ok(progress)
    }

    pub(super) fn finish_resolution_failure(
        &mut self,
        lane: BrokerLane,
        route: BrokerRoute,
        failure: DnsFailure,
    ) -> io::Result<RouteResolutionProgress> {
        let current = self
            .routes
            .get(&lane)
            .and_then(|state| state.advertised.as_ref())
            .is_some_and(|advertised| {
                advertised.route == route && self.route_is_current(route, &advertised.endpoint)
            });
        if !current {
            return Ok(RouteResolutionProgress::Ignored);
        }
        let state = self
            .routes
            .get_mut(&lane)
            .ok_or_else(|| io::Error::other("Bornera route state is stale"))?;
        state.last_dns_failure = Some(failure);
        state.pending_install = None;
        state
            .waiting
            .fail_all(&RequestError::NameResolutionFailed { failure }, None);
        Ok(RouteResolutionProgress::Failed(failure))
    }

    #[cfg(test)]
    pub(super) fn record_route_failure(&mut self, lane: BrokerLane, observed_at: OutcomeStamp) {
        if let Some(state) = self.routes.get_mut(&lane) {
            state.route_failure_at = Some(observed_at);
        }
    }

    pub(super) fn settle_terminal_route_waiting(&mut self, budget: usize) -> io::Result<usize> {
        let mut settled = 0;
        let mut cursor = 0;
        let mut idle = 0;
        while settled < budget && idle < self.route_turn.len() {
            let lane = self.route_turn[cursor];
            cursor = (cursor + 1) % self.route_turn.len();
            let Some(failure) = self.route_terminal_failure(lane)? else {
                idle += 1;
                continue;
            };
            let Some(state) = self.routes.get_mut(&lane) else {
                idle += 1;
                continue;
            };
            if state
                .waiting
                .fail_bounded(&failure, state.route_failure_at, 1)
                == 0
            {
                idle += 1;
            } else {
                settled += 1;
                idle = 0;
            }
        }
        Ok(settled)
    }

    fn route_terminal_failure(&self, lane: BrokerLane) -> io::Result<Option<RequestError>> {
        if self.cluster_draining && self.routes.contains_key(&lane) {
            return Ok(Some(draining()));
        }
        let Some(advertised) = self
            .routes
            .get(&lane)
            .and_then(|state| state.advertised.as_ref())
        else {
            return Ok(None);
        };
        let Some(owner) = self.current_physical_owner(lane, &advertised.endpoint)? else {
            return Ok(None);
        };
        let index = self.index(owner)?;
        Ok(self.lanes[index].terminal_admission_failure())
    }

    pub(super) fn totalize_route_waiting_after_host_failure(&mut self) {
        let failure = closed();
        for state in self.routes.values_mut() {
            state.waiting.fail_all(&failure, state.route_failure_at);
        }
    }

    pub(super) fn route_waiting_has_local_work(&self) -> bool {
        if self.cluster_draining {
            return self.routes.values().any(|state| !state.waiting.is_empty());
        }
        self.routes.iter().any(|(lane, state)| {
            if state.waiting.is_empty() {
                return false;
            }
            let Some(advertised) = state.advertised.as_ref() else {
                return false;
            };
            let owner = match self.current_physical_owner(*lane, &advertised.endpoint) {
                Ok(Some(owner)) => owner,
                Ok(None) => return false,
                Err(_) => return true,
            };
            let Some(index) = self.slots.get(&owner).copied() else {
                return true;
            };
            self.lanes.get(index).is_none_or(|lane| {
                lane.can_admit_public() || lane.terminal_admission_failure().is_some()
            })
        })
    }
}

const fn failed_epoch(state: BrokerState) -> Option<ConnectionEpoch> {
    match state {
        BrokerState::Backoff { failed_epoch, .. }
        | BrokerState::Refreshing { failed_epoch, .. } => Some(failed_epoch),
        BrokerState::Dormant { .. }
        | BrokerState::Connecting { .. }
        | BrokerState::Available { .. }
        | BrokerState::Draining { .. }
        | BrokerState::Closed { .. } => None,
    }
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::Closed,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: kafka_driver_core::CallFailure::Draining,
        delivery: kafka_driver_core::Delivery::NotSent,
    }
}
