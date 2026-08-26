//! Bounded work selection and no-spin readiness for pending route installation.

use std::{collections::BTreeSet, io};

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerEndpoint, BrokerId};

use super::{ClusterRuntime, route_turn::advance_cursor};

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn next_route_install_broker(&mut self) -> Option<BrokerId> {
        let brokers = self.route_install_work_brokers();
        if brokers.is_empty() {
            self.route_install_cursor = 0;
            return None;
        }
        let start = self.route_install_cursor % brokers.len();
        for offset in 0..brokers.len() {
            let index = advance_cursor(start, offset, brokers.len());
            let broker_id = brokers[index];
            if self.broker_install_can_progress(broker_id) {
                self.route_install_cursor = advance_cursor(index, 1, brokers.len());
                return Some(broker_id);
            }
        }
        None
    }

    pub(super) fn route_install_has_local_work(&self) -> bool {
        self.route_install_work_brokers()
            .into_iter()
            .any(|broker_id| self.broker_install_can_progress(broker_id))
    }

    pub(super) fn replacement_victim(&self, target: BrokerId) -> io::Result<Option<BrokerId>> {
        let mut blocked = None;
        for (&broker_id, family) in &self.families {
            if broker_id == target || self.family_is_advertised(broker_id) {
                continue;
            }
            if self.family_reclaimable(broker_id)? {
                return Ok(Some(broker_id));
            }
            if !family.is_retiring() {
                return Ok(Some(broker_id));
            }
            blocked = blocked.or(Some(broker_id));
        }
        Ok(blocked)
    }

    pub(super) fn family_is_advertised(&self, broker_id: BrokerId) -> bool {
        self.directory
            .as_ref()
            .and_then(|directory| directory.route_to(broker_id))
            .is_some()
    }

    pub(super) fn ensure_family_replacement_capacity(
        &self,
        target: BrokerId,
        retire: Option<BrokerId>,
    ) -> io::Result<()> {
        if self.families.contains_key(&target) != (retire == Some(target)) {
            return Err(io::Error::other(
                "Bornera replacement target family ownership diverged",
            ));
        }
        let retained = self
            .families
            .len()
            .checked_sub(usize::from(retire.is_some()))
            .and_then(|families| families.checked_add(1))
            .ok_or_else(|| io::Error::other("Bornera family replacement count overflowed"))?;
        if retained > self.family_capacity() {
            return Err(io::Error::other(
                "Bornera cluster broker family capacity reached",
            ));
        }
        Ok(())
    }

    fn route_install_work_brokers(&self) -> Vec<BrokerId> {
        let mut brokers = BTreeSet::new();
        for state in self.routes.values() {
            let Some(pending) = state.pending_install.as_ref() else {
                continue;
            };
            let family_is_stale =
                self.families
                    .get(&state.lane.broker_id())
                    .is_some_and(|family| {
                        family.is_retiring() || family.endpoint() != &pending.endpoint
                    });
            if !state.waiting.is_empty() || family_is_stale {
                brokers.insert(state.lane.broker_id());
            }
        }
        for (&broker_id, family) in &self.families {
            if family.is_retiring() {
                brokers.insert(broker_id);
            }
        }
        brokers.into_iter().collect()
    }

    fn broker_install_can_progress(&self, broker_id: BrokerId) -> bool {
        if self.pending_endpoint(broker_id).is_none() {
            return self
                .families
                .get(&broker_id)
                .is_some_and(super::family::BrokerFamily::is_retiring)
                && self.family_reclaimable(broker_id).unwrap_or(true);
        }
        if let Some(family) = self.families.get(&broker_id) {
            return if family.is_retiring() {
                self.family_reclaimable(broker_id).unwrap_or(true)
            } else {
                true
            };
        }
        if self.families.len() < self.family_capacity() {
            return true;
        }
        match self.replacement_victim(broker_id) {
            Ok(Some(victim)) => self.families.get(&victim).is_some_and(|family| {
                !family.is_retiring() || self.family_reclaimable(victim).unwrap_or(true)
            }),
            Ok(None) => false,
            Err(_) => true,
        }
    }

    fn pending_endpoint(&self, broker_id: BrokerId) -> Option<&BrokerEndpoint> {
        self.routes
            .values()
            .filter(|state| state.lane.broker_id() == broker_id)
            .find_map(|state| {
                state
                    .pending_install
                    .as_ref()
                    .map(|pending| &pending.endpoint)
            })
    }

    pub(super) fn family_capacity(&self) -> usize {
        self.driver
            .metadata()
            .broker_directory()
            .max_brokers()
            .get()
    }
}
