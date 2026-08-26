//! One broker traffic lane's wait queue and lazy connection child.

use kafka_driver_core::{
    BrokerEndpoint, BrokerId, BrokerPhase, BrokerResolutionMachine, BrokerRoute, ConnectionEpoch,
    ConnectionPhase, Moment,
};

use crate::{
    config::BrokerConfig,
    reactor::{
        Poller,
        broker::{BrokerLimits, SingleBroker},
        resource::ResourceNamespace,
        route_waiting::{RouteWaiting, RouteWaitingOutcome},
    },
};

use super::{BrokerLane, BrokerSetError, replacement::PendingBroker};

pub(super) struct BrokerChild {
    pub(super) lane: BrokerLane,
    pub(super) resolution: BrokerResolutionMachine,
    pub(super) connection: Option<SingleBroker>,
    pub(super) route: Option<BrokerRoute>,
    pub(super) endpoint: Option<BrokerEndpoint>,
    pub(super) waiting: RouteWaiting,
    pub(super) namespace: ResourceNamespace,
    pub(super) limits: BrokerLimits,
    pub(super) next_epoch: Option<u64>,
    pub(super) pending_install: Option<PendingBroker>,
    pub(super) refresh_in_flight: bool,
    pub(super) last_dns_failure: Option<kafka_driver_core::DnsFailure>,
    pub(super) route_failure_at: Option<kafka_driver_core::OutcomeStamp>,
    pub(super) retired: bool,
    pub(super) retirement_started: bool,
}

impl BrokerChild {
    pub(super) fn new(
        lane: BrokerLane,
        namespace: ResourceNamespace,
        limits: BrokerLimits,
        waiting_calls: std::num::NonZeroUsize,
        waiting_bytes: std::num::NonZeroUsize,
        waiting_budget: std::num::NonZeroUsize,
    ) -> Self {
        Self {
            lane,
            resolution: BrokerResolutionMachine::new(lane.broker_id()),
            connection: None,
            route: None,
            endpoint: None,
            waiting: RouteWaiting::new(waiting_calls, waiting_bytes, waiting_budget),
            namespace,
            limits,
            next_epoch: Some(1),
            pending_install: None,
            refresh_in_flight: false,
            last_dns_failure: None,
            route_failure_at: None,
            retired: false,
            retirement_started: false,
        }
    }

    pub(super) const fn broker_id(&self) -> BrokerId {
        self.lane.broker_id()
    }

    pub(super) const fn lane(&self) -> BrokerLane {
        self.lane
    }

    pub(super) fn install(
        &mut self,
        config: BrokerConfig,
        endpoint: BrokerEndpoint,
        epoch: ConnectionEpoch,
        poller: &Poller,
        now: Moment,
        scram_proof: Option<crate::reactor::scram_proof::ScramProofSender>,
    ) -> Result<(), BrokerSetError> {
        let connection = match &mut self.connection {
            Some(connection) => {
                connection
                    .reconfigure(config, epoch)
                    .map_err(BrokerSetError::Broker)?;
                connection
            }
            None => self
                .connection
                .insert(SingleBroker::new_configured_in_epoch(
                    config,
                    self.limits,
                    self.namespace,
                    epoch,
                    scram_proof,
                )),
        };
        connection
            .start(poller, now)
            .map_err(BrokerSetError::Broker)?;
        self.endpoint = Some(endpoint);
        Ok(())
    }

    pub(super) fn admit_one(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let Some(connection) = &mut self.connection else {
            return Ok(false);
        };
        if connection.state().phase() != ConnectionPhase::Ready
            || connection.broker_state().phase() != BrokerPhase::Available
        {
            return Ok(false);
        }
        match self.waiting.pop(now, self.route_failure_at) {
            RouteWaitingOutcome::Empty => {
                self.route_failure_at = None;
                Ok(false)
            }
            RouteWaitingOutcome::Settled => {
                self.route_failure_at = None;
                Ok(true)
            }
            RouteWaitingOutcome::Ready(request) => {
                connection
                    .submit(poller, request, now)
                    .map_err(BrokerSetError::Broker)?;
                self.route_failure_at = None;
                Ok(true)
            }
        }
    }

    pub(super) fn capture_transport_failure(&mut self) {
        if let Some(observed_at) = self
            .connection
            .as_mut()
            .and_then(SingleBroker::take_transport_failure_at)
        {
            self.route_failure_at = Some(observed_at);
        }
        self.clear_recovered_route_failure_if_idle();
    }

    fn clear_recovered_route_failure_if_idle(&mut self) {
        if self.waiting.is_empty()
            && self.connection.as_ref().is_some_and(|connection| {
                connection.state().phase() == ConnectionPhase::Ready
                    && connection.broker_state().phase() == BrokerPhase::Available
            })
        {
            self.route_failure_at = None;
        }
    }
}
