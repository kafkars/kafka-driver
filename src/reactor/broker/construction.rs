//! Construction of one broker owner and its first connection epoch.

use kafka_driver_core::{BrokerMachine, ConnectionEpoch};
use kafka_wire_core::DecodeLimits;

use crate::{
    config::BrokerConfig,
    reactor::{
        entropy::JitterEntropy,
        resource::{ResourceNamespace, TransportResources},
        scram_proof::ScramProofSender,
        timer::TimerHeap,
    },
    response::ResponseRegistry,
};

use super::address_rotation::AddressRotation;
use super::{BrokerLimits, owner::SingleBroker};

impl SingleBroker {
    #[cfg(test)]
    pub(in crate::reactor) fn new(address: std::net::SocketAddr, limits: BrokerLimits) -> Self {
        Self::new_configured(BrokerConfig::plaintext(address), limits)
    }

    #[cfg(test)]
    pub(in crate::reactor) fn new_configured(config: BrokerConfig, limits: BrokerLimits) -> Self {
        Self::new_configured_in(config, limits, ResourceNamespace::single(), None)
    }

    pub(in crate::reactor) fn new_configured_in(
        config: BrokerConfig,
        limits: BrokerLimits,
        namespace: ResourceNamespace,
        scram_proof: Option<ScramProofSender>,
    ) -> Self {
        Self::new_configured_in_epoch(
            config,
            limits,
            namespace,
            ConnectionEpoch::from_raw(1),
            scram_proof,
        )
    }

    pub(in crate::reactor) fn new_configured_in_epoch(
        config: BrokerConfig,
        limits: BrokerLimits,
        namespace: ResourceNamespace,
        epoch: ConnectionEpoch,
        scram_proof: Option<ScramProofSender>,
    ) -> Self {
        let (addresses, security, sasl) = config.into_parts();
        let addresses = AddressRotation::new(addresses);
        let primary = addresses
            .primary()
            .unwrap_or_else(|| panic!("broker address ownership must be nonempty"));
        let resources = TransportResources::in_namespace(
            limits.resource_capacity(),
            limits.transport(),
            security,
            namespace,
        );
        let connection = Self::connection_machine(
            epoch,
            limits.connection(),
            sasl.as_ref(),
            limits.authentication(),
        );
        Self {
            limits,
            entropy: JitterEntropy::for_value(&primary),
            addresses,
            address_refresh: None,
            broker: BrokerMachine::new(epoch, limits.backoff()),
            connection,
            last_close_reason: None,
            connection_limits: limits.connection(),
            authentication_limits: limits.authentication(),
            ids: super::BrokerIds::new(),
            resources,
            resource_token: None,
            responses: ResponseRegistry::new(limits.response_capacity(), DecodeLimits::default()),
            timers: TimerHeap::new(limits.timer_capacity()),
            timer_budget: limits.timer_budget(),
            due_timers: Vec::new(),
            read_budget: limits.read_budget(),
            write_budget: limits.write_budget(),
            outbound_frame: limits.outbound_frame(),
            connect_timeout: limits.connect_timeout(),
            negotiation_exchange: None,
            negotiation_limits: limits.negotiation(),
            negotiation_timeout: limits.negotiation_timeout(),
            authentication_timeout: limits.authentication_timeout(),
            sasl,
            authentication_session: None,
            authentication_exchange: None,
            scram_proof,
            frames: Vec::new(),
            completed_writes: Vec::new(),
            retry_read: false,
            retry_write: false,
            write_frame_rejections: 0,
            write_byte_rejections: 0,
        }
    }
}
