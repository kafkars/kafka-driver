//! Kafka session policy retained behind the compatibility connection machine.

use crate::{AuthenticationPolicy, ConnectionEpoch};
use kafka_wire_core::{ApiKey, ApiVersion};

use super::{ActiveConnection, ConnectionLimits, ConnectionPhase, ConnectionState, StateData};

/// Policy owner for Kafka negotiation, authentication, and usable-session state.
///
/// `ConnectionMachine` remains the public compatibility facade while transport
/// and operation ownership are migrated to their final adapters.
pub(super) struct KafkaSessionMachine {
    pub(super) state: StateData,
    pub(super) limits: ConnectionLimits,
    pub(super) authentication: Option<AuthenticationPolicy>,
}

impl KafkaSessionMachine {
    pub(super) const fn new(epoch: ConnectionEpoch, limits: ConnectionLimits) -> Self {
        Self {
            state: StateData::Dormant { epoch },
            limits,
            authentication: None,
        }
    }

    pub(super) const fn new_authenticated(
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
        authentication: AuthenticationPolicy,
    ) -> Self {
        Self {
            state: StateData::Dormant { epoch },
            limits,
            authentication: Some(authentication),
        }
    }

    pub(super) fn state(&self) -> ConnectionState {
        self.state.snapshot()
    }

    pub(super) const fn epoch(&self) -> ConnectionEpoch {
        self.state.epoch()
    }

    pub(super) const fn phase(&self) -> ConnectionPhase {
        self.state.phase()
    }

    pub(super) fn active(&self) -> Option<&ActiveConnection> {
        match &self.state {
            StateData::Active { connection, .. } => Some(connection),
            _ => None,
        }
    }

    pub(super) fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.active()
            .and_then(|connection| connection.negotiated_version(api_key))
    }

    pub(super) fn negotiated_api(&self, api_key: ApiKey) -> Option<crate::NegotiatedApi> {
        self.active()
            .and_then(|connection| connection.negotiated_api(api_key))
    }
}
