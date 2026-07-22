//! States retaining only resolution ownership valid for one broker identity.

use crate::{BrokerEndpoint, BrokerRoute, ConnectionEpoch, DnsFailure, EffectId};

/// Current advertised endpoint resolution state for one Kafka broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerResolutionState {
    /// No route has requested this broker yet.
    Dormant,
    /// One exact metadata route and connection generation own external DNS work.
    Resolving {
        /// Generation-fenced broker route being activated.
        route: BrokerRoute,
        /// Endpoint advertised by that route's generation.
        endpoint: BrokerEndpoint,
        /// Connection generation reserved for a successful child.
        epoch: ConnectionEpoch,
        /// External DNS identity currently owned.
        effect_id: EffectId,
    },
    /// The route completed resolution and transferred its address set.
    Resolved {
        /// Route whose addresses were transferred.
        route: BrokerRoute,
        /// Connection generation assigned to the child.
        epoch: ConnectionEpoch,
    },
    /// The route reached a sanitized terminal DNS failure.
    Failed {
        /// Route whose endpoint failed resolution.
        route: BrokerRoute,
        /// Connection generation whose activation failed.
        epoch: ConnectionEpoch,
        /// Sanitized terminal failure.
        failure: DnsFailure,
    },
}
