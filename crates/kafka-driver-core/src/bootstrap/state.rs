//! States containing only data valid during one bootstrap attempt.

use crate::{BrokerEndpoint, ConnectionEpoch, DnsFailure, EffectId};

/// Observable phase and owned identity for bootstrap selection and resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapState {
    /// No bootstrap resolution is currently owned.
    Dormant,
    /// One configured endpoint is awaiting its identity-matched DNS outcome.
    Resolving {
        /// Connection generation that will own a successful result.
        epoch: ConnectionEpoch,
        /// DNS effect whose outcome is current.
        effect_id: EffectId,
        /// Configured endpoint currently being resolved.
        endpoint: BrokerEndpoint,
        /// Other configured endpoints still available in this attempt.
        remaining: usize,
    },
    /// A usable bounded address set was returned to the connection owner.
    Resolved {
        /// Connection generation that received the result.
        epoch: ConnectionEpoch,
    },
    /// Every configured endpoint failed once.
    Exhausted {
        /// Connection generation whose bootstrap attempt ended.
        epoch: ConnectionEpoch,
        /// Sanitized failure from the final configured endpoint.
        last_failure: DnsFailure,
    },
}
