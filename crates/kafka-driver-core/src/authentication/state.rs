//! Authentication states containing only stage-valid, secret-free data.

use std::num::NonZeroU8;

use crate::{EffectId, Moment, TimerId};

use super::AuthenticationFailure;

/// One-based SASL client exchange identity within an authentication attempt.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AuthenticationRound(NonZeroU8);

impl AuthenticationRound {
    pub(crate) const FIRST: Self = Self(NonZeroU8::MIN);

    /// Creates a one-based round identity.
    pub const fn new(value: NonZeroU8) -> Self {
        Self(value)
    }

    /// Returns the one-based round value.
    pub const fn get(self) -> u8 {
        self.0.get()
    }

    pub(crate) fn next(self) -> Option<Self> {
        self.0
            .get()
            .checked_add(1)
            .and_then(NonZeroU8::new)
            .map(Self)
    }
}

/// Stable authentication lifecycle name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationPhase {
    /// No authentication work has been requested.
    Dormant,
    /// `SaslHandshake` is outstanding.
    Handshaking,
    /// One `SaslAuthenticate` exchange is outstanding.
    Exchanging,
    /// The broker accepted the complete mechanism exchange.
    Succeeded,
    /// Authentication ended with a sanitized terminal failure.
    Failed,
}

/// Immutable, secret-free authentication state snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationState {
    /// No authentication work has been requested.
    Dormant,
    /// `SaslHandshake` is outstanding.
    Handshaking {
        /// High-level authentication effect identity.
        effect_id: EffectId,
        /// Timer owning the whole authentication deadline.
        deadline_timer: TimerId,
        /// Absolute driver-relative deadline.
        deadline: Moment,
    },
    /// One `SaslAuthenticate` exchange is outstanding.
    Exchanging {
        /// High-level authentication effect identity.
        effect_id: EffectId,
        /// One-based exchange round being completed.
        round: AuthenticationRound,
        /// Timer owning the whole authentication deadline.
        deadline_timer: TimerId,
        /// Absolute driver-relative deadline.
        deadline: Moment,
    },
    /// The broker accepted the complete mechanism exchange.
    Succeeded,
    /// Authentication ended with a sanitized terminal failure.
    Failed {
        /// Secret-free terminal failure category.
        failure: AuthenticationFailure,
    },
}

impl AuthenticationState {
    /// Returns the lifecycle name without state-specific data.
    pub const fn phase(self) -> AuthenticationPhase {
        match self {
            Self::Dormant => AuthenticationPhase::Dormant,
            Self::Handshaking { .. } => AuthenticationPhase::Handshaking,
            Self::Exchanging { .. } => AuthenticationPhase::Exchanging,
            Self::Succeeded => AuthenticationPhase::Succeeded,
            Self::Failed { .. } => AuthenticationPhase::Failed,
        }
    }
}

pub(super) type StateData = AuthenticationState;
