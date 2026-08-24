//! Connection-start failures kept distinct from peer authentication outcomes.

/// Why a configured local authentication session could not start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuthenticationSessionStartError {
    /// A validated SCRAM configuration no longer satisfied its construction invariant.
    ScramConfigurationInvalid,
    /// The operating-system-backed SCRAM nonce source was unavailable.
    ScramNonceUnavailable,
}
