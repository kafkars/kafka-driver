//! Explicit Kafka SCRAM bounds and the one validated client-config construction path.

use std::num::{NonZeroU32, NonZeroUsize};

use kafka_driver_core::SaslMechanism;
use sasl_scram::{
    Algorithm, ChannelBindingMode, ClientConfig, ClientPolicy, IterationPolicy, PolicyError,
    PreparationError, PreparedAuthenticationIdentity, PreparedCredentials, PreparedPassword,
    ResourceLimits, SaltPolicy, SecretBytes,
};

pub(super) const MAX_MESSAGE_BYTES: usize = 16 * 1024;
pub(super) const MAX_ATTRIBUTES: usize = 32;
pub(super) const MAX_IDENTITY_BYTES: usize = 1_024;
pub(super) const MAX_AUTHORIZATION_IDENTITY_BYTES: usize = 1_024;
pub(super) const MAX_NONCE_BYTES: usize = 256;
pub(super) const MAX_SALT_BYTES: usize = 1_024;
pub(super) const MAX_CHANNEL_BINDING_BYTES: usize = 8 * 1_024;
pub(super) const MAX_EXTENSION_BYTES: usize = 4_096;
pub(super) const MIN_ITERATIONS: u32 = 4_096;
pub(super) const MAX_ITERATIONS: u32 = 1_000_000;
pub(super) const MIN_SALT_BYTES: usize = 1;

pub(crate) fn kafka_scram_policy() -> Result<ClientPolicy, PolicyError> {
    let resources = ResourceLimits::builder()
        .max_message_bytes(MAX_MESSAGE_BYTES)
        .max_attributes(MAX_ATTRIBUTES)
        .max_identity_bytes(MAX_IDENTITY_BYTES)
        .max_authorization_identity_bytes(MAX_AUTHORIZATION_IDENTITY_BYTES)
        .max_nonce_bytes(MAX_NONCE_BYTES)
        .max_salt_bytes(MAX_SALT_BYTES)
        .max_channel_binding_bytes(MAX_CHANNEL_BINDING_BYTES)
        .max_extension_bytes(MAX_EXTENSION_BYTES)
        .build()?;
    let iterations = IterationPolicy::new(
        NonZeroU32::MIN.saturating_add(MIN_ITERATIONS - 1),
        NonZeroU32::MIN.saturating_add(MAX_ITERATIONS - 1),
    )?;
    let salt = SaltPolicy::new(
        NonZeroUsize::MIN.saturating_add(MIN_SALT_BYTES - 1),
        NonZeroUsize::MIN.saturating_add(MAX_SALT_BYTES - 1),
    )?;
    Ok(ClientPolicy::new(resources, iterations).with_salt_policy(salt))
}

pub(crate) fn kafka_scram_client_config(
    mechanism: SaslMechanism,
    username: &str,
    password: &[u8],
) -> Result<ClientConfig, ScramClientConfigError> {
    let algorithm = match mechanism {
        SaslMechanism::ScramSha256 => Algorithm::Sha256,
        SaslMechanism::ScramSha512 => Algorithm::Sha512,
        SaslMechanism::Plain => return Err(ScramClientConfigError::UnsupportedMechanism),
    };
    let identity = PreparedAuthenticationIdentity::from_protocol_profile(username)
        .map_err(ScramClientConfigError::Preparation)?;
    let password = PreparedPassword::from_protocol_profile(SecretBytes::new(password));
    let credentials = PreparedCredentials::from_protocol_profile(identity, None, password);
    ClientConfig::builder(algorithm)
        .credentials(credentials)
        .channel_binding(ChannelBindingMode::Unsupported)
        .policy(kafka_scram_policy().map_err(ScramClientConfigError::Policy)?)
        .build()
        .map_err(ScramClientConfigError::Policy)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScramClientConfigError {
    UnsupportedMechanism,
    Preparation(PreparationError),
    Policy(PolicyError),
}
