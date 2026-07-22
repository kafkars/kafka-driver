//! Public SASL credentials with validation and secret-free diagnostics.

use std::{fmt, sync::Arc};

use kafka_driver_core::SaslMechanism;
use stringprep::saslprep;
use zeroize::Zeroize;

/// Validated credentials and mechanism selection for broker authentication.
#[must_use]
#[derive(Clone)]
pub struct SaslConfig {
    mechanism: SaslMechanism,
    authorization_identity: Arc<str>,
    username: Arc<str>,
    password: Arc<SecretText>,
}

impl SaslConfig {
    /// Creates SASL PLAIN credentials using an empty authorization identity.
    pub fn plain(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, SaslConfigError> {
        let username = username.into();
        let password = password.into();
        validate_username(&username)?;
        validate_plain_field(&password, SaslConfigError::PasswordContainsNul)?;
        Ok(Self {
            mechanism: SaslMechanism::Plain,
            authorization_identity: Arc::from(""),
            username: Arc::from(username),
            password: Arc::new(SecretText(password)),
        })
    }

    /// Creates SASL SCRAM-SHA-256 credentials after applying `SASLprep`.
    pub fn scram_sha_256(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, SaslConfigError> {
        Self::scram(SaslMechanism::ScramSha256, username, password)
    }

    /// Creates SASL SCRAM-SHA-512 credentials after applying `SASLprep`.
    pub fn scram_sha_512(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, SaslConfigError> {
        Self::scram(SaslMechanism::ScramSha512, username, password)
    }

    /// Replaces the SASL PLAIN authorization identity.
    pub fn with_authorization_identity(
        mut self,
        authorization_identity: impl Into<String>,
    ) -> Result<Self, SaslConfigError> {
        let authorization_identity = authorization_identity.into();
        if self.mechanism != SaslMechanism::Plain && !authorization_identity.is_empty() {
            return Err(SaslConfigError::UnsupportedAuthorizationIdentity);
        }
        validate_plain_field(
            &authorization_identity,
            SaslConfigError::AuthorizationIdentityContainsNul,
        )?;
        self.authorization_identity = Arc::from(authorization_identity);
        Ok(self)
    }

    fn scram(
        mechanism: SaslMechanism,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, SaslConfigError> {
        let username = username.into();
        let password = password.into();
        let username = saslprep(&username)
            .map_err(|_| SaslConfigError::UsernamePreparation)?
            .into_owned();
        let password = saslprep(&password)
            .map_err(|_| SaslConfigError::PasswordPreparation)?
            .into_owned();
        if username.is_empty() {
            return Err(SaslConfigError::EmptyUsername);
        }
        Ok(Self {
            mechanism,
            authorization_identity: Arc::from(""),
            username: Arc::from(username),
            password: Arc::new(SecretText(password)),
        })
    }

    pub(crate) const fn mechanism(&self) -> SaslMechanism {
        self.mechanism
    }

    pub(crate) fn authorization_identity(&self) -> &str {
        &self.authorization_identity
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn password(&self) -> &str {
        &self.password.0
    }
}

impl fmt::Debug for SaslConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SaslConfig")
            .field("mechanism", &self.mechanism)
            .finish_non_exhaustive()
    }
}

/// Why SASL credentials could not form an unambiguous mechanism message.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SaslConfigError {
    /// A Kafka username must identify a principal.
    EmptyUsername,
    /// The PLAIN username contained its field delimiter.
    UsernameContainsNul,
    /// The PLAIN password contained its field delimiter.
    PasswordContainsNul,
    /// The PLAIN authorization identity contained its field delimiter.
    AuthorizationIdentityContainsNul,
    /// The SCRAM username could not be normalized safely.
    UsernamePreparation,
    /// The SCRAM password could not be normalized safely.
    PasswordPreparation,
    /// A nonempty authorization identity is not supported for SCRAM.
    UnsupportedAuthorizationIdentity,
}

impl fmt::Display for SaslConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyUsername => "the SASL username is empty",
            Self::UsernameContainsNul => "the SASL username contains a NUL delimiter",
            Self::PasswordContainsNul => "the SASL password contains a NUL delimiter",
            Self::AuthorizationIdentityContainsNul => {
                "the SASL authorization identity contains a NUL delimiter"
            }
            Self::UsernamePreparation => "the SCRAM username failed SASLprep",
            Self::PasswordPreparation => "the SCRAM password failed SASLprep",
            Self::UnsupportedAuthorizationIdentity => {
                "SCRAM authorization identities are not supported"
            }
        })
    }
}

impl std::error::Error for SaslConfigError {}

struct SecretText(String);

impl Drop for SecretText {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

fn validate_username(username: &str) -> Result<(), SaslConfigError> {
    if username.is_empty() {
        return Err(SaslConfigError::EmptyUsername);
    }
    validate_plain_field(username, SaslConfigError::UsernameContainsNul)
}

fn validate_plain_field(value: &str, failure: SaslConfigError) -> Result<(), SaslConfigError> {
    if value.as_bytes().contains(&0) {
        Err(failure)
    } else {
        Ok(())
    }
}
