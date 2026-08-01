//! Bounded immutable Kafka request-header client identity.

use kafka_wire_core::StrBytes;

pub(crate) const MAX_CLIENT_ID_BYTES: usize = i16::MAX as usize;

/// One validated client identifier shared by every broker connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClientId(StrBytes);

impl ClientId {
    pub(crate) fn try_new(value: String) -> Result<Self, ClientIdError> {
        if value.len() > MAX_CLIENT_ID_BYTES {
            return Err(ClientIdError {
                actual: value.len(),
                limit: MAX_CLIENT_ID_BYTES,
            });
        }
        Ok(Self(StrBytes::from(value)))
    }

    pub(crate) const fn wire(&self) -> &StrBytes {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClientIdError {
    pub(crate) actual: usize,
    pub(crate) limit: usize,
}
