//! Atomic connect, generational admission, Mio registration, and release.

use std::{fmt, io, net::SocketAddr, num::NonZeroUsize};

use crate::reactor::{
    PollInterest, Poller,
    plaintext::{PlaintextConnection, PlaintextLimits},
};

use super::{
    ResourceAdmissionFailure, ResourceIdentity, ResourceToken, registry::ResourceRegistry,
};

/// Reactor-owned set of registered plaintext transport resources.
#[derive(Debug)]
pub(in crate::reactor) struct PlaintextResources {
    connections: ResourceRegistry<PlaintextConnection>,
    limits: PlaintextLimits,
}

impl PlaintextResources {
    pub(in crate::reactor) fn new(capacity: NonZeroUsize, limits: PlaintextLimits) -> Self {
        Self {
            connections: ResourceRegistry::new(capacity),
            limits,
        }
    }

    pub(in crate::reactor) fn open(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        address: SocketAddr,
    ) -> Result<ResourceToken, ResourceOpenError> {
        let connection = PlaintextConnection::connect(address, self.limits)
            .map_err(ResourceOpenError::Connect)?;
        let token = self
            .connections
            .admit(identity, connection)
            .map_err(|error| {
                let failure = error.failure();
                drop(error.into_resource());
                ResourceOpenError::Admission(failure)
            })?;
        let Some((_, connection)) = self.connections.get_mut(token) else {
            return Err(ResourceOpenError::RegistryInvariant);
        };
        if let Err(source) = poller.register(connection, token, PollInterest::ReadWrite) {
            drop(self.connections.remove(token));
            return Err(ResourceOpenError::Register(source));
        }
        Ok(token)
    }

    pub(in crate::reactor) fn get_mut(
        &mut self,
        token: ResourceToken,
    ) -> Option<(ResourceIdentity, &mut PlaintextConnection)> {
        self.connections.get_mut(token)
    }

    pub(in crate::reactor) fn reregister(
        &mut self,
        poller: &Poller,
        token: ResourceToken,
        interest: PollInterest,
    ) -> io::Result<bool> {
        let Some((_, connection)) = self.connections.get_mut(token) else {
            return Ok(false);
        };
        poller
            .reregister(connection, token, interest)
            .map(|()| true)
    }

    pub(in crate::reactor) fn close(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
    ) -> Result<bool, io::Error> {
        let Some(token) = self.connections.token_for(identity) else {
            return Ok(false);
        };
        let deregistration = self
            .connections
            .get_mut(token)
            .map_or(Ok(()), |(_, connection)| poller.deregister(connection));
        let removed = self.connections.remove(token).is_some();
        deregistration.map(|()| removed)
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn len(&self) -> usize {
        self.connections.len()
    }
}

/// Why a plaintext transport did not become a registered reactor resource.
#[derive(Debug)]
pub(in crate::reactor) enum ResourceOpenError {
    /// The OS rejected creation of the nonblocking connect attempt.
    Connect(io::Error),
    /// The bounded generational registry rejected resource admission.
    Admission(ResourceAdmissionFailure),
    /// Mio rejected registration and the admitted resource was rolled back.
    Register(io::Error),
    /// Internal registry admission could not be observed immediately afterward.
    RegistryInvariant,
}

impl fmt::Display for ResourceOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(_) => formatter.write_str("plaintext connect creation failed"),
            Self::Admission(failure) => write!(formatter, "resource admission failed: {failure}"),
            Self::Register(_) => formatter.write_str("plaintext poll registration failed"),
            Self::RegistryInvariant => {
                formatter.write_str("admitted plaintext resource could not be recovered")
            }
        }
    }
}

impl std::error::Error for ResourceOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(source) | Self::Register(source) => Some(source),
            Self::Admission(source) => Some(source),
            Self::RegistryInvariant => None,
        }
    }
}
