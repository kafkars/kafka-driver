//! Atomic transport selection, generational registration, and release.

use std::{fmt, io, net::SocketAddr, num::NonZeroUsize};

use crate::{
    config::BrokerSecurity,
    reactor::{
        PollInterest, Poller,
        transport::{TransportConnectError, TransportConnection, TransportLimits},
    },
};

use super::{
    ResourceAdmissionFailure, ResourceIdentity, ResourceNamespace, ResourceToken,
    registry::ResourceRegistry,
};

/// Reactor-owned set of registered broker transport resources.
#[derive(Debug)]
pub(in crate::reactor) struct TransportResources {
    connections: ResourceRegistry<TransportConnection>,
    limits: TransportLimits,
    security: BrokerSecurity,
}

impl TransportResources {
    #[cfg(test)]
    pub(in crate::reactor) fn new(
        capacity: NonZeroUsize,
        limits: TransportLimits,
        security: BrokerSecurity,
    ) -> Self {
        Self::in_namespace(capacity, limits, security, ResourceNamespace::single())
    }

    pub(in crate::reactor) fn in_namespace(
        capacity: NonZeroUsize,
        limits: TransportLimits,
        security: BrokerSecurity,
        namespace: ResourceNamespace,
    ) -> Self {
        Self {
            connections: ResourceRegistry::in_namespace(capacity, namespace),
            limits,
            security,
        }
    }

    pub(in crate::reactor) fn open(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        address: SocketAddr,
    ) -> Result<ResourceToken, TransportOpenError> {
        let connection = TransportConnection::connect(address, self.limits, &self.security)
            .map_err(TransportOpenError::Connect)?;
        let token = self
            .connections
            .admit(identity, connection)
            .map_err(|error| {
                let failure = error.failure();
                drop(error.into_resource());
                TransportOpenError::Admission(failure)
            })?;
        let Some((_, connection)) = self.connections.get_mut(token) else {
            return Err(TransportOpenError::RegistryInvariant);
        };
        if let Err(source) = poller.register(connection, token, PollInterest::ReadWrite) {
            drop(self.connections.remove(token));
            return Err(TransportOpenError::Register(source));
        }
        Ok(token)
    }

    pub(in crate::reactor) fn get_mut(
        &mut self,
        token: ResourceToken,
    ) -> Option<(ResourceIdentity, &mut TransportConnection)> {
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

    pub(in crate::reactor) fn replace_security(&mut self, security: BrokerSecurity) {
        self.security = security;
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn len(&self) -> usize {
        self.connections.len()
    }
}

/// Why a selected broker transport did not become a registered resource.
#[derive(Debug)]
pub(in crate::reactor) enum TransportOpenError {
    Connect(TransportConnectError),
    Admission(ResourceAdmissionFailure),
    Register(io::Error),
    RegistryInvariant,
}

impl fmt::Display for TransportOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => error.fmt(formatter),
            Self::Admission(failure) => write!(formatter, "resource admission failed: {failure}"),
            Self::Register(_) => formatter.write_str("transport poll registration failed"),
            Self::RegistryInvariant => {
                formatter.write_str("admitted transport resource could not be recovered")
            }
        }
    }
}

impl std::error::Error for TransportOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(source) => Some(source),
            Self::Admission(source) => Some(source),
            Self::Register(source) => Some(source),
            Self::RegistryInvariant => None,
        }
    }
}
