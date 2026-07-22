//! Construction and graceful closure of one public dedicated driver host.

use kafka_driver::{Driver, DriverHost, SaslConfig, TlsClientConfig};

use crate::error::ProbeError;

pub(crate) struct ProbeSession {
    pub(super) driver: Driver,
    host: DriverHost,
}

impl ProbeSession {
    pub(crate) fn spawn(bootstrap: kafka_driver::BootstrapSet) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().bootstrap(bootstrap))
    }

    pub(crate) fn spawn_sasl(
        bootstrap: kafka_driver::BootstrapSet,
        sasl: SaslConfig,
    ) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().bootstrap(bootstrap).sasl(sasl))
    }

    pub(crate) fn spawn_tls(
        address: std::net::SocketAddr,
        tls: TlsClientConfig,
    ) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().rustls_broker(address, tls))
    }

    pub(crate) fn spawn_tls_sasl(
        address: std::net::SocketAddr,
        tls: TlsClientConfig,
        sasl: SaslConfig,
    ) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().rustls_broker(address, tls).sasl(sasl))
    }

    fn spawn_builder(builder: kafka_driver::DriverBuilder) -> Result<Self, ProbeError> {
        let (driver, host) = builder
            .spawn()
            .map_err(|source| ProbeError::stage("start dedicated driver", source))?;
        Ok(Self { driver, host })
    }

    pub(crate) fn close(self) -> Result<(), ProbeError> {
        let Self { driver, host } = self;
        let shutdown = driver
            .shutdown()
            .map_err(|source| ProbeError::stage("admit graceful shutdown", source));
        let shutdown = shutdown.and_then(|call| {
            call.wait()
                .map_err(|source| ProbeError::stage("wait for graceful shutdown", source))
        });
        drop(driver);
        let joined = host
            .join()
            .map_err(|source| ProbeError::stage("join dedicated driver", source));
        shutdown.and(joined)
    }
}
