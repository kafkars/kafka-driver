//! Opaque host-test bridge for cluster seed fatal totality.

use std::{
    io,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerEndpoint, CallId, ConnectionEpoch, HostName, IpAddress, Moment, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::reactor::{
    broker::BrokerLimits,
    causality::CausalSequence,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        lane_plan::{BorneraLanePlan, KafkaSessionPlan},
        owner::DirectSet,
    },
};
use crate::{DriverLimits, RequestError, config::BrokerTemplate, request::erased_request};

use super::backend::ClusterBackend;

pub(in crate::reactor) struct ClusterSeedFatalFixture {
    backend: ClusterBackend,
}

impl ClusterSeedFatalFixture {
    pub(in crate::reactor) fn waiting() -> (
        Self,
        crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    ) {
        let mut backend =
            ClusterBackend::new(&DriverLimits::default(), BrokerTemplate::plaintext())
                .unwrap_or_else(|error| panic!("construct cluster fatal fixture: {error}"));
        let mut causality = CausalSequence::new();
        match &mut backend {
            ClusterBackend::Plaintext { runtime, .. } => {
                runtime
                    .install_seed(
                        ConnectionEpoch::from_raw(1),
                        failed_resolved_plan(),
                        Moment::ORIGIN,
                    )
                    .unwrap_or_else(|error| panic!("install exhausted seed fixture: {error}"));
            }
            #[cfg(feature = "tls-rustls")]
            ClusterBackend::Rustls { .. } => panic!("plaintext fixture selected Rustls"),
        }
        let (call, request) = erased_request(
            CallId::from_raw(91),
            ApiVersionsRequest::default(),
            Duration::from_secs(5),
        );
        match &mut backend {
            ClusterBackend::Plaintext { runtime, .. } => runtime
                .submit_seed(request, Moment::ORIGIN, &mut causality)
                .unwrap_or_else(|error| panic!("retain cluster fatal waiter: {error}")),
            #[cfg(feature = "tls-rustls")]
            ClusterBackend::Rustls { .. } => panic!("plaintext fixture selected Rustls"),
        }
        assert!(
            backend
                .prepare_seed_bootstrap_restart(Moment::ORIGIN, &mut causality)
                .unwrap_or_else(|error| panic!("prepare cluster bootstrap restart: {error}"))
        );
        assert!(
            backend
                .seed_bootstrap_restart_pending()
                .unwrap_or_else(|error| panic!("inspect prepared bootstrap restart: {error}"))
        );
        (Self { backend }, call)
    }

    pub(in crate::reactor) fn mark_resolution_owned(&mut self) {
        self.backend
            .mark_seed_bootstrap_resolution_owned()
            .unwrap_or_else(|error| panic!("transfer bootstrap resolution ownership: {error}"));
    }

    pub(in crate::reactor) fn totalize_host_error(&mut self, error: io::Error) -> io::Error {
        self.backend
            .finish_seed_host_result::<()>(Err(error))
            .err()
            .unwrap_or_else(|| panic!("host error must remain fatal"))
    }
}

fn failed_resolved_plan() -> BorneraLanePlan<TcpTransport> {
    let broker = BrokerLimits::default();
    BorneraLanePlan::new(
        crate::config::BrokerAddresses::Resolved {
            endpoint: endpoint(),
            addresses: addresses(),
        },
        broker,
        None,
        KafkaSessionPlan::new(None, broker),
        Box::new(RecoverableFailure),
    )
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("127.0.0.1").unwrap_or_else(|error| panic!("seed host: {error}")),
        NonZeroU16::MIN,
    )
}

fn addresses() -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            NonZeroU16::MIN,
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("seed addresses: {error}"))
}

struct RecoverableFailure;

impl DirectConnectionAttempt<TcpTransport> for RecoverableFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: BorneraLaneOwner,
        _address: SocketAddr,
        _epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}
