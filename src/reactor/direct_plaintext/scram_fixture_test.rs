//! Mechanically open direct owner with independently prepared SCRAM continuations.

use std::{
    net::{TcpListener, TcpStream},
    num::{NonZeroU8, NonZeroUsize},
    time::Duration,
};

use bornera::TransportState;
use calandria::Span;
use kafka_driver_core::{
    AuthenticationRound, EffectId, KafkaSessionAuthenticationState, KafkaSessionDeadline,
    KafkaSessionInput, KafkaSessionState, Moment, NegotiatedApi, NegotiatedCapabilities,
};
use kafka_wire::{ApiVersionsRequest, KafkaRequest, SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_wire_core::ApiVersion;
use sasl_scram::PendingDerivation;

use crate::{
    DriverLimits, SaslConfig,
    authentication::{AuthenticationReceive, AuthenticationSession},
    reactor::scram_proof::ScramProofFence,
};

use super::{
    backend::DirectBackend,
    owner::{DirectPlaintextOwner, calandria_moment},
};

pub(in crate::reactor) const NOW: Moment = Moment::from_nanos(1);
pub(super) const DEADLINE: Moment = Moment::from_nanos(10_000_000_001);

pub(super) struct ScramOwnerFixture {
    pub(super) owner: DirectPlaintextOwner,
    _peer: TcpStream,
}

impl ScramOwnerFixture {
    pub(super) fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind direct SCRAM owner: {error}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("read direct SCRAM address: {error}"));
        let mut owner =
            DirectPlaintextOwner::new(&DriverLimits::default(), address, Some(config()), None, NOW)
                .unwrap_or_else(|error| panic!("construct direct SCRAM owner: {error}"));
        let (peer, _) = listener
            .accept()
            .unwrap_or_else(|error| panic!("accept direct SCRAM owner: {error}"));
        open_transport(&mut owner);
        Self { owner, _peer: peer }
    }

    pub(super) fn arm_first_proof(&mut self) -> PendingDerivation {
        arm_first_proof(&mut self.owner)
    }
}

impl DirectBackend {
    pub(in crate::reactor) fn arm_scram_proof_for_test(
        &mut self,
        effect_id: EffectId,
    ) -> ScramProofFence {
        let owner = match self {
            Self::Plaintext(owner) => owner.as_mut(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(_) => panic!("host SCRAM fixture requires plaintext"),
        };
        open_transport(owner);
        let pending = arm_first_proof(owner);
        owner
            .access()
            .dispatch_scram_proof(effect_id, first_round(), pending, NOW)
            .unwrap_or_else(|error| panic!("dispatch hosted direct proof: {error}"));
        owner
            .lane
            .pending_scram_proof
            .unwrap_or_else(|| panic!("hosted direct proof fence missing"))
    }

    pub(in crate::reactor) fn has_scram_sender_for_test(&self) -> bool {
        match self {
            Self::Plaintext(owner) => owner.lane.scram_proof_sender.is_some(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.lane.scram_proof_sender.is_some(),
        }
    }

    pub(in crate::reactor) fn has_pending_scram_proof_for_test(&self) -> bool {
        match self {
            Self::Plaintext(owner) => owner.lane.pending_scram_proof.is_some(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.lane.pending_scram_proof.is_some(),
        }
    }

    pub(in crate::reactor) fn scram_round_for_test(&self) -> Option<AuthenticationRound> {
        let state = match self {
            Self::Plaintext(owner) => owner.lane.session.state(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.lane.session.state(),
        };
        match state {
            KafkaSessionState::Authenticating {
                authentication: KafkaSessionAuthenticationState::Exchanging { round, .. },
                ..
            } => Some(round),
            _ => None,
        }
    }
}

fn arm_first_proof(owner: &mut DirectPlaintextOwner) -> PendingDerivation {
    drop(
        owner
            .lane
            .session
            .apply(KafkaSessionInput::TransportOpened {
                deadline: KafkaSessionDeadline::new(NOW, DEADLINE),
            }),
    );
    drop(
        owner
            .lane
            .session
            .apply(KafkaSessionInput::ApiVersionsSucceededWithAuthentication {
                capabilities: capabilities(),
                deadline: KafkaSessionDeadline::new(NOW, DEADLINE),
            }),
    );
    drop(
        owner
            .lane
            .session
            .apply(KafkaSessionInput::AuthenticationHandshakeSucceeded),
    );
    owner.lane.session_deadline = Some(DEADLINE);
    assert!(matches!(
        owner.lane.session.state(),
        KafkaSessionState::Authenticating {
            authentication: KafkaSessionAuthenticationState::Exchanging { round, .. },
            ..
        } if round == first_round()
    ));
    pending(
        owner
            .lane
            .authentication_session
            .as_mut()
            .unwrap_or_else(|| panic!("direct SCRAM session must remain owned")),
    )
}

pub(super) fn independent_pending() -> PendingDerivation {
    let mut session = AuthenticationSession::new(config())
        .unwrap_or_else(|error| panic!("construct independent SCRAM session: {error:?}"));
    pending(&mut session)
}

pub(super) const fn first_round() -> AuthenticationRound {
    AuthenticationRound::new(NonZeroU8::MIN)
}

fn config() -> SaslConfig {
    SaslConfig::scram_sha_256("proof-user", "proof-password")
        .unwrap_or_else(|error| panic!("construct direct SCRAM config: {error}"))
}

fn pending(session: &mut AuthenticationSession) -> PendingDerivation {
    let first = session
        .next_message(16 * 1_024)
        .unwrap_or_else(|error| panic!("create SCRAM client first: {error:?}"));
    let first = std::str::from_utf8(first.as_bytes())
        .unwrap_or_else(|error| panic!("decode SCRAM client first: {error}"));
    let nonce = first
        .rsplit_once("r=")
        .map_or_else(|| panic!("SCRAM client nonce missing"), |(_, nonce)| nonce);
    let challenge = format!("r={nonce}-server,s=YWJj,i=4096");
    let AuthenticationReceive::Derive(pending) = session.receive(challenge.as_bytes()) else {
        panic!("SCRAM server first must require proof derivation");
    };
    pending
}

fn capabilities() -> NegotiatedCapabilities {
    NegotiatedCapabilities::try_from_iter(
        [
            NegotiatedApi::new(SaslHandshakeRequest::API_KEY, ApiVersion::new(1)),
            NegotiatedApi::new(ApiVersionsRequest::API_KEY, ApiVersion::new(0)),
            NegotiatedApi::new(SaslAuthenticateRequest::API_KEY, ApiVersion::new(1)),
        ],
        NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("construct direct SCRAM capabilities: {error}"))
}

fn open_transport(owner: &mut DirectPlaintextOwner) {
    let wait = Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO);
    for _ in 0..16 {
        owner.connections.last_turn = owner
            .connections
            .set
            .turn_component(calandria_moment(NOW))
            .unwrap_or_else(|error| panic!("drive direct SCRAM transport: {error}"));
        if owner
            .connections
            .set
            .connection_snapshot(
                owner
                    .lane
                    .live_connection()
                    .unwrap_or_else(|error| panic!("read SCRAM fixture connection: {error}")),
            )
            .is_ok_and(|snapshot| snapshot.transport == TransportState::Open)
        {
            return;
        }
        owner
            .connections
            .set
            .poll_io(wait)
            .unwrap_or_else(|error| panic!("poll direct SCRAM transport: {error}"));
    }
    panic!("direct SCRAM transport did not open");
}
