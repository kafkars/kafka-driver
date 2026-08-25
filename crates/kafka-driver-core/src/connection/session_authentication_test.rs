//! SASL session scenarios without transport or operation identities.

use std::num::{NonZeroU8, NonZeroUsize};

use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{
    AuthenticationLimits, AuthenticationPolicy, AuthenticationRound, ExchangeOutcome, Moment,
    NegotiatedApi, NegotiatedCapabilities, SaslMechanism,
};

use super::{
    KafkaSessionAuthenticationState, KafkaSessionDeadline, KafkaSessionEffect, KafkaSessionInput,
    KafkaSessionLimits, KafkaSessionMachine, KafkaSessionState,
};

const HANDSHAKE_API: ApiKey = ApiKey::new(17);
const AUTHENTICATE_API: ApiKey = ApiKey::new(36);

#[test]
fn authenticated_session_owns_handshake_rounds_and_readiness() {
    let mut session = authenticated_session();
    open(&mut session);
    let authentication_deadline = Moment::from_nanos(200);

    let handshake = session.apply(KafkaSessionInput::ApiVersionsSucceededWithAuthentication {
        capabilities: capabilities(),
        deadline: KafkaSessionDeadline::new(Moment::from_nanos(100), authentication_deadline),
    });
    let first = session.apply(KafkaSessionInput::AuthenticationHandshakeSucceeded);
    let second = session.apply(KafkaSessionInput::AuthenticationExchangeCompleted {
        round: round(1),
        outcome: ExchangeOutcome::Continue,
    });
    let ready = session.apply(KafkaSessionInput::AuthenticationExchangeCompleted {
        round: round(2),
        outcome: ExchangeOutcome::Succeeded,
    });

    assert_eq!(
        handshake.effects(),
        [
            KafkaSessionEffect::CancelDeadline,
            KafkaSessionEffect::StartAuthenticationHandshake {
                mechanism: SaslMechanism::Plain,
                version: ApiVersion::new(1),
                deadline: authentication_deadline,
            },
        ]
    );
    assert!(matches!(
        first.effects(),
        [KafkaSessionEffect::StartAuthenticationExchange { round: observed, .. }]
            if *observed == round(1)
    ));
    assert!(matches!(
        second.effects(),
        [KafkaSessionEffect::StartAuthenticationExchange { round: observed, .. }]
            if *observed == round(2)
    ));
    assert_eq!(
        ready.effects(),
        [
            KafkaSessionEffect::CancelDeadline,
            KafkaSessionEffect::SessionReady,
        ]
    );
    assert_eq!(
        session.state(),
        KafkaSessionState::Ready { capabilities: 3 }
    );
}

#[test]
fn authentication_state_exposes_only_semantic_stage_data() {
    let mut session = authenticated_session();
    open(&mut session);
    let _ = session.apply(KafkaSessionInput::ApiVersionsSucceededWithAuthentication {
        capabilities: capabilities(),
        deadline: KafkaSessionDeadline::new(Moment::from_nanos(100), Moment::from_nanos(200)),
    });

    assert!(matches!(
        session.state(),
        KafkaSessionState::Authenticating {
            authentication: KafkaSessionAuthenticationState::Handshaking { .. },
            capabilities: 3,
        }
    ));
}

fn authenticated_session() -> KafkaSessionMachine {
    let policy = AuthenticationPolicy::new(
        SaslMechanism::Plain,
        HANDSHAKE_API,
        AUTHENTICATE_API,
        AuthenticationLimits::default(),
    );
    KafkaSessionMachine::new_authenticated(KafkaSessionLimits::default(), policy)
}

fn open(session: &mut KafkaSessionMachine) {
    let _ = session.apply(KafkaSessionInput::TransportOpened {
        deadline: KafkaSessionDeadline::new(Moment::ORIGIN, Moment::from_nanos(100)),
    });
}

fn capabilities() -> NegotiatedCapabilities {
    NegotiatedCapabilities::try_from_iter(
        [
            NegotiatedApi::new(HANDSHAKE_API, ApiVersion::new(1)),
            NegotiatedApi::new(ApiKey::new(18), ApiVersion::new(4)),
            NegotiatedApi::new(AUTHENTICATE_API, ApiVersion::new(2)),
        ],
        NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("test capabilities must be canonical: {error}"))
}

fn round(value: u8) -> AuthenticationRound {
    AuthenticationRound::new(NonZeroU8::new(value).unwrap_or(NonZeroU8::MIN))
}
