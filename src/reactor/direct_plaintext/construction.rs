//! Transport-specific connection acquisition with shared semantic-owner initialization.

use std::{io, net::SocketAddr};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSet, ConnectionSetConfig, ConnectionToken,
    RegisteredTransport, TcpTransport, TransportLimits,
};
use bornera_core::{ConnectionEpoch, ConnectionId, EndpointId, LaneId};
use calandria::{Deadline, ResourceOwnerId, RetainedBytes, TimerOwnerId, Turn};
use kafka_driver_core::{AuthenticationPolicy, KafkaSessionLimits, KafkaSessionMachine, Moment};
use kafka_wire::{KafkaRequest, SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_wire_core::DecodeLimits;

#[cfg(feature = "tls-rustls")]
use bornera_rustls::RustlsConnector;

use crate::{
    authentication::AuthenticationSession,
    config::{ClientId, DriverLimits, SaslConfig},
    reactor::{
        bornera::{KafkaReplyClassifier, OperationContexts},
        broker::BrokerLimits,
    },
};

#[cfg(feature = "tls-rustls")]
use super::decoder_gate::DecoderGate;
#[cfg(feature = "tls-rustls")]
use super::limits::rustls_transport_limits;
#[cfg(feature = "tls-rustls")]
use super::rustls_transport::{DirectRustlsConnector, DirectRustlsTransport};
use super::{
    limits::{set_limits, slot_limits},
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, DirectSet, ID, calandria_moment, message},
    pending::PendingRequests,
};

impl DirectOwner<TcpTransport> {
    pub(in crate::reactor) fn new(
        driver: &DriverLimits,
        address: SocketAddr,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let session = session_ownership(sasl, broker)?;
        let (decoder, slot) = slot_limits(
            driver,
            broker,
            TransportLimits::new(RetainedBytes::ZERO),
            None,
        )?;
        let mut set = new_set(driver)?;
        let connection = set
            .connect(
                connection_config(address, now, broker)?,
                slot,
                decoder,
                KafkaReplyClassifier,
            )
            .map_err(message)?;
        finish(driver, broker, client_id, session, set, connection)
    }
}

#[cfg(feature = "tls-rustls")]
impl DirectOwner<DirectRustlsTransport> {
    pub(in crate::reactor) fn new(
        driver: &DriverLimits,
        address: SocketAddr,
        tls: crate::config::TlsClientConfig,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let session = session_ownership(sasl, broker)?;
        let transport = rustls_transport_limits()?;
        let decoder_gate = DecoderGate::new();
        let (decoder, slot) = slot_limits(
            driver,
            broker,
            transport.transport_limits(),
            Some(decoder_gate.clone()),
        )?;
        let connector = DirectRustlsConnector::new(
            RustlsConnector::new(tls.into_bornera(transport)),
            decoder_gate,
        );
        let mut set = new_set(driver)?;
        let connection = set
            .connect_with(
                connection_config(address, now, broker)?,
                slot,
                decoder,
                KafkaReplyClassifier,
                connector,
            )
            .map_err(message)?;
        finish(driver, broker, client_id, session, set, connection)
    }
}

fn new_set<T: RegisteredTransport>(driver: &DriverLimits) -> io::Result<DirectSet<T>> {
    ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(ID)),
        set_limits(driver),
    )
    .map_err(message)
}

fn connection_config(
    address: SocketAddr,
    now: Moment,
    broker: BrokerLimits,
) -> io::Result<ConnectionConfig> {
    let connect_deadline = now
        .checked_add(broker.connect_timeout())
        .ok_or_else(|| io::Error::other("direct connect deadline overflowed"))?;
    let lane =
        u32::try_from(ID).map_err(|_| io::Error::other("direct lane identity exceeds u32"))?;
    Ok(ConnectionConfig::new(
        ConnectionIdentity::new(
            EndpointId::new(ID),
            LaneId::new(lane),
            ConnectionId::new(ID),
            ConnectionEpoch::new(ID),
        ),
        address,
        Deadline::at(calandria_moment(connect_deadline)),
        TimerOwnerId::new(ID),
    ))
}

fn finish<T: RegisteredTransport>(
    driver: &DriverLimits,
    broker: BrokerLimits,
    client_id: Option<ClientId>,
    session: SessionOwnership,
    set: DirectSet<T>,
    connection: ConnectionToken,
) -> io::Result<DirectOwner<T>> {
    let retained =
        RetainedBytes::try_from(driver.mailbox_byte_capacity().get()).map_err(message)?;
    Ok(DirectOwner {
        set,
        connection,
        session: session.machine,
        authentication_session: session.authentication,
        scram_proof_sender: None,
        pending_scram_proof: None,
        session_deadline: None,
        contexts: OperationContexts::<DirectOperationContext>::new(
            broker.response_capacity(),
            retained,
        ),
        pending: PendingRequests::new(driver.mailbox_capacity(), driver.mailbox_byte_capacity()),
        client_id,
        outbound_limits: broker.outbound_frame(),
        decode_limits: DecodeLimits::default(),
        negotiation_limits: broker.negotiation(),
        negotiation_timeout: broker.negotiation_timeout(),
        authentication_timeout: broker.authentication_timeout(),
        response_capacity: broker.response_capacity().get(),
        write_frame_capacity: broker.transport().write().max_queued_frames(),
        write_byte_capacity: broker.transport().write().max_buffered_bytes(),
        write_frame_rejections: 0,
        write_byte_rejections: 0,
        last_close_reason: None,
        retired_seed: None,
        submission_budget: driver.command_budget(),
        last_turn: Turn::waiting(),
        admission_open: false,
        terminal: false,
        pending_recovery: None,
    })
}

struct SessionOwnership {
    machine: KafkaSessionMachine,
    authentication: Option<AuthenticationSession>,
}

fn session_ownership(
    sasl: Option<SaslConfig>,
    broker: BrokerLimits,
) -> io::Result<SessionOwnership> {
    let Some(sasl) = sasl else {
        return Ok(SessionOwnership {
            machine: KafkaSessionMachine::new(KafkaSessionLimits::default()),
            authentication: None,
        });
    };
    let policy = AuthenticationPolicy::new(
        sasl.mechanism(),
        SaslHandshakeRequest::API_KEY,
        SaslAuthenticateRequest::API_KEY,
        broker.authentication(),
    );
    let authentication = AuthenticationSession::new(sasl).map_err(|error| {
        io::Error::other(format!(
            "direct authentication session could not start: {error:?}"
        ))
    })?;
    Ok(SessionOwnership {
        machine: KafkaSessionMachine::new_authenticated(KafkaSessionLimits::default(), policy),
        authentication: Some(authentication),
    })
}
