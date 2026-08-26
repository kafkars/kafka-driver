//! Persistent set construction with the first replayable connection attempt.

use std::{io, net::SocketAddr};

use bornera::{
    ConnectionSet, ConnectionSetConfig, ConnectionToken, RegisteredTransport, TcpTransport,
};
use bornera_core::ConnectionEpoch;
use calandria::{ResourceOwnerId, RetainedBytes, Turn};
use kafka_driver_core::Moment;
use kafka_wire_core::DecodeLimits;

use crate::{
    config::{ClientId, DriverLimits, SaslConfig},
    reactor::{bornera::OperationContexts, broker::BrokerLimits},
};

#[cfg(feature = "tls-rustls")]
use super::{attempt::RustlsAttempt, rustls_transport::DirectRustlsTransport};
use super::{
    attempt::{DirectConnectionAttempt, PlaintextAttempt},
    limits::set_limits,
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, DirectSet, ID, message},
    pending::PendingRequests,
    session_plan::{DirectSessionOwnership, DirectSessionPlan},
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
        let session_plan = DirectSessionPlan::new(sasl, broker);
        let session = session_plan.start()?;
        let connection_attempt: Box<dyn DirectConnectionAttempt<TcpTransport>> =
            Box::new(PlaintextAttempt::new(driver, broker, address));
        let mut set = new_set(driver)?;
        let connection = connection_attempt.connect(&mut set, ConnectionEpoch::new(ID), now)?;
        finish(
            driver,
            broker,
            client_id,
            InitialDirect {
                session_plan,
                session,
                set,
                connection_attempt,
                connection,
            },
        )
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
        let session_plan = DirectSessionPlan::new(sasl, broker);
        let session = session_plan.start()?;
        let connection_attempt: Box<dyn DirectConnectionAttempt<DirectRustlsTransport>> =
            Box::new(RustlsAttempt::new(driver, broker, address, tls));
        let mut set = new_set(driver)?;
        let connection = connection_attempt.connect(&mut set, ConnectionEpoch::new(ID), now)?;
        finish(
            driver,
            broker,
            client_id,
            InitialDirect {
                session_plan,
                session,
                set,
                connection_attempt,
                connection,
            },
        )
    }
}

fn new_set<T: RegisteredTransport>(driver: &DriverLimits) -> io::Result<DirectSet<T>> {
    ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(ID)),
        set_limits(driver),
    )
    .map_err(message)
}

struct InitialDirect<T: RegisteredTransport> {
    session_plan: DirectSessionPlan,
    session: DirectSessionOwnership,
    set: DirectSet<T>,
    connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    connection: ConnectionToken,
}

fn finish<T: RegisteredTransport>(
    driver: &DriverLimits,
    broker: BrokerLimits,
    client_id: Option<ClientId>,
    initial: InitialDirect<T>,
) -> io::Result<DirectOwner<T>> {
    let InitialDirect {
        session_plan,
        session,
        set,
        connection_attempt,
        connection,
    } = initial;
    let retained =
        RetainedBytes::try_from(driver.mailbox_byte_capacity().get()).map_err(message)?;
    Ok(DirectOwner {
        set,
        connection_attempt,
        connection,
        session_plan,
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
