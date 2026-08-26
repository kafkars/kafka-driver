//! Persistent set construction with the first replayable connection attempt.

use std::{io, net::SocketAddr};

use bornera::{ConnectionToken, RegisteredTransport, TcpTransport};
use bornera_core::{ConnectionId, EndpointId, LaneId};
use calandria::{RetainedBytes, TimerOwnerId, Turn};
use kafka_driver_core::{
    BrokerEffect, BrokerState, CloseReason, ConnectionEpoch, KafkaSessionInput, Moment,
};
use kafka_wire_core::DecodeLimits;

use crate::{
    config::{ClientId, DriverLimits, SaslConfig},
    reactor::{bornera::OperationContexts, broker::BrokerLimits},
};

#[cfg(feature = "tls-rustls")]
use super::{attempt::RustlsAttempt, rustls_transport::DirectRustlsTransport};
use super::{
    attempt::{
        DirectConnectError, DirectConnectionAttempt, DirectConnectionOwner, PlaintextAttempt,
    },
    failure_translation::synchronous_open_failure,
    lifecycle::DirectLifecycle,
    operation_owner::DirectOperationContext,
    owner::{DirectLane, DirectSet, ID, message},
    pending::PendingRequests,
    runtime::{DirectRuntime, new_set},
    session_plan::{DirectSessionOwnership, DirectSessionPlan},
};

impl DirectRuntime<TcpTransport> {
    pub(in crate::reactor) fn new(
        driver: &DriverLimits,
        address: SocketAddr,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        let session_plan = DirectSessionPlan::new(sasl, broker);
        let connection_attempt: Box<dyn DirectConnectionAttempt<TcpTransport>> =
            Box::new(PlaintextAttempt::new(driver, broker, address));
        start(
            driver,
            broker,
            address,
            client_id,
            session_plan,
            connection_attempt,
            now,
        )
    }

    #[cfg(test)]
    pub(super) fn new_with_attempt(
        driver: &DriverLimits,
        address: SocketAddr,
        sasl: Option<SaslConfig>,
        attempt: Box<dyn DirectConnectionAttempt<TcpTransport>>,
        now: Moment,
    ) -> io::Result<Self> {
        let broker = BrokerLimits::default();
        start(
            driver,
            broker,
            address,
            None,
            DirectSessionPlan::new(sasl, broker),
            attempt,
            now,
        )
    }
}

#[cfg(feature = "tls-rustls")]
impl DirectRuntime<DirectRustlsTransport> {
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
        let connection_attempt: Box<dyn DirectConnectionAttempt<DirectRustlsTransport>> =
            Box::new(RustlsAttempt::new(driver, broker, address, tls));
        start(
            driver,
            broker,
            address,
            client_id,
            session_plan,
            connection_attempt,
            now,
        )
    }
}

fn start<T: RegisteredTransport>(
    driver: &DriverLimits,
    broker: BrokerLimits,
    address: SocketAddr,
    client_id: Option<ClientId>,
    session_plan: DirectSessionPlan,
    connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    now: Moment,
) -> io::Result<DirectRuntime<T>> {
    let mut session = session_plan.start()?;
    let mut lifecycle = DirectLifecycle::started(broker.backoff(), address)?;
    let mut set = new_set(driver)?;
    let connection_owner = DirectConnectionOwner::new(
        EndpointId::new(ID),
        LaneId::new(1),
        ConnectionId::new(ID),
        TimerOwnerId::new(ID),
    );
    let attempt = connection_attempt.connect(
        &mut set,
        connection_owner,
        bornera_core::ConnectionEpoch::new(ID),
        now,
    );
    let (connection, last_close_reason) = match attempt {
        Ok(connection) => (Some(connection), None),
        Err(DirectConnectError::Endpoint(source)) => {
            let reason = synchronous_open_failure(&source);
            drop(session.machine.apply(KafkaSessionInput::Closed));
            session.authentication = None;
            let epoch = ConnectionEpoch::from_raw(ID);
            let effects = lifecycle.generation_ended(epoch, reason, now)?;
            let valid = matches!(
                (lifecycle.state(), effects.as_slice()),
                (
                    BrokerState::Backoff { .. },
                    [BrokerEffect::ScheduleReconnect { .. }]
                ) | (BrokerState::Closed { .. }, [])
            );
            if !valid {
                return Err(io::Error::other(
                    "initial direct endpoint failure produced invalid lifecycle policy",
                ));
            }
            (None, Some(reason))
        }
        Err(DirectConnectError::Fatal(source)) => return Err(source),
    };
    finish(
        driver,
        broker,
        client_id,
        InitialDirect {
            session_plan,
            session,
            set,
            connection_attempt,
            connection_owner,
            connection,
            lifecycle,
            last_close_reason,
        },
    )
}

struct InitialDirect<T: RegisteredTransport> {
    session_plan: DirectSessionPlan,
    session: DirectSessionOwnership,
    set: DirectSet<T>,
    connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    connection_owner: DirectConnectionOwner,
    connection: Option<ConnectionToken>,
    lifecycle: DirectLifecycle,
    last_close_reason: Option<CloseReason>,
}

fn finish<T: RegisteredTransport>(
    driver: &DriverLimits,
    broker: BrokerLimits,
    client_id: Option<ClientId>,
    initial: InitialDirect<T>,
) -> io::Result<DirectRuntime<T>> {
    let InitialDirect {
        session_plan,
        session,
        set,
        connection_attempt,
        connection_owner,
        connection,
        lifecycle,
        last_close_reason,
    } = initial;
    let retained =
        RetainedBytes::try_from(driver.mailbox_byte_capacity().get()).map_err(message)?;
    let terminal = lifecycle.is_closed();
    let lane = DirectLane {
        connection_attempt,
        connection_owner,
        connection,
        lifecycle,
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
        generation_close_reason: None,
        last_close_reason,
        submission_budget: driver.command_budget(),
        runnable: false,
        admission_open: false,
        terminal,
        pending_recovery: None,
    };
    Ok(DirectRuntime {
        set,
        lane,
        last_turn: Turn::waiting(),
    })
}
