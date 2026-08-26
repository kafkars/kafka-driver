//! Coherent Kafka framing, operation, I/O, and publication bounds for one direct slot.

use std::{io, num::NonZeroUsize};

#[cfg(feature = "tls-rustls")]
use bornera::TransportPressure;
use bornera::{
    ConnectionSetLimits, ConnectionSlotLimits, DecoderLimits, IoLimits, PublicationLimits,
    TransportLimits,
};
use bornera_core::{ConnectionLimits, MatchKeySpace};
#[cfg(feature = "tls-rustls")]
use bornera_rustls::RustlsTransportLimits;
use calandria::RetainedBytes;

use crate::config::DriverLimits;

use super::{
    super::{bornera::KafkaFrameDecoder, broker::BrokerLimits},
    decoder_gate::{DecoderGate, DirectFrameDecoder},
};

// One fixed epoch can publish each of Bornera's four lifecycle edges once.
const LIFECYCLE_EVENTS: NonZeroUsize = nonzero(4);

#[derive(Clone, Copy)]
pub(super) struct DirectSetBounds {
    max_connections: NonZeroUsize,
    ready_connections_per_turn: NonZeroUsize,
}

impl DirectSetBounds {
    pub(super) const fn new(
        max_connections: NonZeroUsize,
        ready_connections_per_turn: NonZeroUsize,
    ) -> Self {
        Self {
            max_connections,
            ready_connections_per_turn,
        }
    }

    pub(super) const fn direct() -> Self {
        Self::new(NonZeroUsize::MIN, NonZeroUsize::MIN)
    }

    pub(super) const fn max_connections(self) -> NonZeroUsize {
        self.max_connections
    }

    #[cfg(test)]
    pub(super) const fn ready_connections_per_turn(self) -> NonZeroUsize {
        self.ready_connections_per_turn
    }
}

pub(super) fn set_limits(limits: &DriverLimits, bounds: DirectSetBounds) -> ConnectionSetLimits {
    ConnectionSetLimits::new(
        bounds.max_connections,
        limits.poll_event_budget(),
        limits.mailbox_capacity(),
        limits.command_budget(),
        bounds.ready_connections_per_turn,
    )
}

pub(super) fn slot_limits(
    driver: &DriverLimits,
    broker: BrokerLimits,
    transport_limits: TransportLimits,
    decoder_gate: Option<DecoderGate>,
) -> io::Result<(DirectFrameDecoder, ConnectionSlotLimits)> {
    let transport = broker.transport();
    let frame = transport.frame();
    let chunk = transport.read_chunk_bytes();
    let decoder = DirectFrameDecoder::new(
        KafkaFrameDecoder::new(frame, chunk).map_err(message)?,
        decoder_gate,
    );
    let response_capacity = broker.response_capacity();
    let semantic_bytes = retained(driver.mailbox_byte_capacity().get())?;
    let write = transport.write();
    let write_bytes = retained(write.max_buffered_bytes())?;
    let match_keys = MatchKeySpace::new(0, i32::MAX as u32).map_err(message)?;
    let core = ConnectionLimits::new(
        response_capacity.get(),
        semantic_bytes,
        write.max_queued_frames(),
        write_bytes,
        match_keys,
    )
    .map_err(message)?;
    let decoder_limits = DecoderLimits::new(
        decoder.bornera_retained_limit(),
        retained(frame.max_frame_bytes())?,
    );
    let io_operations = broker
        .read_budget()
        .bytes()
        .checked_div(chunk.get())
        .unwrap_or(0)
        .min(
            broker
                .write_budget()
                .bytes()
                .checked_div(chunk.get())
                .unwrap_or(0),
        );
    let io_operations = NonZeroUsize::new(io_operations)
        .ok_or_else(|| io::Error::other("direct I/O budget must be nonzero"))?;
    let slot = ConnectionSlotLimits::new(
        core,
        decoder_limits,
        IoLimits::new(io_operations, chunk),
        transport_limits,
        PublicationLimits::new(LIFECYCLE_EVENTS),
    )
    .map_err(message)?;
    Ok((decoder, slot))
}

#[cfg(feature = "tls-rustls")]
pub(super) fn rustls_transport_limits() -> io::Result<RustlsTransportLimits> {
    let kib = |value: u64| RetainedBytes::new(value * 1_024);
    let pressure =
        TransportPressure::new(kib(128), kib(128), kib(192), kib(128)).map_err(message)?;
    let limits = RustlsTransportLimits::new(
        nonzero(64 * 1_024),
        nonzero(128 * 1_024),
        nonzero(128 * 1_024),
        pressure,
    )
    .map_err(message)?;
    if limits.transport_limits().retained_bytes() != kib(576) {
        return Err(io::Error::other(
            "direct rustls profile must charge exactly 576 KiB",
        ));
    }
    Ok(limits)
}

fn retained(bytes: usize) -> io::Result<RetainedBytes> {
    RetainedBytes::try_from(bytes).map_err(|error| io::Error::other(error.to_string()))
}

fn message(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

const fn nonzero(value: usize) -> NonZeroUsize {
    match NonZeroUsize::new(value) {
        Some(value) => value,
        None => panic!("direct broker limits must be nonzero"),
    }
}
