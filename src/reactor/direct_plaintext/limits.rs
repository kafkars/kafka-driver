//! Coherent Kafka framing, operation, I/O, and publication bounds for one direct slot.

use std::{io, num::NonZeroUsize};

use bornera::{
    ConnectionSetLimits, ConnectionSlotLimits, DecoderLimits, IoLimits, PublicationLimits,
    TransportLimits,
};
use bornera_core::{ConnectionLimits, MatchKeySpace};
use calandria::RetainedBytes;

use crate::config::DriverLimits;

use super::super::{bornera::KafkaFrameDecoder, broker::BrokerLimits};

// One fixed epoch can publish each of Bornera's four lifecycle edges once.
const LIFECYCLE_EVENTS: NonZeroUsize = nonzero(4);

pub(super) fn set_limits(limits: &DriverLimits) -> ConnectionSetLimits {
    ConnectionSetLimits::new(
        NonZeroUsize::MIN,
        limits.poll_event_budget(),
        limits.mailbox_capacity(),
        limits.command_budget(),
        NonZeroUsize::MIN,
    )
}

pub(super) fn slot_limits(
    driver: &DriverLimits,
    broker: BrokerLimits,
) -> io::Result<(KafkaFrameDecoder, ConnectionSlotLimits)> {
    let transport = broker.transport();
    let frame = transport.frame();
    let chunk = transport.read_chunk_bytes();
    let decoder = KafkaFrameDecoder::new(frame, chunk).map_err(message)?;
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
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(LIFECYCLE_EVENTS),
    )
    .map_err(message)?;
    Ok((decoder, slot))
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
        None => panic!("direct plaintext limits must be nonzero"),
    }
}
