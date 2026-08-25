//! Shared fixed-state visibility between Kafka decoding and TLS terminal progression.

use std::{cell::Cell, mem::size_of, rc::Rc};

use bornera_core::FrameDecoder;
use calandria::RetainedBytes;

use crate::{
    reactor::bornera::{KafkaFrame, KafkaFrameDecodeError, KafkaFrameDecoder},
    request::ALLOCATION_ALLOWANCE_BYTES,
};

// Two Rc counters plus the boolean rounded to pointer alignment, followed by
// the driver's established conservative allocator allowance.
const GATE_ALLOCATION_BYTES: u64 = (3 * size_of::<usize>() + ALLOCATION_ALLOWANCE_BYTES) as u64;
const GATE_RETAINED_BYTES: RetainedBytes = RetainedBytes::new(GATE_ALLOCATION_BYTES);

#[derive(Clone, Debug)]
pub(super) struct DecoderGate(Rc<Cell<bool>>);

impl DecoderGate {
    #[cfg(feature = "tls-rustls")]
    pub(super) fn new() -> Self {
        Self(Rc::new(Cell::new(false)))
    }

    #[cfg(feature = "tls-rustls")]
    pub(super) fn has_pending_decode(&self) -> bool {
        self.0.get()
    }

    fn set(&self, pending: bool) {
        self.0.set(pending);
    }
}

#[derive(Debug)]
pub(super) struct DirectFrameDecoder {
    inner: KafkaFrameDecoder,
    gate: Option<DecoderGate>,
}

impl DirectFrameDecoder {
    pub(super) const fn new(inner: KafkaFrameDecoder, gate: Option<DecoderGate>) -> Self {
        Self { inner, gate }
    }

    pub(super) fn bornera_retained_limit(&self) -> RetainedBytes {
        self.inner
            .bornera_retained_limit()
            .checked_add(self.gate_retained_bytes())
            .unwrap_or(RetainedBytes::new(u64::MAX))
    }

    fn set_gate(&self, pending: bool) {
        if let Some(gate) = &self.gate {
            gate.set(pending);
        }
    }

    fn gate_retained_bytes(&self) -> RetainedBytes {
        self.gate
            .as_ref()
            .map_or(RetainedBytes::ZERO, |_| GATE_RETAINED_BYTES)
    }
}

impl FrameDecoder for DirectFrameDecoder {
    type Frame = KafkaFrame;
    type Error = KafkaFrameDecodeError;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.set_gate(true);
        let result = self.inner.feed(bytes);
        if result.is_err() {
            self.set_gate(false);
        }
        result
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        let result = self.inner.next_frame();
        if !matches!(result, Ok(Some(_))) {
            self.set_gate(false);
        }
        result
    }

    fn retained_bytes(&self) -> RetainedBytes {
        self.inner
            .retained_bytes()
            .checked_add(self.gate_retained_bytes())
            .unwrap_or(RetainedBytes::new(u64::MAX))
    }
}
