//! Bounded encrypted reads, TLS processing, plaintext framing, and close observation.

use std::io::{self, Read};

use kafka_driver_transport::FrameBody;

use crate::reactor::transport::{ReadBudget, ReadProgress, ReadState};

use super::{TlsConnection, TlsError};

impl TlsConnection {
    pub(in crate::reactor) fn drive_read(
        &mut self,
        budget: ReadBudget,
        destination: &mut Vec<FrameBody>,
    ) -> Result<ReadProgress, TlsError> {
        let mut tls_bytes = 0;
        let mut plaintext_bytes = 0;
        let mut frames = 0;
        let mut peer_closed = false;
        loop {
            while frames < budget.frames() {
                let Some(frame) = self.frames.next_frame()? else {
                    break;
                };
                destination.push(frame);
                frames += 1;
            }
            if frames == budget.frames() || plaintext_bytes == budget.bytes() {
                return Ok(progress(
                    tls_bytes,
                    plaintext_bytes,
                    frames,
                    ReadState::BudgetExhausted,
                    self.tls.wants_write(),
                ));
            }

            let read = self.read_plaintext(budget.bytes() - plaintext_bytes)?;
            if read != 0 {
                plaintext_bytes += read;
                continue;
            }
            if peer_closed {
                return Ok(progress(
                    tls_bytes,
                    plaintext_bytes,
                    frames,
                    ReadState::PeerClosed,
                    self.tls.wants_write(),
                ));
            }
            if tls_bytes == budget.bytes() {
                return Ok(progress(
                    tls_bytes,
                    plaintext_bytes,
                    frames,
                    ReadState::BudgetExhausted,
                    self.tls.wants_write(),
                ));
            }

            let mut limited = (&mut self.socket).take((budget.bytes() - tls_bytes) as u64);
            match self.tls.read_tls(&mut limited) {
                Ok(0) => {
                    // Authenticated records and complete Kafka frames were drained above.
                    // Report bare TCP EOF afterward so policy can complete them before close.
                    return Ok(progress(
                        tls_bytes,
                        plaintext_bytes,
                        frames,
                        ReadState::PeerClosed,
                        self.tls.wants_write(),
                    ));
                }
                Ok(read) => {
                    tls_bytes += read;
                    let state = self.tls.process_new_packets().map_err(TlsError::Protocol)?;
                    peer_closed |= state.peer_has_closed();
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(progress(
                        tls_bytes,
                        plaintext_bytes,
                        frames,
                        ReadState::Blocked,
                        self.tls.wants_write(),
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    return Ok(progress(
                        tls_bytes,
                        plaintext_bytes,
                        frames,
                        ReadState::Interrupted,
                        self.tls.wants_write(),
                    ));
                }
                Err(error) => return Err(TlsError::TlsRead(error)),
            }
        }
    }

    fn read_plaintext(&mut self, remaining_budget: usize) -> Result<usize, TlsError> {
        let decoder_space = self
            .max_buffered_read_bytes
            .saturating_sub(self.frames.buffered_bytes());
        let readable = self
            .read_buffer
            .len()
            .min(remaining_budget)
            .min(decoder_space);
        if readable == 0 {
            return Ok(0);
        }
        match self.tls.reader().read(&mut self.read_buffer[..readable]) {
            Ok(0) => Ok(0),
            Ok(read) => {
                self.frames.feed(&self.read_buffer[..read])?;
                Ok(read)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(error) => Err(TlsError::PlaintextRead(error)),
        }
    }
}

fn progress(
    tls_bytes: usize,
    plaintext_bytes: usize,
    frames: usize,
    state: ReadState,
    write_pending: bool,
) -> ReadProgress {
    ReadProgress::new(tls_bytes.max(plaintext_bytes), frames, state, write_pending)
}
