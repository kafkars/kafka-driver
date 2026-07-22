//! Bounded plaintext admission into rustls and encrypted socket progress.

use std::{io, io::Write, num::NonZeroUsize};

use kafka_driver_transport::WriteProgress;

use crate::reactor::transport::{CompletedWrite, WriteBudget, WriteDrive, WriteState};

use super::{TlsConnection, TlsError, io_limit::LimitedWriter};

impl TlsConnection {
    pub(in crate::reactor) fn drive_write(
        &mut self,
        budget: WriteBudget,
        destination: &mut Vec<CompletedWrite>,
    ) -> Result<WriteDrive, TlsError> {
        let mut tls_bytes = 0;
        let mut plaintext_bytes = 0;
        let mut completed = 0;
        loop {
            if self.writes.queued_frames() == 0 && !self.tls.wants_write() {
                return Ok(progress(
                    tls_bytes,
                    plaintext_bytes,
                    completed,
                    WriteState::Idle,
                ));
            }
            if self.tls.wants_write() {
                if tls_bytes == budget.bytes() {
                    return Ok(progress(
                        tls_bytes,
                        plaintext_bytes,
                        completed,
                        WriteState::BudgetExhausted,
                    ));
                }
                let mut writer = LimitedWriter::new(&mut self.socket, budget.bytes() - tls_bytes);
                match self.tls.write_tls(&mut writer) {
                    Ok(0) => return Err(TlsError::WriteZero),
                    Ok(written) => {
                        tls_bytes += written;
                        continue;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(progress(
                            tls_bytes,
                            plaintext_bytes,
                            completed,
                            WriteState::Blocked,
                        ));
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        return Ok(progress(
                            tls_bytes,
                            plaintext_bytes,
                            completed,
                            WriteState::Interrupted,
                        ));
                    }
                    Err(error) => return Err(TlsError::TlsWrite(error)),
                }
            }
            if plaintext_bytes == budget.bytes() {
                return Ok(progress(
                    tls_bytes,
                    plaintext_bytes,
                    completed,
                    WriteState::BudgetExhausted,
                ));
            }
            let Some(remaining) = NonZeroUsize::new(budget.bytes() - plaintext_bytes) else {
                continue;
            };
            let Some(front) = self.writes.front(remaining) else {
                continue;
            };
            match self.tls.writer().write(front.bytes()) {
                Ok(0) => return Err(TlsError::WriteZero),
                Ok(written) => {
                    plaintext_bytes += written;
                    if let WriteProgress::Complete {
                        call_id,
                        effect_id,
                        frame_bytes,
                    } = self.writes.advance(front.effect_id(), written)?
                    {
                        destination.push(CompletedWrite::new(call_id, effect_id, frame_bytes));
                        completed += 1;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if !self.tls.wants_write() {
                        return Err(TlsError::PlaintextWrite(error));
                    }
                }
                Err(error) => return Err(TlsError::PlaintextWrite(error)),
            }
        }
    }
}

fn progress(
    tls_bytes: usize,
    plaintext_bytes: usize,
    completed: usize,
    state: WriteState,
) -> WriteDrive {
    WriteDrive::new(tls_bytes.max(plaintext_bytes), completed, state)
}
