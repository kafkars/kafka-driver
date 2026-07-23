//! Single-owner ordering for external evidence and observable broker outcomes.

use kafka_driver_core::{EvidenceStamp, OutcomeStamp};

#[derive(Debug)]
pub(in crate::reactor) struct CausalSequence {
    pub(super) next: u64,
}

impl CausalSequence {
    pub(in crate::reactor) const fn new() -> Self {
        Self { next: 1 }
    }

    pub(in crate::reactor) fn evidence(&mut self) -> Result<EvidenceStamp, CausalSequenceError> {
        self.reserve().map(EvidenceStamp::from_raw)
    }

    pub(in crate::reactor) fn outcome(&mut self) -> Result<OutcomeStamp, CausalSequenceError> {
        self.reserve().map(OutcomeStamp::from_raw)
    }

    fn reserve(&mut self) -> Result<u64, CausalSequenceError> {
        let current = self.next;
        self.next = current
            .checked_add(1)
            .ok_or(CausalSequenceError::Exhausted)?;
        Ok(current)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum CausalSequenceError {
    Exhausted,
}

impl std::fmt::Display for CausalSequenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the reactor causal sequence is exhausted")
    }
}

impl std::error::Error for CausalSequenceError {}
