//! Single-owner endpoint candidate, failure-pass, and refresh transitions.

use crate::{BrokerEndpoint, ResolvedAddressSet};

use super::{EndpointDialerEffect, EndpointDialerInput, EndpointDialerTransition};

/// Deterministic owner of one logical endpoint and its current DNS candidates.
#[must_use]
#[derive(Debug)]
pub struct EndpointDialer {
    endpoint: BrokerEndpoint,
    addresses: ResolvedAddressSet,
    next: usize,
    failures: usize,
    phase: DialPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DialPhase {
    Idle,
    Candidate(usize),
    Connected(usize),
}

impl EndpointDialer {
    /// Creates idle policy over one nonempty bounded resolver result.
    pub const fn new(endpoint: BrokerEndpoint, addresses: ResolvedAddressSet) -> Self {
        Self {
            endpoint,
            addresses,
            next: 0,
            failures: 0,
            phase: DialPhase::Idle,
        }
    }

    /// Returns the first retained candidate without advancing selection.
    pub fn primary(&self) -> Option<crate::ResolvedAddress> {
        self.addresses.get(0)
    }

    /// Applies one connection or resolver outcome.
    #[must_use = "endpoint-dialer effects must be interpreted"]
    pub fn apply(&mut self, input: EndpointDialerInput) -> EndpointDialerTransition {
        match input {
            EndpointDialerInput::OpenCandidate => self.open_candidate(),
            EndpointDialerInput::ConnectionReady => self.connection_ready(),
            EndpointDialerInput::ConnectionFailed => self.connection_failed(),
            EndpointDialerInput::ResolutionCompleted { addresses } => {
                self.replace_addresses(addresses)
            }
        }
    }

    fn open_candidate(&mut self) -> EndpointDialerTransition {
        if self.phase != DialPhase::Idle {
            return EndpointDialerTransition::ignored();
        }
        let selected = self.next;
        let Some(address) = self.addresses.get(selected) else {
            return EndpointDialerTransition::ignored();
        };
        self.next = (selected + 1) % self.addresses.len();
        self.phase = DialPhase::Candidate(selected);
        EndpointDialerTransition::applied(vec![EndpointDialerEffect::OpenCandidate {
            endpoint: self.endpoint.clone(),
            address,
        }])
    }

    fn connection_ready(&mut self) -> EndpointDialerTransition {
        let DialPhase::Candidate(selected) = self.phase else {
            return EndpointDialerTransition::ignored();
        };
        self.failures = 0;
        self.phase = DialPhase::Connected(selected);
        EndpointDialerTransition::applied(Vec::new())
    }

    fn connection_failed(&mut self) -> EndpointDialerTransition {
        match self.phase {
            DialPhase::Candidate(_) => self.candidate_failed(),
            DialPhase::Connected(selected) => {
                self.next = selected;
                self.failures = 0;
                self.phase = DialPhase::Idle;
                EndpointDialerTransition::applied(Vec::new())
            }
            DialPhase::Idle => EndpointDialerTransition::ignored(),
        }
    }

    fn candidate_failed(&mut self) -> EndpointDialerTransition {
        self.failures += 1;
        self.phase = DialPhase::Idle;
        if self.failures < self.addresses.len() {
            return EndpointDialerTransition::applied(Vec::new());
        }
        self.failures = 0;
        EndpointDialerTransition::applied(vec![EndpointDialerEffect::Resolve {
            endpoint: self.endpoint.clone(),
        }])
    }

    fn replace_addresses(&mut self, addresses: ResolvedAddressSet) -> EndpointDialerTransition {
        self.addresses = addresses;
        self.next = 0;
        self.failures = 0;
        self.phase = DialPhase::Idle;
        EndpointDialerTransition::applied(Vec::new())
    }
}
