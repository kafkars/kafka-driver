//! Cumulative public-call outcomes and classified failures.

/// Public-call admission, completion, receiver, and delivery totals.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CallCounters {
    admitted: u64,
    succeeded: u64,
    failed: u64,
    receiver_abandoned: u64,
    not_sent: u64,
    possibly_sent: u64,
}

impl CallCounters {
    pub(crate) const fn new(values: [u64; 6]) -> Self {
        Self {
            admitted: values[0],
            succeeded: values[1],
            failed: values[2],
            receiver_abandoned: values[3],
            not_sent: values[4],
            possibly_sent: values[5],
        }
    }

    /// Returns public calls accepted for reactor interpretation.
    pub const fn admitted(self) -> u64 {
        self.admitted
    }
    /// Returns calls completed with a generated response.
    pub const fn succeeded(self) -> u64 {
        self.succeeded
    }
    /// Returns calls completed with a typed request failure.
    pub const fn failed(self) -> u64 {
        self.failed
    }
    /// Returns terminal values discarded after caller abandonment.
    pub const fn receiver_abandoned(self) -> u64 {
        self.receiver_abandoned
    }
    /// Returns failures explicitly classified as definitely not sent.
    pub const fn not_sent(self) -> u64 {
        self.not_sent
    }
    /// Returns failures explicitly classified as possibly sent.
    pub const fn possibly_sent(self) -> u64 {
        self.possibly_sent
    }
}

/// Cumulative terminal failure categories for observed public calls.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FailureCounters {
    dns: u64,
    connect: u64,
    transport: u64,
    negotiation: u64,
    authentication: u64,
    deadline: u64,
    local_rejection: u64,
    response_capacity: u64,
    route_capacity: u64,
}

impl FailureCounters {
    pub(crate) const fn new(values: [u64; 9]) -> Self {
        Self {
            dns: values[0],
            connect: values[1],
            transport: values[2],
            negotiation: values[3],
            authentication: values[4],
            deadline: values[5],
            local_rejection: values[6],
            response_capacity: values[7],
            route_capacity: values[8],
        }
    }

    /// Returns broker-name resolution failures.
    pub const fn dns(self) -> u64 {
        self.dns
    }
    /// Returns transport-establishment failures.
    pub const fn connect(self) -> u64 {
        self.connect
    }
    /// Returns established-transport losses.
    pub const fn transport(self) -> u64 {
        self.transport
    }
    /// Returns initial API negotiation failures.
    pub const fn negotiation(self) -> u64 {
        self.negotiation
    }
    /// Returns terminal SASL authentication failures.
    pub const fn authentication(self) -> u64 {
        self.authentication
    }
    /// Returns end-to-end deadline failures.
    pub const fn deadline(self) -> u64 {
        self.deadline
    }
    /// Returns definitely-unsent local preparation or writer rejections.
    pub const fn local_rejection(self) -> u64 {
        self.local_rejection
    }
    /// Returns typed response-registry capacity rejections.
    pub const fn response_capacity(self) -> u64 {
        self.response_capacity
    }
    /// Returns semantic route, query, or coordinator capacity rejections.
    pub const fn route_capacity(self) -> u64 {
        self.route_capacity
    }
}
