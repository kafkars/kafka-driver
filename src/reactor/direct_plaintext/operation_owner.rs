//! Semantic owners published only after one Bornera operation is accepted.

use crate::{negotiation::NegotiationExchange, response::PublicResponseContext};

pub(super) enum DirectOperationContext {
    Negotiation(Option<NegotiationExchange>),
    Public(PublicResponseContext),
}

impl DirectOperationContext {
    pub(super) const fn negotiation() -> Self {
        Self::Negotiation(None)
    }

    pub(super) fn bind_negotiation(&mut self, exchange: NegotiationExchange) -> bool {
        match self {
            Self::Negotiation(slot @ None) => {
                *slot = Some(exchange);
                true
            }
            Self::Negotiation(Some(_)) | Self::Public(_) => false,
        }
    }
}
