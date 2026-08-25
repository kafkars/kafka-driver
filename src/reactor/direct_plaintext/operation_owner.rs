//! Semantic owners published only after one Bornera operation is accepted.

use crate::{
    authentication::AuthenticationExchange, negotiation::NegotiationExchange,
    response::PublicResponseContext,
};

pub(super) enum DirectOperationContext {
    Negotiation(Option<NegotiationExchange>),
    Authentication(Option<AuthenticationExchange>),
    Public(PublicResponseContext),
}

impl DirectOperationContext {
    pub(super) const fn negotiation() -> Self {
        Self::Negotiation(None)
    }

    pub(super) const fn authentication() -> Self {
        Self::Authentication(None)
    }

    pub(super) fn bind_negotiation(&mut self, exchange: NegotiationExchange) -> bool {
        match self {
            Self::Negotiation(slot @ None) => {
                *slot = Some(exchange);
                true
            }
            Self::Negotiation(Some(_)) | Self::Authentication(_) | Self::Public(_) => false,
        }
    }

    pub(super) fn bind_authentication(&mut self, exchange: AuthenticationExchange) -> bool {
        match self {
            Self::Authentication(slot @ None) => {
                *slot = Some(exchange);
                true
            }
            Self::Authentication(Some(_)) | Self::Negotiation(_) | Self::Public(_) => false,
        }
    }
}
