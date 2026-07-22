//! Pure intersection of broker advertisements and generated protocol metadata.

mod error;
mod exchange;
mod exchange_error;
mod intersection;
mod limits;

#[cfg(test)]
mod exchange_test;
#[cfg(test)]
mod intersection_test;

pub(crate) use error::NegotiationError;
pub(crate) use exchange::NegotiationExchange;
pub(crate) use exchange_error::NegotiationExchangeError;
pub(crate) use intersection::negotiate;
pub(crate) use limits::NegotiationLimits;
