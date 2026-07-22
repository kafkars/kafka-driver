//! Secret-loading and transport-security construction for qualification sessions.

mod sasl;

pub(crate) use sasl::session as sasl_session;
