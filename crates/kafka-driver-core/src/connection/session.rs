//! Transport-independent Kafka session establishment and lifecycle policy.

mod authentication;
mod deadline;
mod effect;
mod input;
mod lifecycle;
mod limits;
mod machine;
mod negotiation;
mod state;
mod transition;

pub use effect::KafkaSessionEffect;
pub use input::{KafkaSessionDeadline, KafkaSessionInput};
pub use limits::KafkaSessionLimits;
pub use machine::KafkaSessionMachine;
pub use state::{
    KafkaSessionAuthenticationState, KafkaSessionCloseReason, KafkaSessionPhase,
    KafkaSessionProtocolFailure, KafkaSessionState,
};
pub use transition::{KafkaSessionDisposition, KafkaSessionTransition};

use state::{AuthenticationStage, StateData};
use transition::Decision;
