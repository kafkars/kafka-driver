//! Scenarios for public nonblocking completion extraction.

use crate::{CompletionError, completion::completion_pair};

use super::Call;

#[test]
fn nonblocking_extraction_distinguishes_pending_ready_and_consumed() {
    let (receiver, completion) = completion_pair();
    let call = Call::new(receiver);

    assert_eq!(call.try_result(), None);
    assert!(completion.complete(7).is_ok());
    assert_eq!(call.try_result(), Some(Ok(7)));
    assert_eq!(call.try_result(), Some(Err(CompletionError::Consumed)));
}
