//! Ordering proof for publications drained immediately before recovery capture.

use super::drive::prepend;

#[test]
fn recovery_keeps_predrained_publications_before_capture_publications() {
    let mut recovered = vec![3, 4];

    prepend(&mut recovered, [1, 2].into_iter());

    assert_eq!(recovered, vec![1, 2, 3, 4]);
}
