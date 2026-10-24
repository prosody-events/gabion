use super::*;

#[test]
fn claim_counts_datagrams_skipped_for_want_of_a_buffer() {
    let mut queue = SendQueue::new(1, FrameLimits::default());
    let slot = queue.claim("first").expect("one buffer is free");
    queue.send_to(slot, SocketAddr::from(([127, 0, 0, 1], 1)));
    queue.settle(slot);
    assert_eq!(queue.exhausted(), 0);

    assert_eq!(queue.claim("second"), None, "the only buffer is queued");
    assert_eq!(queue.claim("third"), None);
    assert_eq!(queue.exhausted(), 2);
}
