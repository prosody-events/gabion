use super::*;

fn handle(index: u32, generation: u32) -> CellHandle {
    CellHandle { index, generation }
}

fn outbox(sync: usize, broadcast: usize) -> Outbox<u32> {
    Outbox::new(8, sync, broadcast)
}

#[test]
fn a_cell_queues_once() {
    let mut o = outbox(4, 4);
    o.queue_sync(handle(1, 1)).unwrap();
    o.queue_sync(handle(1, 1)).unwrap();
    o.queue_broadcast(handle(1, 1)).unwrap();
    o.queue_broadcast(handle(1, 1)).unwrap();
    assert_eq!(o.sync_len(), 1);
    assert_eq!(o.broadcast_len(), 1);
}

#[test]
fn a_full_queue_refuses_new_cells_only() {
    let mut o = outbox(1, 1);
    o.queue_sync(handle(1, 1)).unwrap();
    assert_eq!(o.queue_sync(handle(2, 1)), Err(QueueFull));
    o.queue_sync(handle(1, 1)).unwrap();
    o.queue_broadcast(handle(1, 1)).unwrap();
    assert_eq!(o.queue_broadcast(handle(2, 1)), Err(QueueFull));
}

#[test]
fn unqueueing_lets_the_cell_queue_again() {
    let mut o = outbox(4, 4);
    o.queue_sync(handle(1, 1)).unwrap();
    o.unqueue_sync(0);
    assert_eq!(o.sync_len(), 0);
    o.queue_sync(handle(1, 1)).unwrap();
    assert_eq!(o.sync_len(), 1);
}

#[test]
fn broadcast_pops_in_arrival_order() {
    let mut o = outbox(4, 4);
    for i in 1..4 {
        o.queue_broadcast(handle(i, 1)).unwrap();
    }
    assert_eq!(o.pop_broadcast(), Some(handle(1, 1)));
    assert_eq!(o.pop_broadcast(), Some(handle(2, 1)));
    o.queue_broadcast(handle(1, 1)).unwrap();
    assert_eq!(o.pop_broadcast(), Some(handle(3, 1)));
    assert_eq!(o.pop_broadcast(), Some(handle(1, 1)));
    assert_eq!(o.pop_broadcast(), None);
}

#[test]
fn a_dead_cells_entry_does_not_block_its_slots_next_cell() {
    let mut o = outbox(4, 4);
    let dead = handle(3, 1);
    let reborn = handle(3, 3);
    o.queue_sync(dead).unwrap();
    o.forget(dead);
    o.queue_sync(reborn).unwrap();
    assert_eq!(o.sync_len(), 2);
    // Visiting the dead cell's entry must not unqueue the reborn cell.
    o.unqueue_sync(0);
    o.queue_sync(reborn).unwrap();
    assert_eq!(o.sync_len(), 1);
}

#[test]
fn delivery_is_per_owner_and_monotone() {
    let mut o = outbox(4, 4);
    let cell = handle(2, 1);
    let (a, b) = (
        Token::of(([10, 0, 0, 1], 1).into()),
        Token::of(([10, 0, 0, 2], 1).into()),
    );
    assert!(!o.is_delivered(cell, 1, a));
    o.delivered(cell, 5, a);
    assert!(o.is_delivered(cell, 5, a));
    assert!(o.is_delivered(cell, 3, a));
    assert!(!o.is_delivered(cell, 6, a));
    assert!(!o.is_delivered(cell, 1, b));
    o.delivered(cell, 2, a);
    assert!(
        o.is_delivered(cell, 5, a),
        "a late ack must not lower the count"
    );
    o.delivered(cell, 1, b);
    assert!(
        !o.is_delivered(cell, 1, a),
        "a new owner starts from what it acknowledged"
    );
    o.forget(cell);
    assert!(!o.is_delivered(cell, 1, b));
}
