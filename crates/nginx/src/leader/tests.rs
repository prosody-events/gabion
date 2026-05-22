use super::*;

#[test]
fn defaults_match_shared_production_defaults() {
    let gossip = GossipSettings::default();
    assert_eq!(
        gossip.tick_interval,
        Duration::from_millis(defaults::GOSSIP_TICK_INTERVAL_MILLIS)
    );
    assert_eq!(gossip.fanout, defaults::GOSSIP_FANOUT);
    assert_eq!(gossip.max_payload_bytes, defaults::GOSSIP_MAX_PAYLOAD_BYTES);
    assert_eq!(
        gossip.max_cells_per_frame,
        defaults::GOSSIP_MAX_CELLS_PER_FRAME
    );
    assert_eq!(
        gossip.max_cells_per_tick,
        defaults::GOSSIP_MAX_CELLS_PER_TICK
    );
    assert_eq!(
        gossip.send_queue_capacity,
        defaults::GOSSIP_SEND_QUEUE_CAPACITY
    );
    assert_eq!(
        gossip.limit_queue_capacity,
        defaults::GOSSIP_LIMIT_QUEUE_CAPACITY
    );
    assert_eq!(gossip.cluster_id_hash, defaults::GOSSIP_CLUSTER_ID_HASH);
    assert_eq!(gossip.target_err_bps, defaults::GOSSIP_TARGET_ERR_BPS);
    assert_eq!(
        gossip.min_emit_interval,
        Duration::from_millis(defaults::GOSSIP_MIN_EMIT_INTERVAL_MS)
    );

    let cell_store = production_cell_store_config();
    assert_eq!(cell_store.cell_capacity, defaults::STORAGE_MAX_CELLS as u32);
    assert_eq!(
        cell_store.rule_dictionary_capacity,
        defaults::STORAGE_RULE_DICTIONARY_CAPACITY
    );
    assert_eq!(
        cell_store.node_dictionary_capacity,
        defaults::STORAGE_NODE_DICTIONARY_CAPACITY
    );
    assert_eq!(
        cell_store.local_dirty_capacity,
        defaults::STORAGE_LOCAL_DIRTY_CAPACITY
    );
    assert_eq!(
        cell_store.forwarded_dirty_capacity,
        defaults::STORAGE_FORWARDED_DIRTY_CAPACITY
    );
    assert_eq!(cell_store.peer_capacity, defaults::STORAGE_PEER_CAPACITY);

    let leader = LeaderConfig::default();
    assert_eq!(
        leader.cell_store.cell_capacity,
        defaults::STORAGE_MAX_CELLS as u32
    );
}
