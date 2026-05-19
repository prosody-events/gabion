use std::hint::black_box;
use std::time::{Duration, Instant};

use gabion_gossip::{
    CounterCell, GossipHeader, GossipLimits, GossipMessage, HmacKey, NodeId,
    decode_authenticated_message, encode_authenticated_message,
};

fn main() {
    let key = HmacKey::new([9_u8; 32]);
    let mut cells = Vec::with_capacity(64);
    for index in 0..64 {
        cells.push(CounterCell {
            rule_id: 1,
            key_hash_hi: index,
            key_hash_lo: index + 1,
            bucket_start_millis: 1_000,
            origin_node_id: NodeId { hi: 1, lo: 2 },
            origin_incarnation: 1,
            count: index + 1,
            last_update_millis: 2_000,
            sequence: 0,
        });
    }
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: NodeId { hi: 1, lo: 2 },
            sender_incarnation: 1,
            min_bucket: 0,
            max_bucket: 1_000,
            flags: 0,
        },
        digests: Vec::new(),
        cells,
        truncated: false,
    };
    let limits = GossipLimits::default();
    let mut buffer = Vec::with_capacity(8192);
    let deadline = Instant::now() + Duration::from_millis(200);
    let mut iterations = 0_u64;

    while Instant::now() < deadline {
        encode_authenticated_message(black_box(&message), key, &mut buffer, limits);
        let decoded =
            decode_authenticated_message(black_box(&buffer), key, limits).expect("decode");
        black_box(decoded);
        iterations = iterations.saturating_add(1);
    }

    println!("gossip_codec_iterations_200ms {iterations}");
}
