//! Every operator setting, named once: the `gabiond` YAML key (also the
//! `config_key` log field), its environment variable, and its NGINX
//! directive. Messages interpolate a [`Setting`] instead of spelling it.

use std::fmt;

macro_rules! settings {
    ($($variant:ident => $yaml:literal, $env:literal, $nginx:expr;)*) => {
        /// One operator setting.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum Setting {
            $($variant,)*
        }

        impl Setting {
            /// Every setting, in table order.
            pub const ALL: &[Setting] = &[$(Setting::$variant,)*];

            /// Dotted `gabiond` YAML key; also the `config_key` log field.
            pub const fn yaml_key(self) -> &'static str {
                match self {
                    $(Setting::$variant => $yaml,)*
                }
            }

            /// `gabiond` environment variable, which wins over the YAML file.
            pub const fn env_var(self) -> &'static str {
                match self {
                    $(Setting::$variant => $env,)*
                }
            }

            /// NGINX directive, or `None` for a `gabiond`-only setting.
            pub const fn nginx_directive(self) -> Option<&'static str> {
                match self {
                    $(Setting::$variant => $nginx,)*
                }
            }
        }
    };
}

settings! {
    EnvoyBind => "envoy_bind", "GABION_ENVOY_BIND", None;
    AdminBind => "admin_bind", "GABION_ADMIN_BIND", None;
    StorageMaxCells => "storage.max_cells", "GABION_STORAGE_MAX_CELLS",
        Some("gabion_storage_max_cells");
    StorageRuleDictionaryCapacity => "storage.rule_dictionary_capacity",
        "GABION_STORAGE_RULE_DICTIONARY_CAPACITY",
        Some("gabion_storage_rule_dictionary_capacity");
    StorageNodeDictionaryCapacity => "storage.node_dictionary_capacity",
        "GABION_STORAGE_NODE_DICTIONARY_CAPACITY",
        Some("gabion_storage_node_dictionary_capacity");
    StorageLocalDirtyCapacity => "storage.local_dirty_capacity",
        "GABION_STORAGE_LOCAL_DIRTY_CAPACITY",
        Some("gabion_storage_local_dirty_capacity");
    StorageForwardedDirtyCapacity => "storage.forwarded_dirty_capacity",
        "GABION_STORAGE_FORWARDED_DIRTY_CAPACITY",
        Some("gabion_storage_forwarded_dirty_capacity");
    StoragePeerCapacity => "storage.peer_capacity", "GABION_STORAGE_PEER_CAPACITY",
        Some("gabion_storage_peer_capacity");
    StorageMaxDescriptorCount => "storage.max_descriptor_count",
        "GABION_STORAGE_MAX_DESCRIPTOR_COUNT", Some("gabion_storage_max_descriptor_count");
    StorageMaxDescriptorBytes => "storage.max_descriptor_bytes",
        "GABION_STORAGE_MAX_DESCRIPTOR_BYTES", Some("gabion_storage_max_descriptor_bytes");
    StorageMaxKeyBytes => "storage.max_key_bytes", "GABION_STORAGE_MAX_KEY_BYTES",
        Some("gabion_storage_max_key_bytes");
    RuntimeNodeIdSeed => "runtime.node_id_seed", "GABION_RUNTIME_NODE_ID_SEED",
        Some("gabion_node_id_seed");
    RuntimeRngSeed => "runtime.rng_seed", "GABION_RUNTIME_RNG_SEED",
        Some("gabion_runtime_rng_seed");
    GossipBind => "gossip.bind", "GABION_GOSSIP_BIND", Some("gabion_gossip_bind");
    GossipTickInterval => "gossip.tick_interval", "GABION_GOSSIP_TICK_INTERVAL",
        Some("gabion_gossip_tick_interval");
    GossipFanout => "gossip.fanout", "GABION_GOSSIP_FANOUT", Some("gabion_gossip_fanout");
    GossipMaxPayloadBytes => "gossip.max_payload_bytes", "GABION_GOSSIP_MAX_PAYLOAD_BYTES",
        Some("gabion_gossip_max_payload_bytes");
    GossipMaxCellsPerFrame => "gossip.max_cells_per_frame",
        "GABION_GOSSIP_MAX_CELLS_PER_FRAME", Some("gabion_gossip_max_cells_per_frame");
    GossipMaxCellsPerTick => "gossip.max_cells_per_tick", "GABION_GOSSIP_MAX_CELLS_PER_TICK",
        Some("gabion_gossip_max_cells_per_tick");
    GossipSendQueueCapacity => "gossip.send_queue_capacity",
        "GABION_GOSSIP_SEND_QUEUE_CAPACITY", Some("gabion_gossip_send_queue_capacity");
    GossipLimitQueueCapacity => "gossip.limit_queue_capacity",
        "GABION_GOSSIP_LIMIT_QUEUE_CAPACITY", None;
    GossipClusterIdHash => "gossip.cluster_id_hash", "GABION_GOSSIP_CLUSTER_ID_HASH",
        Some("gabion_gossip_cluster");
    GossipTargetErrBps => "gossip.target_err_bps", "GABION_GOSSIP_TARGET_ERR_BPS",
        Some("gabion_gossip_target_err_bps");
    GossipMinEmitInterval => "gossip.min_emit_interval", "GABION_GOSSIP_MIN_EMIT_INTERVAL",
        Some("gabion_gossip_min_emit_interval");
    DiscoverySelfAddr => "discovery.self_addr", "GABION_DISCOVERY_SELF_ADDR",
        Some("gabion_discovery_self_addr");
    DiscoveryNamespaceAllow => "discovery.namespace_allow",
        "GABION_DISCOVERY_NAMESPACE_ALLOW", Some("gabion_discovery_namespace_allow");
    DiscoveryServiceAllow => "discovery.service_allow", "GABION_DISCOVERY_SERVICE_ALLOW",
        Some("gabion_discovery_service_allow");
}

/// Formats the way messages name a setting: `` `storage.max_cells`
/// (`GABION_STORAGE_MAX_CELLS`; NGINX `gabion_storage_max_cells`) ``, with no
/// NGINX part for a `gabiond`-only setting.
impl fmt::Display for Setting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}` (`{}`", self.yaml_key(), self.env_var())?;
        if let Some(directive) = self.nginx_directive() {
            write!(f, "; NGINX `{directive}`")?;
        }
        f.write_str(")")
    }
}
