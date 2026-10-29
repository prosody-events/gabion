//! Every scalar operator setting, named, parsed and validated once for both
//! adapters. A row of the table gives the `gabiond` YAML key (also the
//! `config_key` log field), its environment variable, its NGINX directive,
//! the [`Settings`] field and type that hold it, its default and, for a count,
//! a capacity or a duration, what a zero would break.
//!
//! An adapter starts from [`Settings::default`], passes each value's text to
//! [`Settings::apply`] and calls [`Settings::validate`] once, at load: the
//! gossip runtime and the cell store take only the validated configs it
//! returns. The library parses scalar text only, never YAML or NGINX
//! configuration.

use std::ffi::CStr;
use std::fmt;
use std::net::SocketAddr;
use std::num::{
    IntErrorKind, NonZeroU8, NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU128, NonZeroUsize,
    ParseIntError,
};
use std::time::Duration;

use crate::crdt::{MAX_DICTIONARY_CAPACITY, ValidStorageConfig};
use crate::defaults;
use crate::discovery::DiscoveryConfig;
use crate::gossip::ValidGossipConfig;
use crate::rules::CardinalityLimits;
use crate::wire::{FrameLimits, MAX_PAYLOAD_BYTES, MIN_PAYLOAD_BYTES};

/// A non-zero default; zero fails to compile.
macro_rules! nz {
    ($value:expr) => {
        const { std::num::NonZero::new($value).unwrap() }
    };
}

macro_rules! settings {
    ($(
        $variant:ident => $yaml:literal, $env:literal $(, $nginx:literal)?;
            $field:ident: $ty:ty = $default:expr $(, $zero:literal)?;
    )*) => {
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
                    $(Setting::$variant => [$($nginx)?].first().copied(),)*
                }
            }

            /// [`Self::nginx_directive`] NUL-terminated, as NGINX's command
            /// table needs it: nginx compares directive names with `strcmp`.
            pub const fn nginx_directive_cstr(self) -> Option<&'static CStr> {
                match self {
                    $(Setting::$variant => [$(const {
                        match CStr::from_bytes_with_nul(concat!($nginx, "\0").as_bytes()) {
                            Ok(name) => name,
                            Err(_) => panic!("a directive name holds no NUL"),
                        }
                    })?].first().copied(),)*
                }
            }

            /// What a zero would break, for a count, capacity or duration.
            pub fn zero_reason(self) -> Option<&'static str> {
                match self {
                    $(Setting::$variant => [$($zero)?].first().copied(),)*
                }
            }

            /// The default as an operator writes it, or `None` when the
            /// setting is unset by default.
            pub fn default_text(self) -> Option<String> {
                match self {
                    $(Setting::$variant => <$ty as Field>::show(&$default),)*
                }
            }

            /// True for a list, which each value extends: a comma-separated
            /// environment variable, a YAML sequence, a repeated directive.
            pub fn is_list(self) -> bool {
                match self {
                    $(Setting::$variant => <$ty as Field>::LIST,)*
                }
            }
        }

        /// Every setting's value, typed so that a zero count, capacity or
        /// duration cannot be stored. Cross-setting rules and ranges are
        /// [`Settings::validate`]'s.
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct Settings {
            $(pub $field: $ty,)*
        }

        impl Default for Settings {
            fn default() -> Self {
                Self {
                    $($field: $default,)*
                }
            }
        }

        impl Settings {
            /// Parse `text` into `key`'s field. A list setting appends.
            pub fn apply(&mut self, key: Setting, text: &str) -> Result<(), SettingError> {
                let text = text.trim();
                let parsed = match key {
                    $(Setting::$variant => Field::set(&mut self.$field, text),)*
                };
                parsed.map_err(|problem| match problem {
                    Problem::Zero => SettingError::NotPositive { setting: key },
                    Problem::Malformed(expected, example) => SettingError::Malformed {
                        setting: key,
                        value: text.into(),
                        expected,
                        example,
                    },
                })
            }
        }
    };
}

settings! {
    EnvoyBind => "envoy_bind", "GABION_ENVOY_BIND";
        envoy_bind: Option<SocketAddr> = None;
    AdminBind => "admin_bind", "GABION_ADMIN_BIND";
        admin_bind: Option<SocketAddr> = None;
    StorageMaxCells => "storage.max_cells", "GABION_STORAGE_MAX_CELLS",
        "gabion_storage_max_cells";
        max_cells: NonZeroU32 = nz!(defaults::STORAGE_MAX_CELLS as u32),
        "gabion could not track any rate-limit key";
    StorageRuleDictionaryCapacity => "storage.rule_dictionary_capacity",
        "GABION_STORAGE_RULE_DICTIONARY_CAPACITY", "gabion_storage_rule_dictionary_capacity";
        rule_dictionary_capacity: Option<NonZeroU16> = None,
        "gabion could not register any rate-limit rule; the default is twice the configured \
         rules, at least 256";
    StorageNodeDictionaryCapacity => "storage.node_dictionary_capacity",
        "GABION_STORAGE_NODE_DICTIONARY_CAPACITY", "gabion_storage_node_dictionary_capacity";
        node_dictionary_capacity: Option<NonZeroU16> = None,
        "gabion could not count the hits of any node, not even this one; the default is twice \
         `gossip.peer_capacity`";
    StorageMaxDescriptorCount => "storage.max_descriptor_count",
        "GABION_STORAGE_MAX_DESCRIPTOR_COUNT", "gabion_storage_max_descriptor_count";
        max_descriptor_count: NonZeroU16 = nz!(defaults::STORAGE_MAX_DESCRIPTOR_COUNT),
        "every rule and every descriptor would have too many entries";
    StorageMaxDescriptorBytes => "storage.max_descriptor_bytes",
        "GABION_STORAGE_MAX_DESCRIPTOR_BYTES", "gabion_storage_max_descriptor_bytes";
        max_descriptor_bytes: NonZeroUsize = nz!(defaults::STORAGE_MAX_DESCRIPTOR_BYTES),
        "every rate-limited request would be rejected as too large";
    StorageMaxKeyBytes => "storage.max_key_bytes", "GABION_STORAGE_MAX_KEY_BYTES",
        "gabion_storage_max_key_bytes";
        max_key_bytes: NonZeroUsize = nz!(defaults::STORAGE_MAX_KEY_BYTES),
        "no descriptor key would fit";
    StorageMaxRequestDescriptors => "storage.max_request_descriptors",
        "GABION_STORAGE_MAX_REQUEST_DESCRIPTORS";
        max_request_descriptors: NonZeroU16 = nz!(defaults::STORAGE_MAX_REQUEST_DESCRIPTORS),
        "every Envoy request with a rate-limit descriptor would be rejected";
    StorageMaxMatchedRules => "storage.max_matched_rules", "GABION_STORAGE_MAX_MATCHED_RULES",
        "gabion_storage_max_matched_rules";
        max_matched_rules: NonZeroU16 = nz!(defaults::STORAGE_MAX_MATCHED_RULES),
        "no request could be counted against any rule";
    RuntimeNodeIdSeed => "runtime.node_id_seed", "GABION_RUNTIME_NODE_ID_SEED",
        "gabion_node_id_seed";
        node_id_seed: Option<Box<str>> = None;
    GossipBind => "gossip.bind", "GABION_GOSSIP_BIND", "gabion_gossip_bind";
        gossip_bind: Option<SocketAddr> = None;
    GossipTickInterval => "gossip.tick_interval", "GABION_GOSSIP_TICK_INTERVAL",
        "gabion_gossip_tick_interval";
        tick_interval: Millis = Millis(nz!(defaults::GOSSIP_TICK_INTERVAL_MILLIS)),
        "gossip needs a positive interval between heartbeats";
    GossipPeerCapacity => "gossip.peer_capacity", "GABION_GOSSIP_PEER_CAPACITY",
        "gabion_gossip_peer_capacity";
        peer_capacity: NonZeroU16 = nz!(defaults::GOSSIP_PEER_CAPACITY),
        "this node could not track any peer";
    GossipMaxPayloadBytes => "gossip.max_payload_bytes", "GABION_GOSSIP_MAX_PAYLOAD_BYTES",
        "gabion_gossip_max_payload_bytes";
        max_payload_bytes: usize = defaults::GOSSIP_MAX_PAYLOAD_BYTES;
    GossipLimitQueueCapacity => "gossip.limit_queue_capacity",
        "GABION_GOSSIP_LIMIT_QUEUE_CAPACITY", "gabion_gossip_limit_queue_capacity";
        limit_queue_capacity: NonZeroUsize = nz!(defaults::GOSSIP_LIMIT_QUEUE_CAPACITY),
        "no counted hit could reach the gossip runtime";
    GossipRecvBufferBytes => "gossip.recv_buffer_bytes", "GABION_GOSSIP_RECV_BUFFER_BYTES",
        "gabion_gossip_recv_buffer_bytes";
        recv_buffer_bytes: NonZeroU32 = nz!(defaults::GOSSIP_RECV_BUFFER_BYTES),
        "the gossip socket could not hold any packet it receives";
    GossipSendBufferBytes => "gossip.send_buffer_bytes", "GABION_GOSSIP_SEND_BUFFER_BYTES",
        "gabion_gossip_send_buffer_bytes";
        send_buffer_bytes: NonZeroU32 = nz!(defaults::GOSSIP_SEND_BUFFER_BYTES),
        "the gossip socket could not queue any packet it sends";
    GossipClusterIdHash => "gossip.cluster_id_hash", "GABION_GOSSIP_CLUSTER_ID_HASH",
        "gabion_gossip_cluster";
        cluster_id_hash: NonZeroU128 = nz!(defaults::GOSSIP_CLUSTER_ID_HASH);
    GossipTargetErrBps => "gossip.target_err_bps", "GABION_GOSSIP_TARGET_ERR_BPS",
        "gabion_gossip_target_err_bps";
        target_err_bps: u32 = defaults::GOSSIP_TARGET_ERR_BPS;
    GossipMinEmitInterval => "gossip.min_emit_interval", "GABION_GOSSIP_MIN_EMIT_INTERVAL",
        "gabion_gossip_min_emit_interval";
        min_emit_interval: Millis = Millis(nz!(defaults::GOSSIP_MIN_EMIT_INTERVAL_MS)),
        "gossip needs a positive gap between two sends";
    GossipSuspectAfter => "gossip.suspect_after", "GABION_GOSSIP_SUSPECT_AFTER",
        "gabion_gossip_suspect_after";
        suspect_after: Option<Millis> = None,
        "a peer would be routed around the moment one packet to it went unanswered; the default \
         is four heartbeats and at least 2 s";
    GossipSweepConcurrency => "gossip.sweep_concurrency", "GABION_GOSSIP_SWEEP_CONCURRENCY",
        "gabion_gossip_sweep_concurrency";
        sweep_concurrency: NonZeroU8 = nz!(defaults::GOSSIP_SWEEP_CONCURRENCY),
        "this node could not send any newly started peer its live counts";
    DiscoverySelfAddr => "discovery.self_addr", "GABION_DISCOVERY_SELF_ADDR",
        "gabion_discovery_self_addr";
        self_addr: Option<SocketAddr> = None;
    DiscoveryNamespaceAllow => "discovery.namespace_allow", "GABION_DISCOVERY_NAMESPACE_ALLOW",
        "gabion_discovery_namespace_allow";
        namespace_allow: Box<[Box<str>]> = Box::default();
    DiscoveryServiceAllow => "discovery.service_allow", "GABION_DISCOVERY_SERVICE_ALLOW",
        "gabion_discovery_service_allow";
        service_allow: Box<[Box<str>]> = Box::default();
}

/// The smallest `gossip.limit_queue_capacity`, and the depth of the runtime's
/// request queue under [`ValidGossipConfig::with_shallow_limit_queue`].
pub const MIN_LIMIT_QUEUE_CAPACITY: NonZeroUsize = nz!(64);

/// The smallest socket buffer either `gossip.*_buffer_bytes` setting may ask
/// for.
pub const MIN_SOCKET_BUFFER_BYTES: NonZeroU32 = nz!(65_536);

/// The largest: the kernel doubles the request and keeps it in a C `int`.
const MAX_SOCKET_BUFFER_BYTES: u64 = (1 << 30) - 1;

/// The largest error budget: 10,000 basis points are the whole limit.
const MAX_ERROR_BUDGET_BPS: u32 = 10_000;

impl Settings {
    /// The discovery settings, for the adapter's discovery stream.
    pub fn discovery(&self) -> DiscoveryConfig {
        DiscoveryConfig {
            self_addr: self.self_addr,
            namespace_allow: self.namespace_allow.clone(),
            service_allow: self.service_allow.clone(),
        }
    }

    /// Check the ranges and cross-setting rules, and build the configs the
    /// gossip runtime and the cell store take. Each adapter calls this once,
    /// while loading its configuration.
    pub fn validate(&self) -> Result<(ValidGossipConfig, ValidStorageConfig), SettingError> {
        let Some(wire_limits) = FrameLimits::new(self.max_payload_bytes) else {
            return Err(SettingError::PayloadOutOfRange {
                configured: self.max_payload_bytes,
                min: MIN_PAYLOAD_BYTES,
                max: MAX_PAYLOAD_BYTES,
            });
        };
        let Some(target_err_bps) =
            NonZeroU32::new(self.target_err_bps).filter(|bps| bps.get() <= MAX_ERROR_BUDGET_BPS)
        else {
            return Err(SettingError::ErrorBudgetOutOfRange {
                configured: self.target_err_bps,
            });
        };
        if self.min_emit_interval >= self.tick_interval {
            return Err(SettingError::EmitIntervalOutOfRange {
                configured: self.min_emit_interval,
                tick: self.tick_interval,
            });
        }
        let two_ticks = Millis(self.tick_interval.0.saturating_mul(nz!(2)));
        if let Some(configured) = self.suspect_after.filter(|&after| after < two_ticks) {
            return Err(SettingError::SuspectAfterTooShort {
                configured,
                min: two_ticks,
            });
        }
        let heartbeats = (self.tick_interval.0)
            .saturating_mul(nz!(defaults::GOSSIP_SUSPECT_AFTER_TICKS))
            .max(nz!(defaults::GOSSIP_SUSPECT_AFTER_FLOOR_MS));
        let suspect_after = self.suspect_after.unwrap_or(Millis(heartbeats));
        let socket = "The kernel doubles the request and keeps it in a C `int`, so it grants at \
                      most 1,073,741,823 bytes; below 64 KiB one burst of gossip packets \
                      overflows it. The host's `net.core.rmem_max` (receive) and \
                      `net.core.wmem_max` (send) cap what it grants; see \"Socket buffers\" in \
                      the gabion README.";
        let node_dictionary_capacity = self
            .node_dictionary_capacity
            .unwrap_or(self.peer_capacity.saturating_mul(nz!(2)));
        let capacities = [
            (
                Setting::StorageMaxCells,
                u64::from(self.max_cells.get()),
                1,
                1 << 30,
                "The key index holds twice this many cells, rounded up to a power of two, and \
                 numbers them with a `u32`, so it supports at most 1,073,741,824 cells.",
            ),
            (
                Setting::GossipLimitQueueCapacity,
                self.limit_queue_capacity.get() as u64,
                MIN_LIMIT_QUEUE_CAPACITY.get() as u64,
                1 << 20,
                "It may be 64 to 1,048,576; each slot is 72 bytes of the NGINX zone (72 MiB at \
                 the ceiling), so raise SIZE in `gabion_limit_zone` with it.",
            ),
            (
                Setting::GossipRecvBufferBytes,
                u64::from(self.recv_buffer_bytes.get()),
                u64::from(MIN_SOCKET_BUFFER_BYTES.get()),
                MAX_SOCKET_BUFFER_BYTES,
                socket,
            ),
            (
                Setting::GossipSendBufferBytes,
                u64::from(self.send_buffer_bytes.get()),
                u64::from(MIN_SOCKET_BUFFER_BYTES.get()),
                MAX_SOCKET_BUFFER_BYTES,
                socket,
            ),
            (
                Setting::StorageRuleDictionaryCapacity,
                self.rule_dictionary_capacity.map_or(1, |c| c.get().into()),
                1,
                MAX_DICTIONARY_CAPACITY.into(),
                "Its slots are numbered by a `u16` whose largest value marks an empty slot, so it \
                 holds at most 65,534 rules. Remove it to use the default, twice the configured \
                 rules and at least 256.",
            ),
            (
                Setting::GossipPeerCapacity,
                u64::from(self.peer_capacity.get()),
                1,
                u64::from(MAX_DICTIONARY_CAPACITY / 2),
                "The node dictionary's default is twice this many origins, and its slots are \
                 numbered by a `u16` whose largest value marks an empty slot, so it holds at most \
                 65,534; a cluster larger than 32,767 pods is outside what gabion supports.",
            ),
            (
                Setting::StorageNodeDictionaryCapacity,
                u64::from(node_dictionary_capacity.get()),
                u64::from(self.peer_capacity.get()) + 1,
                MAX_DICTIONARY_CAPACITY.into(),
                "It must exceed `gossip.peer_capacity`, because every peer and this node need an \
                 entry, and its slots are numbered by a `u16` whose largest value marks an empty \
                 slot, so it holds at most 65,534. Remove it to use the default, twice \
                 `gossip.peer_capacity`.",
            ),
        ];
        for (setting, configured, min, max, reason) in capacities {
            if !(min..=max).contains(&configured) {
                return Err(SettingError::CapacityOutOfRange {
                    setting,
                    configured,
                    min,
                    max,
                    reason,
                });
            }
        }
        let gossip = ValidGossipConfig {
            cluster_id_hash: self.cluster_id_hash,
            bootstrap_peers: Vec::new(),
            peer_capacity: self.peer_capacity,
            quantum: quantum(self.max_cells, wire_limits),
            sweep_concurrency: self.sweep_concurrency,
            wire_limits,
            limit_queue_capacity: self.limit_queue_capacity,
            #[cfg(feature = "transport-udp")]
            recv_buffer_bytes: self.recv_buffer_bytes,
            send_buffer_bytes: self.send_buffer_bytes,
            tick_interval: self.tick_interval.duration(),
            target_err_bps,
            min_emit_interval: self.min_emit_interval,
            suspect_after,
            zone: None,
        };
        let storage = ValidStorageConfig {
            cell_capacity: self.max_cells,
            rule_dictionary_capacity: self.rule_dictionary_capacity,
            max_matched_rules: self.max_matched_rules,
            node_dictionary_capacity,
            cardinality: CardinalityLimits {
                max_descriptor_count: usize::from(self.max_descriptor_count.get()),
                max_descriptor_bytes: self.max_descriptor_bytes.get(),
                max_key_bytes: self.max_key_bytes.get(),
                max_request_descriptors: usize::from(self.max_request_descriptors.get()),
            },
        };
        Ok((gossip, storage))
    }
}

/// The per-round quantum `q`: the most cells one stream sends one peer in one
/// round, so a full store's backlog drains in
/// [`defaults::GOSSIP_BACKLOG_ROUNDS`] rounds, and never less than one packet.
fn quantum(max_cells: NonZeroU32, wire_limits: FrameLimits) -> NonZeroUsize {
    let backlog = (max_cells.get() as usize).div_ceil(defaults::GOSSIP_BACKLOG_ROUNDS);
    NonZeroUsize::new(backlog.max(wire_limits.cell_bound())).unwrap_or(NonZeroUsize::MIN)
}

/// A positive duration in whole milliseconds, the resolution of the timers
/// that use it.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Millis(pub NonZeroU64);

impl Millis {
    pub fn duration(self) -> Duration {
        Duration::from_millis(self.0.get())
    }
}

impl fmt::Display for Millis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_duration(self.duration()).fmt(f)
    }
}

/// Why one setting's text did not parse.
enum Problem {
    Zero,
    /// What the text must be, and an example.
    Malformed(&'static str, &'static str),
}

/// How a field's type parses and prints its setting's text.
trait Field {
    const LIST: bool = false;

    fn set(&mut self, text: &str) -> Result<(), Problem>;

    /// The value as an operator writes it, or `None` when unset.
    fn show(&self) -> Option<String>;
}

macro_rules! int_field {
    ($($ty:ty => $expected:literal, $example:literal;)*) => {$(
        impl Field for $ty {
            fn set(&mut self, text: &str) -> Result<(), Problem> {
                *self = text.parse().map_err(|error: ParseIntError| match error.kind() {
                    IntErrorKind::Zero => Problem::Zero,
                    _ => Problem::Malformed($expected, $example),
                })?;
                Ok(())
            }

            fn show(&self) -> Option<String> {
                Some(self.to_string())
            }
        }
    )*};
}

int_field! {
    NonZeroU8 => "a whole number from 1 to 255", "4";
    NonZeroU16 => "a whole number from 1 to 65535", "100";
    NonZeroU32 => "a whole number from 1 to 4294967295", "100";
    NonZeroUsize => "a positive whole number", "100";
    usize => "a whole number", "1400";
    u32 => "a whole number from 0 to 4294967295", "100";
}

macro_rules! optional_field {
    ($($ty:ty => $expected:literal, $example:literal;)*) => {$(
        impl Field for Option<$ty> {
            fn set(&mut self, text: &str) -> Result<(), Problem> {
                *self = Some(text.parse().map_err(|_| Problem::Malformed($expected, $example))?);
                Ok(())
            }

            fn show(&self) -> Option<String> {
                self.as_ref().map(ToString::to_string)
            }
        }
    )*};
}

optional_field! {
    SocketAddr => "a socket address", "0.0.0.0:9000";
}

/// An override with a derived default: unset until set, and 0 is not
/// positive, as for the plain value.
macro_rules! override_field {
    ($($ty:ty = $seed:expr;)*) => {$(
        impl Field for Option<$ty> {
            fn set(&mut self, text: &str) -> Result<(), Problem> {
                let mut value = $seed;
                value.set(text)?;
                *self = Some(value);
                Ok(())
            }

            fn show(&self) -> Option<String> {
                self.map(|value| value.to_string())
            }
        }
    )*};
}

override_field! {
    NonZeroU16 = NonZeroU16::MIN;
    Millis = Millis(NonZeroU64::MIN);
}

impl Field for Option<Box<str>> {
    fn set(&mut self, text: &str) -> Result<(), Problem> {
        if text.is_empty() {
            return Err(Problem::Malformed("a non-empty string", "pod-0"));
        }
        *self = Some(text.into());
        Ok(())
    }

    fn show(&self) -> Option<String> {
        self.as_deref().map(str::to_owned)
    }
}

impl Field for Millis {
    fn set(&mut self, text: &str) -> Result<(), Problem> {
        let malformed = Problem::Malformed("a duration in whole milliseconds", "500ms");
        let Ok(duration) = humantime::parse_duration(text) else {
            return Err(malformed);
        };
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        if Duration::from_millis(millis) != duration {
            return Err(malformed);
        }
        *self = Millis(NonZeroU64::new(millis).ok_or(Problem::Zero)?);
        Ok(())
    }

    fn show(&self) -> Option<String> {
        Some(self.to_string())
    }
}

impl Field for NonZeroU128 {
    /// Decimal (`12345`) or `0x`-prefixed hex (`0xc0ffee`), as both adapters
    /// spell a cluster id; `0` and a sign are malformed.
    fn set(&mut self, text: &str) -> Result<(), Problem> {
        let malformed = Problem::Malformed(
            "a non-zero cluster id in decimal or `0x`-prefixed hex",
            "0xc0ffee",
        );
        let (digits, radix) = match text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
            Some(hex) => (hex, 16),
            None => (text, 10),
        };
        // `from_str_radix` also takes a leading `+`; an id is digits only.
        if !digits.starts_with(|c: char| c.is_digit(radix)) {
            return Err(malformed);
        }
        *self = u128::from_str_radix(digits, radix)
            .ok()
            .and_then(NonZeroU128::new)
            .ok_or(malformed)?;
        Ok(())
    }

    fn show(&self) -> Option<String> {
        Some(self.to_string())
    }
}

/// The Kubernetes allow-lists, the only list settings: each value appends one
/// RFC 1123 DNS label, `[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?`, so a typo fails
/// to load instead of silently matching nothing.
impl Field for Box<[Box<str>]> {
    const LIST: bool = true;

    fn set(&mut self, text: &str) -> Result<(), Problem> {
        let bytes = text.as_bytes();
        let edge = |b: Option<&u8>| b.is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
        let label = bytes.len() <= 63
            && edge(bytes.first())
            && edge(bytes.last())
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
        if !label {
            return Err(Problem::Malformed(
                "a Kubernetes name: 1 to 63 lower-case letters, digits or `-`, starting and \
                 ending with a letter or digit",
                "gabion",
            ));
        }
        let mut list = std::mem::take(self).into_vec();
        list.push(text.into());
        *self = list.into_boxed_slice();
        Ok(())
    }

    fn show(&self) -> Option<String> {
        (!self.is_empty()).then(|| self.join(","))
    }
}

/// A setting that fails to parse or validate. Each text names the setting in
/// both adapters' spellings.
#[derive(Debug, Eq, PartialEq)]
pub enum SettingError {
    NotPositive {
        setting: Setting,
    },
    Malformed {
        setting: Setting,
        value: Box<str>,
        expected: &'static str,
        example: &'static str,
    },
    PayloadOutOfRange {
        configured: usize,
        min: usize,
        max: usize,
    },
    ErrorBudgetOutOfRange {
        configured: u32,
    },
    EmitIntervalOutOfRange {
        configured: Millis,
        tick: Millis,
    },
    SuspectAfterTooShort {
        configured: Millis,
        /// Two heartbeats.
        min: Millis,
    },
    CapacityOutOfRange {
        setting: Setting,
        configured: u64,
        min: u64,
        max: u64,
        /// Why the range is what it is, and what to do.
        reason: &'static str,
    },
}

impl fmt::Display for SettingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::NotPositive { setting } => {
                write!(
                    f,
                    "`{}` is 0, but it must be at least 1",
                    setting.yaml_key()
                )?;
                if let Some(reason) = setting.zero_reason() {
                    write!(f, ": {reason}")?;
                }
                f.write_str(". Remove the setting to use the default")?;
                // A derived default has no fixed text; its zero reason gives
                // it.
                if let Some(default) = setting.default_text() {
                    write!(f, ", {default}")?;
                }
                write!(f, ", or set a positive value ({}).", Alternatives(setting))
            }
            Self::Malformed {
                setting,
                ref value,
                expected,
                example,
            } => {
                write!(
                    f,
                    "`{}` is `{value}`, which is not {expected}, for example `{example}`. Correct \
                     it ({}) or remove it to ",
                    setting.yaml_key(),
                    Alternatives(setting)
                )?;
                match setting.default_text() {
                    Some(default) => write!(f, "use the default, {default}."),
                    None => f.write_str("leave it unset."),
                }
            }
            Self::PayloadOutOfRange {
                configured,
                min,
                max,
            } => write!(
                f,
                "The gossip packet size is {configured} bytes, but gabion needs between {min} and \
                 {max}: {min} bytes hold the packet header and one counter, and {max} is the \
                 largest UDP payload over IPv4. Set {} inside that range, and to the same value \
                 on every node. The default, {}, fits a standard 1,500-byte Ethernet path.",
                Setting::GossipMaxPayloadBytes,
                defaults::GOSSIP_MAX_PAYLOAD_BYTES
            ),
            Self::ErrorBudgetOutOfRange { configured } => write!(
                f,
                "The gossip error budget is {configured} basis points, outside 1 to \
                 {MAX_ERROR_BUDGET_BPS} (0.01 % to 100 % of a rule's limit). Set {} in that \
                 range; the default, {}, is 1 % of each rule's limit.",
                Setting::GossipTargetErrBps,
                defaults::GOSSIP_TARGET_ERR_BPS
            ),
            Self::EmitIntervalOutOfRange { configured, tick } => write!(
                f,
                "{} is {configured}, but it must be at least 1ms and shorter than \
                 `gossip.tick_interval` ({tick}). It is the shortest gap between two gossip sends \
                 other than the heartbeat. At or above the heartbeat, every change would wait for \
                 the heartbeat and limits would be enforced late. Set it below {tick}, or remove \
                 it to use the default of {}.",
                Setting::GossipMinEmitInterval,
                Setting::GossipMinEmitInterval
                    .default_text()
                    .unwrap_or_default()
            ),
            Self::SuspectAfterTooShort { configured, min } => write!(
                f,
                "{} is {configured}, shorter than two heartbeats ({min}, twice \
                 `gossip.tick_interval`). A single lost packet would then move this node's \
                 traffic off a healthy peer. Set it to at least {min}, or remove it to use the \
                 default, four heartbeats and at least 2 s.",
                Setting::GossipSuspectAfter,
            ),
            Self::CapacityOutOfRange {
                setting,
                configured,
                min,
                max,
                reason,
            } => {
                write!(
                    f,
                    "`{}` is {configured}; gabion supports {min} to {max}.",
                    setting.yaml_key()
                )?;
                if let Some(default) = setting.default_text() {
                    write!(f, " The default is {default}.")?;
                }
                write!(
                    f,
                    " Set it inside that range ({}). {reason}",
                    Alternatives(setting)
                )
            }
        }
    }
}

impl std::error::Error for SettingError {}

/// A setting's other spellings: `` `GABION_X`; NGINX `gabion_x` ``, with no
/// NGINX part for a `gabiond`-only setting.
struct Alternatives(Setting);

impl fmt::Display for Alternatives {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}`", self.0.env_var())?;
        match self.0.nginx_directive() {
            Some(directive) => write!(f, "; NGINX `{directive}`"),
            None => Ok(()),
        }
    }
}

/// Formats the way messages name a setting: `` `storage.max_cells`
/// (`GABION_STORAGE_MAX_CELLS`; NGINX `gabion_storage_max_cells`) ``.
impl fmt::Display for Setting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}` ({})", self.yaml_key(), Alternatives(*self))
    }
}

#[cfg(test)]
pub(crate) mod mock;
#[cfg(test)]
mod tests;
