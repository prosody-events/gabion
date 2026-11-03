//! This node's own zone: the setting, else its Kubernetes EndpointSlice
//! entry, else none, and the three messages that report it (`zone_known`,
//! `zone_unknown`, `zone_conflict`). Nothing here runs per packet.

use crate::discovery::{Membership, SelfMatch};
use crate::settings::Setting;
use crate::zone::{UnknownZone, ZoneHash, ZoneName, ZoneSource, ZoneStatus};

use super::ValidGossipConfig;

/// What `zone_unknown` opens with, whatever the reason.
const ONE_ZONE: &str = "gabion does not know which availability zone this node runs in, so it \
                        treats the whole cluster as one zone and sends every change to every peer \
                        directly. Counts stay correct, but none of the cross-zone traffic that \
                        zone-aware gossip saves is saved.";

/// What `zone_unknown` closes with.
const SINGLE_ZONE: &str = "If this cluster runs in a single zone, set the zone setting to any \
                           name to silence this warning. If a later `zone_known` line shows this \
                           node's zone, discovery was only slow and this warning no longer \
                           applies.";

/// The node's zone and what is left to say about it; only its own methods
/// move it from state to state.
pub(super) struct OwnZone(State);

enum State {
    /// `gossip.zone`; an own entry that names another zone is reported once.
    Configured {
        name: ZoneName,
        conflict_logged: bool,
    },
    /// Read from the own EndpointSlice entry; it never changes.
    Learned(ZoneName),
    /// Kubernetes discovery runs and has not named the zone yet.
    Awaiting { entry: Entry, warning: Warning },
    /// A static list or no peers, and no setting: no Kubernetes discovery
    /// runs to name the zone, so nothing waits for it.
    Unconfigured,
}

/// What discovery has said of the own entry while the zone is awaited.
enum Entry {
    NotFound,
    /// Found, naming no zone; the Kubernetes node it runs on is
    /// `zone_unknown`'s advice.
    NoZone {
        node_name: Option<Box<str>>,
    },
}

/// When `zone_unknown` fires.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Warning {
    /// Not before the first peer event.
    Unarmed,
    /// At the first heartbeat at or after this instant.
    Due(u64),
    /// Logged.
    Done,
}

/// A zone the own entry named while none was known, which only
/// [`OwnZone::own_entry`] makes, once, so the peer table adopts at most one.
pub(super) struct LearnedZone(ZoneHash);

impl LearnedZone {
    pub(super) fn hash(self) -> ZoneHash {
        self.0
    }
}

impl OwnZone {
    /// The setting's zone, else a wait for the own entry on Kubernetes, else
    /// none.
    pub(super) fn new(config: &ValidGossipConfig) -> Self {
        Self(match (&config.zone, &config.membership) {
            (Some(name), _) => State::Configured {
                name: name.clone(),
                conflict_logged: false,
            },
            (None, Membership::Kubernetes { .. }) => State::Awaiting {
                entry: Entry::NotFound,
                warning: Warning::Unarmed,
            },
            (None, Membership::Static { .. } | Membership::Alone) => State::Unconfigured,
        })
    }

    /// The zone, or why none is known.
    pub(super) fn status(&self) -> ZoneStatus {
        match &self.0 {
            State::Configured { name, .. } => ZoneStatus::Known {
                name: name.clone(),
                source: ZoneSource::Config,
            },
            State::Learned(name) => ZoneStatus::Known {
                name: name.clone(),
                source: ZoneSource::EndpointSlice,
            },
            State::Awaiting { entry, .. } => ZoneStatus::Unknown(entry.reason()),
            State::Unconfigured => ZoneStatus::Unknown(UnknownZone::NotConfigured),
        }
    }

    /// The runtime starts: a configured zone says so, and a static list with
    /// no zone setting warns now, since nothing else can name the zone. A
    /// node with no peers never warns.
    pub(super) fn start(&self, config: &ValidGossipConfig) {
        match &self.0 {
            State::Configured { name, .. } => log_known(name, ZoneSource::Config),
            State::Unconfigured if matches!(config.membership, Membership::Static { .. }) => {
                tracing::warn!(
                    code = "zone_unknown",
                    zone_source = "none",
                    reason = UnknownZone::NotConfigured.as_str(),
                    membership = config.membership.name(),
                    config_key = Setting::GossipZone.yaml_key(),
                    "{ONE_ZONE} This node uses a static peer list, so the zone setting is the \
                     only place it can learn its zone. Set {} to this node's zone, for example \
                     the cloud's zone ID; the gabion README lists commands that read it from \
                     cloud metadata. {SINGLE_ZONE}",
                    Setting::GossipZone,
                );
            }
            State::Unconfigured | State::Learned(_) | State::Awaiting { .. } => {}
        }
    }

    /// A peer event arrived at `now`: the first one starts the wait for the
    /// own entry.
    pub(super) fn note_peer_event(&mut self, now: u64, config: &ValidGossipConfig) {
        if let State::Awaiting { warning, .. } = &mut self.0
            && *warning == Warning::Unarmed
        {
            *warning = Warning::Due(now.saturating_add(config.zone_grace.0.get()));
        }
    }

    /// Discovery reported the own entry: its zone is adopted only while none
    /// is known, and returned for the peer table; one that differs from the
    /// setting is reported once. A zoneless entry never retracts a known zone.
    pub(super) fn own_entry(
        &mut self,
        zone: Option<ZoneName>,
        node_name: Option<Box<str>>,
    ) -> Option<LearnedZone> {
        match (&mut self.0, zone) {
            (State::Awaiting { .. } | State::Unconfigured, Some(name)) => {
                log_known(&name, ZoneSource::EndpointSlice);
                let learned = LearnedZone(name.hash());
                self.0 = State::Learned(name);
                Some(learned)
            }
            (State::Awaiting { entry, .. }, None) => {
                *entry = Entry::NoZone { node_name };
                None
            }
            (
                State::Configured {
                    name,
                    conflict_logged,
                },
                Some(found),
            ) if found.as_str() != name.as_str() && !*conflict_logged => {
                *conflict_logged = true;
                tracing::warn!(
                    code = "zone_conflict",
                    configured_zone = name.as_str(),
                    discovered_zone = found.as_str(),
                    zone_source = ZoneSource::Config.as_str(),
                    config_key = Setting::GossipZone.yaml_key(),
                    "The zone setting says this node is in zone `{configured}`, but its \
                     Kubernetes EndpointSlice entry says `{discovered}`. gabion uses \
                     `{configured}`, because the setting takes precedence, and every peer sees \
                     that value, so counts stay correct. If the node really runs in \
                     `{discovered}`, its traffic is grouped with the wrong zone and the \
                     cross-zone savings for this node are lost. Correct {}, or remove it to use \
                     the Kubernetes value.",
                    Setting::GossipZone,
                    configured = name.as_str(),
                    discovered = found.as_str(),
                );
                None
            }
            (State::Learned(name), Some(found)) if found.as_str() != name.as_str() => {
                tracing::debug!(
                    zone = name.as_str(),
                    discovered_zone = found.as_str(),
                    "This node's EndpointSlice entry now names another zone; it keeps the zone it \
                     learned first.",
                );
                None
            }
            _ => None,
        }
    }

    /// A heartbeat at `now`: once the wait for the own entry has passed with
    /// no zone, say why.
    pub(super) fn check(&mut self, now: u64, config: &ValidGossipConfig) {
        let State::Awaiting { entry, warning } = &mut self.0 else {
            return;
        };
        match *warning {
            Warning::Due(at) if at <= now => *warning = Warning::Done,
            Warning::Unarmed | Warning::Due(_) | Warning::Done => return,
        }
        let grace_ms = config.zone_grace.0.get();
        let reason = entry.reason().as_str();
        match entry {
            Entry::NoZone { node_name } => {
                let node = node_name.as_deref().unwrap_or("<node>");
                tracing::warn!(
                    code = "zone_unknown",
                    zone_source = "none",
                    reason,
                    membership = config.membership.name(),
                    node_name = node_name.as_deref(),
                    grace_ms,
                    config_key = Setting::GossipZone.yaml_key(),
                    "{ONE_ZONE} This pod's EndpointSlice entry has no zone. Kubernetes copies it \
                     from the `topology.kubernetes.io/zone` label of the node the pod runs on \
                     (`{node}`), and that node has no such label. Label the node (`kubectl label \
                     node {node} topology.kubernetes.io/zone=<zone>`) and restart this pod, or \
                     set {}. {SINGLE_ZONE}",
                    Setting::GossipZone,
                );
            }
            Entry::NotFound => {
                let self_match = match &config.membership {
                    Membership::Kubernetes { discovery, .. } => discovery.self_match.as_ref(),
                    Membership::Static { .. } | Membership::Alone => None,
                };
                let (hostname, self_addr, host_name_text) = match self_match {
                    Some(SelfMatch::PodName(name)) => (Some(&**name), None, &**name),
                    Some(SelfMatch::Addr(addr)) => (
                        None,
                        Some(addr),
                        "not read, since discovery.self_addr is set",
                    ),
                    None => (None, None, "could not be read, so set discovery.self_addr"),
                };
                tracing::warn!(
                    code = "zone_unknown",
                    zone_source = "none",
                    reason,
                    membership = config.membership.name(),
                    self_match = self_match.map_or("none", SelfMatch::name),
                    hostname,
                    self_addr = self_addr.map(tracing::field::display),
                    grace_ms,
                    config_key = Setting::GossipZone.yaml_key(),
                    "{ONE_ZONE} Kubernetes discovery is running, but after {grace_ms} ms none of \
                     the EndpointSlice entries it watches belongs to this pod. gabion looks for \
                     the entry whose pod name is this host's name (`{}`), or whose address is \
                     `discovery.self_addr` when that is set. The usual cause is a pod whose host \
                     name differs from its pod name (`hostNetwork: true`, or `spec.hostname` \
                     set), or a pod outside the watched Services. Set {} to this pod's IP and \
                     gossip port, or set {} directly. If this cluster only writes EndpointSlices \
                     slowly, raise {} above the time the entry takes to appear. {SINGLE_ZONE}",
                    host_name_text,
                    Setting::DiscoverySelfAddr,
                    Setting::GossipZone,
                    Setting::GossipZoneGrace,
                );
            }
        }
    }
}

impl Entry {
    fn reason(&self) -> UnknownZone {
        match self {
            Self::NotFound => UnknownZone::OwnEntryNotFound,
            Self::NoZone { .. } => UnknownZone::OwnEntryHasNoZone,
        }
    }
}

/// `zone_known`: the zone is known, from `source`.
fn log_known(name: &ZoneName, source: ZoneSource) {
    let from = match source {
        ZoneSource::Config => "the zone setting",
        ZoneSource::EndpointSlice => "its Kubernetes EndpointSlice entry",
    };
    tracing::info!(
        code = "zone_known",
        zone = name.as_str(),
        zone_hash = %name.hash(),
        zone_source = source.as_str(),
        "This node is in zone `{zone}`, read from {from}. It sends each change directly to peers \
         in this zone and through one gateway in each other zone.",
        zone = name.as_str(),
    );
}
