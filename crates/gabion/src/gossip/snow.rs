//! Snow broadcast trees (Tong, "Snow: Self-organizing Broadcast Protocol for
//! Cloud", arXiv 2504.02676): the pure parts.
//!
//! * [`ring`] — the membership view and the child function: given the local
//!   view, a root and a carried range, which peers does this node forward to.
//! * [`ctrl`] — the control extension each tree packet carries.
//! * [`flight`] — bounded tables of broadcasts in flight and the round-trip
//!   estimator behind the acknowledgement timeout.
//!
//! The runtime half lives in `runtime/tree.rs`. See "Design notes" in the
//! gabion README for how this follows the paper and where it adapts it.

use crate::defaults;

mod ctrl;
mod flight;
mod ring;

#[cfg(test)]
mod tests;

pub(super) use ctrl::{CTRL_MAX_LEN, Ctrl};
pub(super) use flight::{MAX_CHILDREN, Origins, Relay, Relays, RttEstimator, full_mask};
pub(super) use ring::{Child, Children, Ring, tree_height};

/// The Snow tuning knobs, fixed at construction. Defaults are the paper's;
/// see [`crate::defaults`] for the reasoning behind each.
#[derive(Clone, Copy, Debug)]
pub(super) struct TreeParams {
    /// Branching factor `k`.
    pub branching: usize,
    /// Rebroadcast after this many expected tree round trips.
    pub ack_timeout_rtt_multiple: u32,
    /// Rebroadcasts a root attempts before leaving a broadcast to repair.
    pub rebroadcasts: u8,
    /// Cells per heartbeat in the repair pass.
    pub repair_cells_per_tick: usize,
}

impl TreeParams {
    /// Parameters for branching factor `fanout` (clamped to what the ack
    /// masks can hold) and the shipped defaults for the rest.
    pub fn new(fanout: usize) -> Self {
        Self {
            branching: fanout.clamp(2, MAX_CHILDREN),
            ack_timeout_rtt_multiple: defaults::GOSSIP_ACK_TIMEOUT_RTT_MULTIPLE,
            rebroadcasts: defaults::GOSSIP_ROOT_REBROADCASTS,
            repair_cells_per_tick: defaults::GOSSIP_REPAIR_CELLS_PER_TICK,
        }
    }
}
