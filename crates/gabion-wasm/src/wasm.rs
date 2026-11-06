//! The `#[wasm_bindgen]` surface: a thin [`Sim`] handle the TypeScript
//! frontend holds. Compiled **only** for `wasm32`, so native builds and
//! `cargo nextest` never see it — the engine and shims stay testable without
//! any wasm toolchain.
//!
//! Every method is a one-line bridge: build a [`Command`], send it down the
//! channel to the single engine task spawned in [`Sim::new`], await the
//! `oneshot` reply, and hand the result back to JS as a plain object via
//! `serde-wasm-bindgen`. `u128` keys/fingerprints render as hex strings on the
//! way out (see [`crate::hex`]); request keys come in as small dense `u64`
//! ids, widened to `u128`.
//!
//! # Runtime on wasm32
//!
//! `spawn_local` runs on the browser's and node's microtask queue, but
//! tokio's time machinery cannot: `tokio::time::clock::now` calls
//! `std::time::Instant::now`, which panics on `wasm32-unknown-unknown`. So
//! the tick source comes from the [`gabion::gossip::Clock`] trait, whose
//! associated [`gabion::gossip::Ticker`] production fills with a
//! `tokio::time::interval`, while [`crate::clock::ManualClock`] drives ticks
//! and the bucket clock from the engine's virtual time, touching no
//! `tokio::time`. `smoke.cjs` runs the full submit, gossip, converge and
//! snapshot loop against the real wasm artifact; node and the browser share
//! the same single-threaded wasm executor.

use std::num::NonZeroU64;

use gabion::crdt::KeyHash;
use tokio::sync::{mpsc, oneshot};
use wasm_bindgen::prelude::*;

use crate::config::{MAX_ZONES, SimConfig};
use crate::engine::{Command, EngineError, LinkPolicyKind, StepUntil, Watch, run_engine};

/// A live simulation the page drives. Holds the sender end of the engine's
/// command channel; every method round-trips one command and awaits its reply.
#[wasm_bindgen]
pub struct Sim {
    cmd_tx: mpsc::Sender<Command>,
}

#[wasm_bindgen]
impl Sim {
    /// Build the cluster and spawn its engine. `config` is a [`SimConfig`] as a
    /// plain JS object; an invalid config **throws synchronously** so a typo in
    /// a shared URL surfaces immediately instead of producing a quietly
    /// different run.
    #[wasm_bindgen(constructor)]
    pub fn new(config: JsValue) -> Result<Sim, JsValue> {
        let config: SimConfig = serde_wasm_bindgen::from_value(config)?;
        let valid = config
            .validate()
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        // The engine drives virtual time itself through `ManualClock` (see the
        // module-level R2 note), so it needs no tokio time driver —
        // `spawn_local` is all the executor it requires. The node smoke
        // test confirms the spawned task runs the full submit → gossip
        // → snapshot loop on the wasm microtask queue.
        wasm_bindgen_futures::spawn_local(async move {
            let _ = run_engine(valid, cmd_rx).await;
        });
        Ok(Sim { cmd_tx })
    }

    /// A request of `hits` for `key_id` at `node`, at the current virtual
    /// time. Resolves to the [`crate::event::EventBatch`] it produced: the
    /// node's decision first, then, when admitted, its cell events and any
    /// eager-flush packets, which land at the next command. Throws for zero
    /// hits.
    pub async fn submit_request(
        &self,
        node: u32,
        key_id: u64,
        hits: u64,
    ) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::SubmitRequest {
            node,
            key: KeyHash(u128::from(key_id)),
            hits: nonzero_hits(hits)?,
            reply,
        })
        .await?;
        let batch = rx
            .await
            .map_err(|_| engine_gone())?
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        to_js(&batch)
    }

    /// Install a directed link policy between two nodes. `policy` is a
    /// [`LinkPolicyKind`] object, e.g. `{ "kind": "Block" }` or
    /// `{ "kind": "DropProb", "p": 0.1 }`.
    pub async fn set_link_policy(
        &self,
        src: u32,
        dst: u32,
        policy: JsValue,
    ) -> Result<(), JsValue> {
        let policy: LinkPolicyKind = serde_wasm_bindgen::from_value(policy)?;
        self.apply_link(src, dst, policy).await
    }

    /// Cut every link between the two node groups, both directions — a clean
    /// network partition. Convenience over repeated [`Sim::set_link_policy`].
    pub async fn partition(&self, group_a: JsValue, group_b: JsValue) -> Result<(), JsValue> {
        let group_a: Vec<u32> = serde_wasm_bindgen::from_value(group_a)?;
        let group_b: Vec<u32> = serde_wasm_bindgen::from_value(group_b)?;
        for &a in &group_a {
            for &b in &group_b {
                self.apply_link(a, b, LinkPolicyKind::Block).await?;
                self.apply_link(b, a, LinkPolicyKind::Block).await?;
            }
        }
        Ok(())
    }

    /// Restore the configured network, uniform loss included, on every
    /// directed link — undo any partition or per-link policy across the whole
    /// cluster. Engine-driven: it sweeps the *live* link set in one
    /// round-trip, so it stays correct after nodes have joined or left.
    pub async fn heal(&self) -> Result<(), JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Heal { reply }).await?;
        rx.await.map_err(|_| engine_gone())
    }

    /// Add a fresh cold-start node to the live cluster. It joins by gossip and
    /// catches up by anti-entropy — no rebuild. Resolves to the
    /// [`crate::event::EventBatch`] the join produced; the new node's stable id
    /// appears in the next [`Sim::snapshot`]. It joins zone `zone`, or the zone
    /// with the fewest live members; an index past the last zone throws
    /// [`crate::engine::EngineError::UnknownZone`].
    pub async fn add_node(&self, zone: Option<u32>) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::AddNode { zone, reply }).await?;
        let batch = rx
            .await
            .map_err(|_| engine_gone())?
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        to_js(&batch)
    }

    /// Remove the live node with stable `id`. Survivors keep their own ids and
    /// re-converge; the removed id leaves a gap and is never reused. Resolves
    /// to the [`crate::event::EventBatch`] the removal produced, or throws
    /// [`crate::engine::EngineError::UnknownNode`] if no live node has that id.
    pub async fn remove_node(&self, id: u32) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::RemoveNode { id, reply }).await?;
        let batch = rx
            .await
            .map_err(|_| engine_gone())?
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        to_js(&batch)
    }

    /// Advance virtual time by at most `delta_ms`, stopping where `until`
    /// (`"Time"` or `{ Event: { key } }`, a [`StepUntil`]) says, resolving to
    /// the events produced.
    pub async fn step(&self, delta_ms: u64, until: JsValue) -> Result<JsValue, JsValue> {
        let until: StepUntil = serde_wasm_bindgen::from_value(until)?;
        let (reply, rx) = oneshot::channel();
        self.send(Command::Step {
            delta_ms,
            until,
            reply,
        })
        .await?;
        let batch = rx.await.map_err(|_| engine_gone())?;
        to_js(&batch)
    }

    /// Advance virtual time to absolute `virtual_ms` (a no-op if already past).
    pub async fn step_to(&self, virtual_ms: u64) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::StepTo { virtual_ms, reply }).await?;
        let batch = rx.await.map_err(|_| engine_gone())?;
        to_js(&batch)
    }

    /// Pull every node's state of key `key_id` for a re-render, with the
    /// cells and peer rows of the node with stable id `inspect` alone, and
    /// that node's decision on a request of `probe_hits`. Throws for zero
    /// hits.
    pub async fn snapshot(
        &self,
        inspect: Option<u32>,
        key_id: u64,
        probe_hits: u64,
    ) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        let watch = Watch {
            key: KeyHash(u128::from(key_id)),
            probe_hits: nonzero_hits(probe_hits)?,
        };
        self.send(Command::Snapshot {
            inspect,
            watch,
            reply,
        })
        .await?;
        let state = rx.await.map_err(|_| engine_gone())?;
        to_js(&state)
    }

    /// End the session and shut every runtime down. The handle is inert after
    /// this; build a fresh [`Sim`] to start over.
    pub async fn shutdown(&self) -> Result<(), JsValue> {
        // Best-effort: if the engine has already stopped, there is nothing to
        // tear down, so a closed channel is success, not an error.
        let _ = self.cmd_tx.send(Command::Shutdown).await;
        Ok(())
    }
}

impl Sim {
    /// Send one command, mapping a closed channel to a clear operator message.
    async fn send(&self, cmd: Command) -> Result<(), JsValue> {
        self.cmd_tx.send(cmd).await.map_err(|_| engine_gone())
    }

    /// One `SetLinkPolicy` round-trip, shared by `set_link_policy` and
    /// `partition`.
    async fn apply_link(&self, src: u32, dst: u32, policy: LinkPolicyKind) -> Result<(), JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::SetLinkPolicy {
            src,
            dst,
            policy,
            reply,
        })
        .await?;
        rx.await
            .map_err(|_| engine_gone())?
            .map_err(|err| JsValue::from_str(&err.to_string()))
    }
}

/// The production-aligned [`SimConfig`] defaults as a plain JS object. The
/// single source the frontend reads, so its control sliders open at exactly the
/// values an omitted field would take — no hand-maintained TypeScript mirror to
/// drift from `gabion::defaults`. `u128` fields cross as hex strings, like
/// every other boundary payload.
#[wasm_bindgen]
pub fn default_config() -> Result<JsValue, JsValue> {
    to_js(&SimConfig::default())
}

/// The most zones a session places nodes in: the zone stepper's ceiling.
#[wasm_bindgen]
pub fn max_zones() -> u32 {
    MAX_ZONES as u32
}

/// A request's weight as the engine takes it: zero hits throws, as
/// [`EngineError::NoHits`].
fn nonzero_hits(hits: u64) -> Result<NonZeroU64, JsValue> {
    NonZeroU64::new(hits).ok_or_else(|| JsValue::from_str(&EngineError::NoHits.to_string()))
}

/// Serialize an owned reply into a plain JS object.
fn to_js<T: serde::Serialize>(value: &T) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(Into::into)
}

/// The error every method returns once the engine task is gone.
fn engine_gone() -> JsValue {
    JsValue::from_str(
        "the simulation engine has stopped, so this command could not run. Reload the page to \
         start a new session.",
    )
}
