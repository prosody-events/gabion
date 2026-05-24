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
//! # Runtime on wasm32 (plan risk R2 — resolved)
//!
//! `spawn_local` composes fine under the browser/node microtask queue; the one
//! thing that could not run on `wasm32-unknown-unknown` was tokio's *time*
//! machinery — `tokio::time::clock::now` calls `std::time::Instant::now`,
//! which is unimplemented on this target and panics. So neither
//! `GossipRuntime`'s `tokio::time::interval` tick nor `tokio::time::advance`
//! works here.
//!
//! The fix made the tick source injectable through the existing
//! [`gabion::gossip::Clock`] trait (it gained an associated
//! [`gabion::gossip::Ticker`]): production keeps a `tokio::time::interval`
//! ticker (byte-identical), while [`crate::clock::ManualClock`] drives ticks
//! and the bucket clock from engine-owned virtual time, touching no
//! `tokio::time`. A node smoke test against the real wasm artifact now runs
//! the full submit → gossip → converge → snapshot loop (see `smoke.cjs`). The
//! code path has no browser-specific dependency — node and the browser share
//! the same single-threaded wasm executor — but an in-browser check is still
//! worth doing before the frontend phase relies on it.

use tokio::sync::{mpsc, oneshot};
use wasm_bindgen::prelude::*;

use crate::config::SimConfig;
use crate::engine::{Command, LinkPolicyKind, run_engine};

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
        config
            .validate()
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        // The engine drives virtual time itself through `ManualClock` (see the
        // module-level R2 note), so it needs no tokio time driver — `spawn_local`
        // is all the executor it requires. The node smoke test confirms the
        // spawned task runs the full submit → gossip → snapshot loop on the wasm
        // microtask queue.
        wasm_bindgen_futures::spawn_local(async move {
            let _ = run_engine(config, cmd_rx).await;
        });
        Ok(Sim { cmd_tx })
    }

    /// Inject `hits` requests for `key_id` at `node`, at the current virtual
    /// time. Resolves to the [`crate::event::EventBatch`] the injection
    /// produced (the cell create/update events plus the reached virtual time).
    pub async fn submit_request(
        &self,
        node: u32,
        key_id: u64,
        hits: u64,
    ) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::SubmitRequest {
            node,
            key: key_id as u128,
            hits,
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

    /// Restore every directed link to lossless `Pass` — undo any partition or
    /// per-link drop policy across the whole cluster. Engine-driven: it sweeps
    /// the *live* link set in one round-trip, so it stays correct after nodes
    /// have joined or left (the caller no longer tracks the node count).
    pub async fn heal(&self) -> Result<(), JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Heal { reply }).await?;
        rx.await.map_err(|_| engine_gone())
    }

    /// Add a fresh cold-start node to the live cluster. It joins by gossip and
    /// catches up by anti-entropy — no rebuild. Resolves to the
    /// [`crate::event::EventBatch`] the join produced; the new node's stable id
    /// appears in the next [`Sim::snapshot`].
    pub async fn add_node(&self) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::AddNode { reply }).await?;
        let batch = rx.await.map_err(|_| engine_gone())?;
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

    /// Advance virtual time by `delta_ms`, resolving to the events produced.
    pub async fn step(&self, delta_ms: u64) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Step { delta_ms, reply }).await?;
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

    /// Pull the full per-node cluster state for a seek / re-render.
    pub async fn snapshot(&self) -> Result<JsValue, JsValue> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Snapshot { reply }).await?;
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

    /// One `SetLinkPolicy` round-trip, shared by `set_link_policy`,
    /// `partition`, and `heal`.
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

/// Serialize an owned reply into a plain JS object.
fn to_js<T: serde::Serialize>(value: &T) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(Into::into)
}

/// The error every method returns once the engine task is gone.
fn engine_gone() -> JsValue {
    JsValue::from_str(
        "the simulation engine has stopped, so this command could not run. \
         Reload the page to start a new session.",
    )
}
