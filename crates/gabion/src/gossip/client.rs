//! Client-side handle and the request enum the runtime drains.

use std::marker::PhantomData;

use tokio::sync::{mpsc, oneshot};

use crate::crdt::{BucketEpoch, Count, KeyHash};

use super::GossipError;

/// Send-side handle to the gossip runtime. Cheap to clone; safe to share
/// across threads even though the runtime itself is single-threaded — the
/// mpsc channel is the boundary.
///
/// `C` is carried as a marker so the client and the [`super::GossipRuntime`]
/// it was created from share the same count width by construction. The
/// channel itself transports plain `u64` hit deltas and the runtime does
/// the saturating cast at the boundary.
#[derive(Debug)]
pub struct GossipClient<C: Count> {
    requests: mpsc::Sender<Request>,
    _marker: PhantomData<fn() -> C>,
}

impl<C: Count> Clone for GossipClient<C> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            _marker: PhantomData,
        }
    }
}

impl<C: Count> GossipClient<C> {
    pub(super) fn new(requests: mpsc::Sender<Request>) -> Self {
        Self {
            requests,
            _marker: PhantomData,
        }
    }

    /// Record one or more hits against the local origin. Returns when the
    /// runtime has consumed the request, applied the delta to the CRDT,
    /// and flushed the resulting rows through [`super::AggregateStore::apply`]
    /// — at that point the caller's own store handle reflects the increment.
    pub async fn record(
        &self,
        rule_fingerprint: u128,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        hits: u64,
        now_millis: u64,
    ) -> Result<(), GossipError> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send(Request::Limit(LimitRequest {
                rule_fingerprint,
                key_hash,
                bucket,
                hits,
                now_millis,
                reply: tx,
            }))
            .await
            .map_err(|_| GossipError::RuntimeShutDown)?;
        rx.await.map_err(|_| GossipError::RuntimeShutDown)
    }

    /// Ask the runtime to stop after draining whatever is already queued.
    pub async fn shutdown(&self) -> Result<(), GossipError> {
        self.requests
            .send(Request::Shutdown)
            .await
            .map_err(|_| GossipError::RuntimeShutDown)
    }
}

pub(super) enum Request {
    Limit(LimitRequest),
    Shutdown,
}

impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit(_) => f.write_str("Limit(..)"),
            Self::Shutdown => f.write_str("Shutdown"),
        }
    }
}

pub(super) struct LimitRequest {
    pub rule_fingerprint: u128,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
    pub hits: u64,
    pub now_millis: u64,
    pub reply: oneshot::Sender<()>,
}
