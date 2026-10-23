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
    ///
    /// `rule_limit` is the matched rule's cluster-wide budget; the runtime
    /// uses it to size the per-rule threshold-trigger error budget. Carried
    /// per-request (rather than registered separately) so the library stays
    /// oblivious to rule-reload protocols — the freshest limit always rides
    /// with the hit.
    pub async fn record(
        &self,
        rule_fingerprint: u128,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        hits: u64,
        rule_limit: u64,
        now_millis: u64,
    ) -> Result<(), GossipError> {
        let permit = self
            .requests
            .reserve()
            .await
            .map_err(|_| GossipError::RuntimeShutDown)?;
        let (reply, applied) = oneshot::channel();
        RecordPermit {
            permit,
            reply: Some(reply),
        }
        .record(
            rule_fingerprint,
            key_hash,
            bucket,
            hits,
            rule_limit,
            now_millis,
        );
        applied.await.map_err(|_| GossipError::RuntimeShutDown)
    }

    /// Wait until the runtime's request queue has room for `n` hits, so a
    /// caller can leave its hits where they are until the runtime can take
    /// them. `n` must not exceed [`super::GossipConfig::limit_queue_capacity`].
    pub async fn reserve(
        &self,
        n: usize,
    ) -> Result<impl Iterator<Item = RecordPermit<'_>>, GossipError> {
        debug_assert!(n <= self.requests.max_capacity());
        let permits = self
            .requests
            .reserve_many(n)
            .await
            .map_err(|_| GossipError::RuntimeShutDown)?;
        Ok(permits.map(|permit| RecordPermit {
            permit,
            reply: None,
        }))
    }

    /// Ask the runtime to stop after draining whatever is already queued.
    pub async fn shutdown(&self) -> Result<(), GossipError> {
        self.requests
            .send(Request::Shutdown)
            .await
            .map_err(|_| GossipError::RuntimeShutDown)
    }
}

/// A slot in the runtime's request queue, from [`GossipClient::reserve`].
#[must_use = "dropping a permit releases its slot without recording a hit"]
pub struct RecordPermit<'a> {
    permit: mpsc::Permit<'a, Request>,
    reply: Option<oneshot::Sender<()>>,
}

impl RecordPermit<'_> {
    /// Hand one hit to the runtime without waiting for it to be applied, and
    /// without the reply channel [`GossipClient::record`] allocates.
    pub fn record(
        self,
        rule_fingerprint: u128,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        hits: u64,
        rule_limit: u64,
        now_millis: u64,
    ) {
        self.permit.send(Request::Limit(LimitRequest {
            rule_fingerprint,
            key_hash,
            bucket,
            hits,
            rule_limit,
            now_millis,
            reply: self.reply,
        }));
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
    pub rule_limit: u64,
    pub now_millis: u64,
    pub reply: Option<oneshot::Sender<()>>,
}
