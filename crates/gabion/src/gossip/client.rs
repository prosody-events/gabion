//! Client-side handle and the request enum the runtime drains.

use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{BucketEpoch, KeyHash};

use super::GossipError;

/// Send-side handle to the gossip runtime. Cheap to clone; safe to share
/// across threads even though the runtime itself is single-threaded — the
/// mpsc channel is the boundary.
#[derive(Clone, Debug)]
pub struct GossipClient {
    requests: mpsc::Sender<Request>,
}

impl GossipClient {
    pub(super) fn new(requests: mpsc::Sender<Request>) -> Self {
        Self { requests }
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

    /// Hand one hit to the runtime without waiting: no reply channel, and a
    /// full queue refuses the hit rather than block the caller.
    pub fn try_record(
        &self,
        rule_fingerprint: u128,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        hits: u64,
        rule_limit: u64,
        now_millis: u64,
    ) -> Result<(), TryRecordError> {
        let permit = self.requests.try_reserve().map_err(|error| match error {
            TrySendError::Full(()) => TryRecordError::Full,
            TrySendError::Closed(()) => TryRecordError::ShutDown,
        })?;
        RecordPermit {
            permit,
            reply: None,
        }
        .record(
            rule_fingerprint,
            key_hash,
            bucket,
            hits,
            rule_limit,
            now_millis,
        );
        Ok(())
    }

    /// The request queue's depth, `gossip.limit_queue_capacity`.
    pub fn capacity(&self) -> usize {
        self.requests.max_capacity()
    }

    /// Wait until the runtime's request queue has room for `n` hits, so a
    /// caller can leave its hits where they are until the runtime can take
    /// them. `n` must not exceed the queue's capacity,
    /// `gossip.limit_queue_capacity`
    /// or [`super::ValidGossipConfig::with_shallow_limit_queue`]'s depth.
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

/// Why [`GossipClient::try_record`] refused a hit.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TryRecordError {
    #[error("the gossip request queue is full")]
    Full,
    #[error("gossip runtime is no longer running")]
    ShutDown,
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
