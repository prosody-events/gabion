//! Downstream count store contract.
//!
//! The gossip runtime hands every CRDT-touching event-loop iteration's
//! [`DeltaSink`] and [`ExpirationSink`] to a caller-supplied store via
//! [`AggregateStore::apply`]. Production backends (`Arc<DashMapStore>`,
//! `Arc<ShmStore>`) plug in via this trait; tests use the in-memory backing
//! store defined in `gossip::tests`.

use crate::crdt::{Count, DeltaSink, ExpirationSink};

/// Downstream count store. The runtime calls `apply` once per event-loop
/// iteration with whatever rows the arm produced (each sink may be empty —
/// the caller-side short-circuit decides whether to read them).
///
/// `&self` so cloneable handles (`Arc<DashMapStore>`, `Arc<ShmStore>`) can be
/// shared between the runtime (writer) and the read path. Backends use
/// interior mutability — DashMap, atomics, or `Mutex`.
///
/// Apply order contract: implementations should fold deltas first, then
/// expirations. The two arms producing rows today cannot produce both in the
/// same iteration, but the contract makes the row-source order explicit for
/// future cross-batching.
///
/// `apply` is synchronous and non-blocking by contract — backends must use
/// lock-free or fine-grained-locked structures so the gossip runtime is not
/// stalled by a coarse lock acquisition.
pub trait AggregateStore<C: Count> {
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>);
}

/// `Arc<T>` forwards to its inner store. Lets downstream code share one
/// `Arc<DashMapStore>` handle between the runtime and the read path without
/// any wrapper boilerplate.
impl<C: Count, T: AggregateStore<C> + ?Sized> AggregateStore<C> for std::sync::Arc<T> {
    #[inline]
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        T::apply(self, deltas, expirations)
    }
}

/// `Rc<T>` forwards too, for single-threaded sim setups.
impl<C: Count, T: AggregateStore<C> + ?Sized> AggregateStore<C> for std::rc::Rc<T> {
    #[inline]
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        T::apply(self, deltas, expirations)
    }
}
