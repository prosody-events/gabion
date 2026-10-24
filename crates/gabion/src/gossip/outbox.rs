//! What a node still owes the cluster: local cells an owner has not
//! acknowledged, and owned cells that changed since the last broadcast.
//!
//! Both are bounded FIFO queues of cell handles, each cell queued at most
//! once, with per-cell state in a column indexed by store slot. Entries for
//! cells that have since expired are recognised by their stale generation
//! and dropped when next visited.
//!
//! A bucket's own and others cells may both be queued for one broadcast, so
//! a round of broadcast marks the cell that reports each bucket it gathers,
//! and a bucket already in the round is skipped rather than gathered twice.

use std::collections::VecDeque;

use crate::crdt::{CellHandle, Count};

use super::membership::Token;

/// Per-cell delivery state, indexed by the cell's store slot.
#[derive(Clone, Copy)]
struct CellState<C: Count> {
    /// The count of this cell that `owner` holds (for a cell this node owns,
    /// the count last queued for broadcast). Zero means no owner has it.
    delivered: C,
    owner: Token,
    /// Generation of the handle currently queued for sync, or 0. A freed and
    /// reused slot carries a new generation, so the entry a dead cell left
    /// in the queue never blocks the slot's next cell from queueing.
    queued_sync: u32,
    queued_broadcast: u32,
    /// The broadcast round that last gathered this cell, or 0.
    broadcast_round: u32,
}

impl<C: Count> CellState<C> {
    fn new() -> Self {
        Self {
            delivered: C::default(),
            owner: Token::default(),
            queued_sync: 0,
            queued_broadcast: 0,
            broadcast_round: 0,
        }
    }
}

/// A queue was full, so the cell was not queued.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct QueueFull;

pub(super) struct Outbox<C: Count> {
    cells: Box<[CellState<C>]>,
    sync: VecDeque<CellHandle>,
    broadcast: VecDeque<CellHandle>,
    round: u32,
}

impl<C: Count> Outbox<C> {
    pub(super) fn new(
        cell_capacity: usize,
        sync_capacity: usize,
        broadcast_capacity: usize,
    ) -> Self {
        Self {
            cells: vec![CellState::new(); cell_capacity].into_boxed_slice(),
            sync: VecDeque::with_capacity(sync_capacity),
            broadcast: VecDeque::with_capacity(broadcast_capacity),
            round: 0,
        }
    }

    pub(super) fn sync_len(&self) -> usize {
        self.sync.len()
    }

    pub(super) fn broadcast_len(&self) -> usize {
        self.broadcast.len()
    }

    /// Queue `handle` for sync unless it is already queued.
    pub(super) fn queue_sync(&mut self, handle: CellHandle) -> Result<(), QueueFull> {
        let state = &mut self.cells[handle.index as usize];
        if state.queued_sync == handle.generation {
            return Ok(());
        }
        if self.sync.len() == self.sync.capacity() {
            return Err(QueueFull);
        }
        state.queued_sync = handle.generation;
        self.sync.push_back(handle);
        Ok(())
    }

    /// Queue `handle` for the next broadcast unless it is already queued.
    pub(super) fn queue_broadcast(&mut self, handle: CellHandle) -> Result<(), QueueFull> {
        let state = &mut self.cells[handle.index as usize];
        if state.queued_broadcast == handle.generation {
            return Ok(());
        }
        if self.broadcast.len() == self.broadcast.capacity() {
            return Err(QueueFull);
        }
        state.queued_broadcast = handle.generation;
        self.broadcast.push_back(handle);
        Ok(())
    }

    /// The `i`-th queued sync entry.
    pub(super) fn sync_at(&self, i: usize) -> CellHandle {
        self.sync[i]
    }

    /// Drop the `i`-th sync entry (order of the rest is not preserved).
    pub(super) fn unqueue_sync(&mut self, i: usize) {
        if let Some(handle) = self.sync.swap_remove_back(i) {
            let state = &mut self.cells[handle.index as usize];
            if state.queued_sync == handle.generation {
                state.queued_sync = 0;
            }
        }
    }

    /// Take the oldest queued broadcast entry.
    pub(super) fn pop_broadcast(&mut self) -> Option<CellHandle> {
        let handle = self.broadcast.pop_front()?;
        let state = &mut self.cells[handle.index as usize];
        if state.queued_broadcast == handle.generation {
            state.queued_broadcast = 0;
        }
        Some(handle)
    }

    /// Start a broadcast round; cells marked in an earlier one no longer
    /// count as gathered.
    pub(super) fn begin_broadcast_round(&mut self) {
        // Round 0 means "never gathered", so a wrapped counter skips it.
        self.round = self.round.checked_add(1).unwrap_or(1);
    }

    /// Mark the cell as gathered by the current broadcast round.
    pub(super) fn mark_gathered(&mut self, handle: CellHandle) {
        self.cells[handle.index as usize].broadcast_round = self.round;
    }

    /// Whether the current broadcast round already gathered the cell.
    pub(super) fn is_gathered(&self, handle: CellHandle) -> bool {
        self.cells[handle.index as usize].broadcast_round == self.round
    }

    /// Record that `owner` holds `count` of the cell.
    pub(super) fn delivered(&mut self, handle: CellHandle, count: C, owner: Token) {
        let state = &mut self.cells[handle.index as usize];
        if state.owner == owner {
            state.delivered = state.delivered.max(count);
        } else {
            state.owner = owner;
            state.delivered = count;
        }
    }

    /// Whether `owner` is already known to hold at least `count` of the cell.
    pub(super) fn is_delivered(&self, handle: CellHandle, count: C, owner: Token) -> bool {
        let state = &self.cells[handle.index as usize];
        state.owner == owner && state.delivered >= count
    }

    /// The cell was freed: forget everything about its slot.
    pub(super) fn forget(&mut self, handle: CellHandle) {
        self.cells[handle.index as usize] = CellState::new();
    }
}

#[cfg(test)]
mod tests;
