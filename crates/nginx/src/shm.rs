//! Shared-memory layout for the nginx adapter.
//!
//! One `MAP_SHARED | MAP_ANONYMOUS` region is mmap'd by the nginx master in
//! the config phase; every worker inherits the mapping at fork. Every
//! cross-process structure lives in this module, communicates via atomics,
//! and uses `#[repr(C)]` so the offsets are stable across worker processes.

pub mod aggregate;
pub mod header;
pub mod lease;
#[cfg(test)]
pub(crate) mod mock;
pub mod queue;
pub mod stats;

pub use aggregate::{AggregateSlot, AggregateTable, ShmAggregateStore};
pub use header::{Header, NodeIdentityFields};
pub use lease::LeaderLease;
pub use queue::{QueueControl, QueueOverflow, QueueSlot, RequestQueue};
pub use stats::Stats;

/// Bytes of padding between regions; rounded up to the cacheline boundary.
pub const CACHELINE: usize = 64;

pub fn align_up(value: usize, align: usize) -> usize {
    let mask = align - 1;
    (value + mask) & !mask
}

/// Depth of the queue workers push hits onto for the leader to drain.
const QUEUE_CAPACITY: usize = 2048;

/// The fewest aggregate slots a zone may hold.
pub const MIN_AGGREGATE_CAPACITY: usize = 1024;

/// Lays out the SHM zone given capacity targets. Computed once per config
/// load, from the zone's size; nginx reuses a zone across reload only when
/// its size is unchanged, so a reused region keeps a matching layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Layout {
    pub queue_capacity: usize,
    pub aggregate_capacity: usize,

    pub header_offset: usize,
    pub lease_offset: usize,
    pub queue_control_offset: usize,
    pub queue_slots_offset: usize,
    pub aggregate_slots_offset: usize,
    pub stats_offset: usize,
    pub total_bytes: usize,
}

impl Layout {
    /// Compute the SHM layout. `queue_capacity` must be a power of two.
    /// `aggregate_capacity` must be a power of two and large enough to hold
    /// the expected `(rule, key, bucket)` working set.
    pub fn new(queue_capacity: usize, aggregate_capacity: usize) -> Option<Self> {
        if !queue_capacity.is_power_of_two() || queue_capacity < 2 {
            return None;
        }
        if !aggregate_capacity.is_power_of_two() || aggregate_capacity < 2 {
            return None;
        }

        let header_offset = 0;
        let lease_offset = align_up(header_offset + size_of::<Header>(), CACHELINE);
        let queue_control_offset = align_up(lease_offset + size_of::<LeaderLease>(), CACHELINE);
        let queue_slots_offset =
            align_up(queue_control_offset + size_of::<QueueControl>(), CACHELINE);
        let queue_slots_bytes = queue_capacity.checked_mul(size_of::<QueueSlot>())?;
        let aggregate_slots_offset = align_up(queue_slots_offset + queue_slots_bytes, CACHELINE);
        let aggregate_slots_bytes =
            aggregate_capacity.checked_mul(size_of::<aggregate::AggregateSlot>())?;
        let stats_offset = align_up(aggregate_slots_offset + aggregate_slots_bytes, CACHELINE);
        let total_bytes = align_up(stats_offset + size_of::<Stats>(), CACHELINE);

        Some(Self {
            queue_capacity,
            aggregate_capacity,
            header_offset,
            lease_offset,
            queue_control_offset,
            queue_slots_offset,
            aggregate_slots_offset,
            stats_offset,
            total_bytes,
        })
    }

    /// Lay out a zone of `zone_bytes` on `page_bytes` pages: the fixed
    /// queue plus the largest power-of-two aggregate table that fits, or
    /// `None` below `MIN_AGGREGATE_CAPACITY` slots. nginx's slab pool keeps
    /// a 24-byte descriptor per page (under 1/128 of the zone for pages of
    /// 4 KiB or more) and loses up to a page each to its header, to
    /// aligning its first page, and to rounding its page count down; our
    /// block is rounded up to whole pages, which can cost one more.
    pub fn for_zone(zone_bytes: usize, page_bytes: usize) -> Option<Self> {
        let usable = zone_bytes.checked_sub(zone_bytes / 128 + 4 * page_bytes)?;
        let mut capacity = 1 << (usable / size_of::<AggregateSlot>()).checked_ilog2()?;
        while capacity >= MIN_AGGREGATE_CAPACITY {
            let layout = Self::new(QUEUE_CAPACITY, capacity)?;
            if layout.total_bytes <= usable {
                return Some(layout);
            }
            capacity /= 2;
        }
        None
    }
}

/// Live view of an initialized SHM zone. Workers reconstruct one from the
/// `mmap`'d base pointer (which has the same value in every fork-child) and
/// then access the sub-regions via raw pointers. All cross-process state is
/// stored in atomics or seqlock-protected slots.
#[derive(Clone, Copy, Debug)]
pub struct ShmRegion {
    base: *mut u8,
    layout: Layout,
}

// SAFETY: `ShmRegion` is two POD fields (`*mut u8` + `Copy` `Layout`). The
// pointer addresses an nginx shared zone that the master maps before forking,
// so it is identical in every fork-child and meaningful across
// processes/threads. A worker's mapping lives until the worker exits; the
// master frees a zone only after a reload drops it, which leaves workers'
// mappings in place. Crucially, every byte inside the region is only
// accessed through atomics or seqlock-protected slots defined on the contained
// `#[repr(C)]` types (see `shm/header.rs`, `shm/lease.rs`, `shm/queue.rs`,
// `shm/aggregate.rs`, `shm/stats.rs`). No code ever materialises a `&mut T`
// into the mapping after `initialize` returns, so handing the wrapper to
// another thread (Send) or sharing `&ShmRegion` across threads (Sync) cannot
// introduce a data race. See the Nomicon chapter "Send and Sync".
unsafe impl Send for ShmRegion {}
// SAFETY: see the `Send` impl above. `&ShmRegion` only exposes accessors that
// hand out `&T` to `#[repr(C)]` types whose interior mutability is provided by
// atomics/seqlocks; concurrent `&` access from multiple threads/processes is
// therefore sound.
unsafe impl Sync for ShmRegion {}

impl ShmRegion {
    /// # Safety
    /// The caller must guarantee all of the following:
    /// * `base` is non-null and points at a fresh, writable mapping of at least
    ///   `layout.total_bytes` bytes. In practice this is a block of the nginx
    ///   shared zone the master maps before any worker is forked.
    /// * `base` is aligned at least to `CACHELINE` (64). The zone's slab
    ///   allocator returns page-aligned blocks, which satisfies the alignment
    ///   requirements of every `#[repr(C)]` sub-structure stored in the region.
    /// * `layout` is the value computed by `Layout::new`, and its offsets/sizes
    ///   describe the actual mapping.
    /// * The caller has *exclusive* access to the mapping for the duration of
    ///   this call (i.e. no worker is reading or writing it yet), so the
    ///   subsequent `write_bytes` and `ptr::write`s cannot race.
    /// * The mapping must outlive every copy of the returned `ShmRegion`.
    pub unsafe fn initialize(base: *mut u8, layout: Layout) -> Self {
        // SAFETY: The caller of this `unsafe fn` upholds the preconditions
        // above. Specifically:
        // * `write_bytes(base, 0, total_bytes)` is valid because `base` is
        //   non-null, writable, exclusive (no other reader), and the mapping
        //   covers `total_bytes` (caller precondition). Zero is a valid bit
        //   pattern for every field in the region (all stored types are
        //   `#[repr(C)]` composites of atomics / integers / arrays of bytes).
        // * Each `ptr::write(p, value)` overwrites a `#[repr(C)]` POD at the
        //   offset computed by `Layout::new`; `Layout::new` rounds every offset
        //   up to `CACHELINE`, which exceeds the alignment of every stored
        //   type, so the destination pointer is properly aligned. `Layout::new`
        //   also guarantees `offset + size_of::<T>() <= total_bytes`, so each
        //   write stays in-bounds of the one mapping (Nomicon: "Working with
        //   raw pointers" / `pointer::add` rules).
        // * The queue/aggregate loops only index `0..capacity`, matching the
        //   per-slot bounds checked by the private `*_slot_ptr` helpers.
        // * No `&mut T` to any region byte escapes this function, so the
        //   shared-`&T` accessors that run afterwards are sound (Nomicon:
        //   "References and Aliasing").
        unsafe {
            std::ptr::write_bytes(base, 0, layout.total_bytes);
            let region = Self { base, layout };

            std::ptr::write(region.header_ptr() as *mut Header, Header::default());
            std::ptr::write(
                region.lease_ptr() as *mut LeaderLease,
                LeaderLease::default(),
            );
            std::ptr::write(
                region.queue_control_ptr() as *mut QueueControl,
                QueueControl::new(layout.queue_capacity),
            );
            for i in 0..layout.queue_capacity {
                let slot = region.queue_slot_ptr(i) as *mut QueueSlot;
                std::ptr::write(slot, QueueSlot::empty(i as u64));
            }
            for i in 0..layout.aggregate_capacity {
                let slot = region.aggregate_slot_ptr(i) as *mut aggregate::AggregateSlot;
                std::ptr::write(slot, aggregate::AggregateSlot::empty());
            }
            std::ptr::write(region.stats_ptr() as *mut Stats, Stats::default());
            region
        }
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    pub fn header(&self) -> &Header {
        // SAFETY: The `ShmRegion` invariants (see `initialize` / `Send`+`Sync`
        // docs) guarantee that `header_ptr()` is non-null, cacheline-aligned
        // (hence aligned for `Header`), in-bounds of one allocation, and
        // points at an initialized `Header`. `Header` only exposes interior
        // mutability through atomics, so handing out a `&Header` cannot alias
        // any `&mut Header` (none exists after `initialize`). The borrow
        // borrows from `self`, so the mapping outlives it.
        unsafe { &*(self.header_ptr() as *const Header) }
    }

    pub fn lease(&self) -> &LeaderLease {
        // SAFETY: As for `header()` — `lease_ptr()` is aligned, in-bounds,
        // and points at an initialized `LeaderLease`; all interior mutation
        // is via atomics, so a shared `&LeaderLease` cannot race with any
        // `&mut LeaderLease` (none ever exists post-init).
        unsafe { &*(self.lease_ptr() as *const LeaderLease) }
    }

    pub fn queue(&self) -> RequestQueue<'_> {
        // SAFETY:
        // * `queue_control_ptr()` is aligned and in-bounds; `QueueControl` was
        //   initialized by `initialize` and contains only atomics, so the
        //   shared reference is sound (see `header()` reasoning).
        // * `slice::from_raw_parts`: `queue_slot_ptr(0)` is the start of a
        //   contiguous `queue_capacity * size_of::<QueueSlot>()` byte range
        //   that `Layout::new` reserves inside `total_bytes` (so the slice fits
        //   in one allocation), every slot was initialized in `initialize`, and
        //   `QueueSlot` is `#[repr(C)]` with atomic-only interior mutability.
        //   `queue_capacity` fits in `isize` because the master successfully
        //   mmaps `total_bytes`. See Nomicon: "`std::slice::from_raw_parts`"
        //   requirements.
        unsafe {
            let control = &*(self.queue_control_ptr() as *const QueueControl);
            let slots = std::slice::from_raw_parts(
                self.queue_slot_ptr(0) as *const QueueSlot,
                self.layout.queue_capacity,
            );
            RequestQueue::from_parts(control, slots)
        }
    }

    pub fn aggregate(&self) -> AggregateTable<'_> {
        // SAFETY: Same reasoning as the slice construction in `queue()`:
        // `aggregate_slot_ptr(0)` is aligned and in-bounds, the
        // `aggregate_capacity * size_of::<AggregateSlot>()` byte range fits
        // within `total_bytes` (per `Layout::new`), every slot was
        // initialized by `initialize`, and `AggregateSlot` exposes interior
        // mutability only through atomics / seqlocks. Nomicon:
        // "`std::slice::from_raw_parts`".
        unsafe {
            let slots = std::slice::from_raw_parts(
                self.aggregate_slot_ptr(0) as *const AggregateSlot,
                self.layout.aggregate_capacity,
            );
            AggregateTable::from_slots(slots)
        }
    }

    pub fn stats(&self) -> &Stats {
        // SAFETY: As for `header()` / `lease()` — `stats_ptr()` is aligned
        // and in-bounds, `Stats` was initialized by `initialize`, and all
        // interior mutation goes through atomics.
        unsafe { &*(self.stats_ptr() as *const Stats) }
    }

    fn header_ptr(&self) -> *mut u8 {
        // SAFETY: `header_offset` is `0` and `Layout::new` guarantees
        // `header_offset + size_of::<Header>() <= total_bytes`, so the
        // resulting pointer is in-bounds of (or one past the end of) the
        // single mmap'd allocation, satisfying `pointer::add`'s requirements
        // (Nomicon: "Working with raw pointers"). The byte offset cannot
        // overflow `isize` because the master successfully allocated
        // `total_bytes` bytes.
        unsafe { self.base.add(self.layout.header_offset) }
    }

    fn lease_ptr(&self) -> *mut u8 {
        // SAFETY: As for `header_ptr()` — `lease_offset +
        // size_of::<LeaderLease>() <= total_bytes` by `Layout::new`, so
        // the `pointer::add` lands inside the single allocation.
        unsafe { self.base.add(self.layout.lease_offset) }
    }

    fn queue_control_ptr(&self) -> *mut u8 {
        // SAFETY: As for `header_ptr()` — `queue_control_offset +
        // size_of::<QueueControl>() <= total_bytes` per `Layout::new`.
        unsafe { self.base.add(self.layout.queue_control_offset) }
    }

    fn queue_slot_ptr(&self, index: usize) -> *mut u8 {
        debug_assert!(index < self.layout.queue_capacity);
        // SAFETY: `Layout::new` reserves `queue_capacity *
        // size_of::<QueueSlot>()` bytes starting at `queue_slots_offset` and
        // guarantees the whole range lies within `total_bytes`. The caller
        // upholds `index < queue_capacity` (checked above in debug builds),
        // so both `add` operations stay in-bounds of the one mapping
        // (Nomicon: `pointer::add` rules). The composite byte offset fits in
        // `isize` because `total_bytes` does.
        unsafe {
            self.base
                .add(self.layout.queue_slots_offset)
                .add(index * size_of::<QueueSlot>())
        }
    }

    fn aggregate_slot_ptr(&self, index: usize) -> *mut u8 {
        debug_assert!(index < self.layout.aggregate_capacity);
        // SAFETY: Symmetric to `queue_slot_ptr()` — `Layout::new` reserves
        // `aggregate_capacity * size_of::<AggregateSlot>()` bytes at
        // `aggregate_slots_offset` inside `total_bytes`, and the caller
        // upholds `index < aggregate_capacity`.
        unsafe {
            self.base
                .add(self.layout.aggregate_slots_offset)
                .add(index * size_of::<AggregateSlot>())
        }
    }

    fn stats_ptr(&self) -> *mut u8 {
        // SAFETY: As for `header_ptr()` — `stats_offset + size_of::<Stats>()
        // <= total_bytes` per `Layout::new`.
        unsafe { self.base.add(self.layout.stats_offset) }
    }
}

/// Read-side cell summary used by the access path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AggregateCell {
    pub rule_fingerprint: u128,
    pub key_hash: u128,
    pub bucket: u32,
    pub count: u64,
    pub last_update_millis: u64,
}

#[cfg(test)]
mod tests;
