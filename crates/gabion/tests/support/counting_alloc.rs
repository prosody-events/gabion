//! The counting global allocator every `no_alloc` test binary installs. It
//! forwards to `System` and, while [`COUNTING`] is set on the calling thread,
//! counts each allocation in [`ALLOCATIONS`] and its bytes in [`BYTES`].

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Set on this thread while a counted section runs.
    pub static COUNTING: Cell<bool> = const { Cell::new(false) };
    pub static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    pub static BYTES: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds `GlobalAlloc`'s contract; the bookkeeping touches only
// const-initialised thread-locals with no destructor, so it never allocates
// or re-enters the allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: the caller's guarantees for `layout` pass straight through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` through `alloc` or `realloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        // SAFETY: the caller's guarantees pass straight through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn note(bytes: usize) {
    if COUNTING.get() {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
        BYTES.set(BYTES.get() + bytes as u64);
    }
}
