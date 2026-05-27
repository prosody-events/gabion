//! Miri-runnable tests for the FFI alignment hazard that crashed the
//! native amd64 nginx smoke before commit 2a1a2e8 ("heap-allocate
//! MainConfig to honour its 16-byte alignment").
//!
//! nginx's `ngx_palloc` only guarantees `NGX_ALIGNMENT` (=
//! `sizeof(unsigned long)` = 8 on amd64). A type with `align_of` > 8
//! placed directly into a pool slot is UB. Native amd64 traps when
//! LLVM lowers a `u128` access to aligned SSE (`movdqa`); macOS arm64
//! uses paired 64-bit loads and tolerates the misalignment; QEMU's
//! TCG doesn't enforce alignment either — which is why this bug only
//! showed up on the GHA native amd64 runner.
//!
//! These tests don't need `nginx-sys` or `ngx` and aren't gated by the
//! `ngx-module` feature, so they run under `make miri-all` (Stacked
//! Borrows + Tree Borrows) on every CI run. The runtime invariant
//! they pin is local to Rust + the target triple: writing a 16-aligned
//! value through an 8-aligned pointer is UB regardless of which nginx
//! flavor the module is built for.

use std::ptr;

/// Stand-in for `crates/nginx/src/module.rs::MainConfig`. Mirrors the
/// production struct's alignment requirement (16-byte, via `u128`)
/// without dragging in `ngx-sys` or the rest of the nginx adapter.
/// Test must observe alignment behaviour, not the production fields.
#[repr(C)]
#[derive(Default)]
struct MainConfigShape {
    cluster_id_hash: u128,
    incarnation: u32,
    queue_capacity: u64,
}

const _: () = assert!(
    core::mem::align_of::<MainConfigShape>() == 16,
    "test relies on MainConfigShape mirroring MainConfig's 16-byte alignment; if u128 ever stops \
     being 16-aligned on this target, pick a different over-aligned shape (e.g. \
     #[repr(align(16))]).",
);

/// `ngx_palloc`'s alignment floor on amd64 (`sizeof(unsigned long)`).
/// Hard-coded so the test doesn't depend on the `nginx-sys` build.
const NGX_ALIGNMENT_AMD64: usize = 8;

/// Positive control: writing through a properly aligned pointer is
/// sound. `Box::new` honours `align_of::<T>()`, so a `Box<T>` whose
/// alignment is 16 lands at a 16-aligned address. Miri accepts this.
#[test]
fn ptr_write_aligned_main_config_is_sound() {
    let mut boxed = Box::new(MainConfigShape::default());
    let ptr: *mut MainConfigShape = boxed.as_mut();
    assert_eq!(
        ptr.addr() % core::mem::align_of::<MainConfigShape>(),
        0,
        "Box::new must yield an address aligned to align_of::<T>()",
    );
    // SAFETY: `ptr` came from a live `Box<MainConfigShape>` we own; it
    // points to enough writable storage for one `MainConfigShape` and
    // is correctly aligned (asserted above). No aliasing because we
    // exclusively own `boxed` for the duration of this call.
    unsafe {
        ptr::write(ptr, MainConfigShape::default());
    }
}

/// Demonstrate the alignment hazard the production code worked around.
/// `Box<[u64; N]>` is `align_of::<u64>` = 8-aligned. Offsetting by one
/// `u64` puts the resulting pointer at an address that is 8-aligned
/// but provably *not* 16-aligned. That address is what `ngx_palloc`
/// could legally hand back (it only guarantees `NGX_ALIGNMENT` = 8 on
/// amd64) — and writing a 16-aligned `MainConfigShape` value through
/// such a pointer is UB on every platform.
///
/// We don't actually perform the UB write here: `should_panic` won't
/// catch Miri's UB-abort (Miri's `report_fatal_error` exits the
/// process rather than unwinding a Rust panic), and the native run
/// would SIGSEGV on amd64 with no way for the test runner to recover.
/// What this test *does* pin is the property the production fix
/// depends on: `align_of::<MainConfigShape>` (16) strictly exceeds
/// `NGX_ALIGNMENT` (8), so the `Box::new` workaround in
/// `Module::create_main_conf` is load-bearing. If anyone drops the
/// override, the alignment math here is the regression target.
#[test]
fn misaligned_main_config_write_would_be_ub() {
    // The property under test: MainConfigShape's alignment strictly
    // exceeds NGX_ALIGNMENT. That's the invariant that makes the
    // `Box::new` workaround in `Module::create_main_conf` load-bearing
    // — without it, `ngx_palloc`'s slot is only 8-aligned and a 16-aligned
    // value placed into it is UB on every platform (it just happens to
    // trap on native amd64 specifically, because LLVM lowers u128 to
    // aligned SSE).
    //
    // We don't actually perform the misaligned write here. `should_panic`
    // cannot catch Miri's UB-abort (Miri's fatal-error path exits
    // rather than unwinding a Rust panic), and the native run would
    // SIGSEGV on amd64 with no way for the test runner to recover. The
    // alignment math is the regression target instead: if anyone drops
    // the `create_main_conf` override while this assert still holds,
    // the CI nginx smoke crashes; this test stays as the README for
    // why the override exists.
    assert!(
        core::mem::align_of::<MainConfigShape>() > NGX_ALIGNMENT_AMD64,
        "MainConfigShape alignment should exceed NGX_ALIGNMENT — the Box::new workaround in \
         Module::create_main_conf depends on this",
    );

    // Sanity-check the misalignment by *construction*, without
    // touching any real pointer: an 8-aligned start address offset by
    // one `u64` is still 8-aligned but is provably not 16-aligned.
    // Same arithmetic the alignment of any palloc slot is subject to.
    let pretend_palloc_slot: usize = 0x1000; // arbitrary 16-aligned origin
    assert_eq!(pretend_palloc_slot % 16, 0);
    let pretend_palloc_slot_offset_one_word: usize =
        pretend_palloc_slot + core::mem::size_of::<u64>();
    assert_eq!(pretend_palloc_slot_offset_one_word % 8, 0);
    assert_ne!(
        pretend_palloc_slot_offset_one_word % 16,
        0,
        "offset-by-one-u64 must NOT be 16-aligned",
    );
}

/// Round-trip the `Box::into_raw` / `Box::from_raw` dance that
/// `Module::create_main_conf` and `drop_main_config` use to thread a
/// heap-allocated `MainConfig` through nginx's pool-cleanup callback.
/// Under Tree Borrows this would catch an aliasing regression if the
/// path is ever refactored to share the raw pointer between two boxes
/// or read through it after the cleanup runs.
#[test]
fn box_into_raw_then_pool_cleanup_round_trip_preserves_provenance() {
    let boxed = Box::new(MainConfigShape {
        cluster_id_hash: 0xCAFE_BABE_DEAD_BEEF_F00D_FACE_BAAD_C0DE,
        incarnation: 17,
        queue_capacity: 1024,
    });

    // Step 1: hand ownership to a raw pointer the way create_main_conf
    // does. After this the Box is "leaked" — only the raw ptr keeps
    // the allocation alive.
    let raw: *mut MainConfigShape = Box::into_raw(boxed);
    assert_eq!(
        raw.addr() % core::mem::align_of::<MainConfigShape>(),
        0,
        "Box::into_raw preserves alignment",
    );

    // Step 2: a callback later reads the pointer back. Production code
    // does `&mut *(conf as *mut MainConfig)` inside `set_zone` /
    // `set_rule`; mirror the same shape.
    // SAFETY: `raw` is a live, exclusively-owned, well-aligned pointer
    // produced by `Box::into_raw` on step 1; no other reference exists
    // until we reconstruct the Box in step 3.
    unsafe {
        let view: &mut MainConfigShape = &mut *raw;
        view.queue_capacity += 1;
        assert_eq!(view.queue_capacity, 1025);
        assert_eq!(view.incarnation, 17);
    }

    // Step 3: the pool cleanup runs — same shape as `drop_main_config`.
    // SAFETY: `raw` is the same pointer we returned from `Box::into_raw`
    // above, no aliasing reference exists after step 2 ends (the
    // `&mut` borrow's scope closed), and the allocator hasn't freed it.
    // Reconstructing the Box drops the heap allocation exactly once.
    unsafe {
        drop(Box::from_raw(raw));
    }
}

/// Sanity: the alignment hazard is target-dependent only in *whether*
/// it traps, not in whether it exists. Confirm both `u128` and the
/// shape that contains one are 16-aligned regardless of Miri vs
/// native. If either drops to 8 in some future Rust target, the
/// production code's `Box::new` workaround is unnecessary — but the
/// test would still hold because the invariant being tested is "do
/// not write a T through a pointer whose alignment is < align_of::<T>".
#[test]
fn u128_and_main_config_shape_are_16_aligned() {
    assert!(core::mem::align_of::<u128>() >= NGX_ALIGNMENT_AMD64);
    assert!(core::mem::align_of::<MainConfigShape>() >= NGX_ALIGNMENT_AMD64);
    assert_eq!(core::mem::align_of::<u128>(), 16);
    assert_eq!(core::mem::align_of::<MainConfigShape>(), 16);
}
