//! Checks that nginx-sys lays out every nginx struct field gabion and `ngx`
//! touch where the C compiler does for the nginx tree it was built against.
//! See "Binding layout check" in `deploy/nginx/README.md`.

use std::env;
use std::fs;
use std::io;
use std::mem::{MaybeUninit, offset_of, size_of};
use std::path::Path;
use std::process::{Command, ExitCode};

use nginx_sys::*;

/// The C probe. Each check below `main` passes a struct and field name plus
/// the value nginx-sys has for it, and `check` compares that with C's.
const PRELUDE: &str = r#"
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <nginx.h>

static int mismatches;

static void
check(const char *what, size_t c, size_t rust)
{
    if (c != rust) {
        if (mismatches++ == 0) {
            fprintf(stderr, "gabion: the nginx-sys bindings disagree with the C "
                    "layout of " NGINX_VER ":\n");
        }
        fprintf(stderr, "  %s is %zu in C but %zu in nginx-sys\n", what, c, rust);
    }
}

#define SIZE(T, rust) check("sizeof(" #T ")", sizeof(T), rust)

#define FIELD(T, f, offset, size)                                             \
    check("offsetof(" #T ", " #f ")", offsetof(T, f), offset);                \
    check("sizeof(" #T "." #f ")", sizeof(((T *) 0)->f), size)

/* Sets each bit of a zeroed T alone and records which ones s.f reads. */
#define BITS(T, f, first, width) do {                                         \
        T s; unsigned char *b = (unsigned char *) &s;                         \
        size_t bit, c_first = 0, c_width = 0;                                 \
        memset(&s, 0, sizeof(T));                                             \
        for (bit = 0; bit < sizeof(T) * 8; bit++) {                           \
            b[bit / 8] = 1 << bit % 8;                                        \
            if (s.f) {                                                        \
                if (c_width++ == 0) {                                         \
                    c_first = bit;                                            \
                }                                                             \
            }                                                                 \
            b[bit / 8] = 0;                                                   \
        }                                                                     \
        check("first bit of bitfield " #T "." #f, c_first, first);           \
        check("bit width of bitfield " #T "." #f, c_width, width);           \
    } while (0)

int
main(void)
{
"#;

const EPILOGUE: &str = r#"
    if (mismatches) {
        fprintf(stderr, "gabion's nginx module would read or write the wrong "
                "memory through these fields on " NGINX_VER ", so this build "
                "must not ship. Usually " NGINX_VER " changed one of these "
                "structs, or bindgen placed a bitfield differently from the C "
                "compiler. Pin or patch nginx-sys in the workspace Cargo.toml "
                "to bindings that match, or stop using these fields in "
                "crates/nginx/src and drop them from "
                "crates/nginx/src/bin/nginx_layout.rs.\n");
        return 1;
    }
    printf("gabion: nginx-sys matches the C layout of " NGINX_VER " for "
           "every field gabion uses\n");
    return 0;
}
"#;

/// Emits the C checks for every listed struct size, field, and bitfield.
macro_rules! checks {
    (
        sized [$($sized:ident),* $(,)?];
        fields { $($t:ident { $($f:ident),* $(,)? })* }
        bits { $($bt:ident { $($b:ident),* $(,)? })* }
    ) => {{
        let mut c = String::from(PRELUDE);
        $(
            c += &format!("    SIZE({}, {});\n", stringify!($sized), size_of::<$sized>());
        )*
        $($(
            c += &format!(
                "    FIELD({}, {}, {}, {});\n",
                stringify!($t),
                c_name(stringify!($f)),
                offset_of!($t, $f),
                field_size(|s: &$t| &s.$f),
            );
        )*)*
        $($(
            // SAFETY: nginx-sys structs hold only integers, raw pointers,
            // and `Option` callbacks, so any bit pattern is a valid one.
            let (first, width) = unsafe { bit_span(|s: &$bt| s.$b() != 0) };
            c += &format!("    BITS({}, {}, {first}, {width});\n", stringify!($bt), stringify!($b));
        )*)*
        c + EPILOGUE
    }};
}

fn main() -> io::Result<ExitCode> {
    let source = checks! {
        // Built, embedded, or strided over by Rust, so the whole struct counts.
        sized [
            ngx_command_t,
            ngx_event_t,
            ngx_http_compile_complex_value_t,
            ngx_http_complex_value_t,
            ngx_http_module_t,
            ngx_http_phase_t,
            ngx_module_t,
            ngx_queue_t,
            ngx_rbtree_node_t,
            ngx_str_t,
        ];
        fields {
            ngx_array_t { elts, nelts }
            ngx_command_t { name, type_, set, conf, offset, post }
            ngx_conf_t { args, pool, ctx, log }
            ngx_connection_t { addr_text }
            ngx_cycle_t { conf_ctx, log }
            ngx_event_t { data, handler, log, timer, queue }
            ngx_http_compile_complex_value_t { cf, value, complex_value }
            ngx_http_conf_ctx_t { main_conf, loc_conf }
            ngx_http_core_main_conf_t { phases }
            ngx_http_headers_out_t { headers }
            ngx_http_module_t {
                preconfiguration, postconfiguration, create_main_conf, init_main_conf,
                create_srv_conf, merge_srv_conf, create_loc_conf, merge_loc_conf,
            }
            ngx_http_phase_t { handlers }
            ngx_http_request_t {
                connection, main_conf, loc_conf, pool, headers_out, uri, args, unparsed_uri,
            }
            ngx_http_variable_value_t { data }
            ngx_log_t { log_level }
            ngx_module_t {
                ctx_index, index, name, version, signature, ctx, commands, type_, init_master,
                init_module, init_process, init_thread, exit_thread, exit_process, exit_master,
            }
            ngx_pool_cleanup_t { handler, data }
            ngx_queue_t { prev, next }
            ngx_rbtree_node_t { key, left, right, parent }
            ngx_shm_t { addr, size, name }
            ngx_shm_zone_t { data, shm, init }
            ngx_str_t { len, data }
            ngx_table_elt_t { hash, key, value, lowcase_key }
        }
        bits {
            ngx_event_t { posted, timer_set, timedout, cancelable }
            ngx_http_variable_value_t { len, valid, not_found }
        }
    };

    let dir = Path::new(env!("OUT_DIR"));
    let (c_file, probe) = (dir.join("nginx_layout.c"), dir.join("nginx_layout"));
    fs::write(&c_file, source)?;
    let compiled = Command::new(env::var_os("CC").unwrap_or_else(|| "cc".into()))
        .args(env::split_paths(env!("DEP_NGINX_INCLUDE")).map(|dir| format!("-I{}", dir.display())))
        .arg(&c_file)
        .arg("-o")
        .arg(&probe)
        .status()?;
    if !compiled.success() {
        eprintln!(
            "gabion: could not compile the nginx layout probe {}, so the nginx-sys bindings went \
             unchecked. The compiler errors above say why; the probe builds with `cc` (or `$CC`) \
             against the include path nginx-sys generated its bindings from.",
            c_file.display(),
        );
        return Ok(ExitCode::FAILURE);
    }
    Ok(if Command::new(&probe).status()?.success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// bindgen appends `_` to fields named after Rust keywords (`type_`).
fn c_name(field: &str) -> &str {
    field.strip_suffix('_').unwrap_or(field)
}

/// The size of the field `field` projects out of a `T`.
fn field_size<T, F>(_field: fn(&T) -> &F) -> usize {
    size_of::<F>()
}

/// The first bit and the bit count of the bits `read` sees in a `T`, found
/// the same way the C side's `BITS` does. Not run under Miri: it needs
/// bindings for a configured nginx tree.
///
/// # Safety
///
/// Every bit pattern must be a valid `T`.
unsafe fn bit_span<T>(read: fn(&T) -> bool) -> (usize, usize) {
    let mut value = MaybeUninit::<T>::zeroed();
    let bytes = value.as_mut_ptr().cast::<u8>();
    let (mut first, mut width) = (0, 0);
    for bit in 0..size_of::<T>() * 8 {
        // SAFETY: `bit / 8 < size_of::<T>()`, so the writes stay inside
        // `value`, and the caller guarantees any bit pattern is a valid `T`.
        let set = unsafe {
            *bytes.add(bit / 8) = 1 << (bit % 8);
            let set = read(value.assume_init_ref());
            *bytes.add(bit / 8) = 0;
            set
        };
        if set {
            if width == 0 {
                first = bit;
            }
            width += 1;
        }
    }
    (first, width)
}
