//! Format the response headers emitted by gabion on every admission decision.
//!
//! `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset`
//! ride on both allowed and rejected responses; `Retry-After` is added on
//! rejections only. All formatting goes into stack buffers; nothing
//! allocates.

use std::fmt::Write;

use crate::access::{AllowInfo, RejectInfo};

/// Header values formatted for one admission decision. The buffers are
/// sized to fit any `u64`/`u32` value plus padding. `retry_after` is
/// `Some(_)` only on the reject path — allowed responses get the
/// `X-RateLimit-*` triplet without a `Retry-After`.
pub struct AdmissionHeaders {
    pub limit: HeaderBuffer,
    pub remaining: HeaderBuffer,
    pub reset: HeaderBuffer,
    pub retry_after: Option<HeaderBuffer>,
}

/// Header value formatted into a fixed-cap byte buffer. Holds at most 32
/// ASCII digits — comfortably more than `u64::MAX.to_string().len()`.
#[derive(Clone, Copy)]
pub struct HeaderBuffer {
    bytes: [u8; 32],
    len: u8,
}

impl HeaderBuffer {
    pub fn new() -> Self {
        Self {
            bytes: [0; 32],
            len: 0,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: the buffer is only written via `write_u64`, which
        // routes a `u64`'s `Display` output through `StackWriter`. The
        // `Display` impl for `u64` emits only ASCII digit bytes
        // (`'0'..='9'`), all of which are valid UTF-8 by themselves
        // and form a valid UTF-8 string when concatenated. No other
        // writer touches `bytes`, so every byte in `as_bytes()` is a
        // valid ASCII digit. Avoiding `.expect("...")` because this is
        // on the admission hot path (every response builds three or
        // four of these headers) and the panic surface across nginx's
        // C handler is UB if it ever fires — keeping the contract
        // local to this module pins the invariant where it's upheld.
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    fn write_u64(&mut self, value: u64) {
        let mut tmp = StackWriter::new(&mut self.bytes);
        // Display for u64 cannot fail and produces at most 20 ASCII bytes.
        let _ = write!(&mut tmp, "{value}");
        self.len = tmp.len as u8;
    }

    fn from_u64(value: u64) -> Self {
        let mut buf = Self::new();
        buf.write_u64(value);
        buf
    }
}

impl Default for HeaderBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Response body buffer for the 429 rejection text. Bounded at compile time
/// and only constructed on the reject path.
#[derive(Clone, Copy)]
pub struct RejectBody {
    bytes: [u8; 128],
    len: u8,
}

impl RejectBody {
    pub fn new() -> Self {
        Self {
            bytes: [0; 128],
            len: 0,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).unwrap_or("rate limit exceeded\n")
    }

    pub fn build(info: RejectInfo) -> Self {
        let mut body = Self::new();
        let mut tmp = StackWriter::new(&mut body.bytes);
        let over = info.total.saturating_add(1).saturating_sub(info.spec.limit);
        let _ = writeln!(
            &mut tmp,
            "rate limit exceeded: rule={} limit={} over_by={}",
            info.spec.id, info.spec.limit, over
        );
        body.len = tmp.len.min(body.bytes.len()) as u8;
        body
    }
}

impl Default for RejectBody {
    fn default() -> Self {
        Self::new()
    }
}

impl AdmissionHeaders {
    /// Build the `X-RateLimit-*` triplet for an allowed response.
    ///
    /// * `X-RateLimit-Limit` — the rule's request budget (matching the reject
    ///   path).
    /// * `X-RateLimit-Remaining` — budget left *after* this request.
    /// * `X-RateLimit-Reset` — unix-timestamp seconds at which the oldest live
    ///   bucket ages off, i.e. the next moment the client's budget might grow.
    ///   In gabion's uniform sliding window this is identical to "time until
    ///   the next bucket boundary" (no SHM walk needed — see
    ///   `gabion::window::time_until_next_bucket_boundary_millis`).
    ///
    /// No `Retry-After`: the request was admitted.
    pub fn build_for_allow(info: AllowInfo) -> Self {
        let delta = gabion::window::time_until_next_bucket_boundary_millis(
            info.now_millis,
            info.spec.bucket_millis,
        );
        let reset_unix_s = gabion::window::reset_unix_seconds(info.now_millis, delta);
        Self {
            limit: HeaderBuffer::from_u64(info.spec.limit),
            remaining: HeaderBuffer::from_u64(info.remaining),
            reset: HeaderBuffer::from_u64(reset_unix_s),
            retry_after: None,
        }
    }

    /// Build the four-header set for one rejection. Conventions:
    ///
    /// * `X-RateLimit-Limit` — request budget per window (GitHub/Envoy style).
    /// * `X-RateLimit-Remaining` — `0` once we're past the budget.
    /// * `X-RateLimit-Reset` — unix-timestamp seconds at which the rate limit
    ///   resets. Matches the Envoy ratelimit filter and GitHub/Twitter.
    /// * `Retry-After` — delta-seconds per RFC 7231 §7.1.3.
    ///
    /// Both `Retry-After` and `Reset` come from `info.delta_until_admit_millis`
    /// — the sliding-window-precise "time until a same-weight request
    /// would be admitted" computed at reject time
    /// (`gabion::window::time_until_admit_millis`). The value is bucket-
    /// distribution-aware: two clients rejected on the same key one second
    /// apart see `Retry-After` values that differ by exactly 1, and a
    /// rejection with stale hits in older buckets reports a shorter wait
    /// than the full window.
    pub fn build_for_reject(info: RejectInfo) -> Self {
        let retry_after_s = gabion::window::retry_after_seconds(info.delta_until_admit_millis);
        let reset_unix_s =
            gabion::window::reset_unix_seconds(info.now_millis, info.delta_until_admit_millis);
        Self {
            limit: HeaderBuffer::from_u64(info.spec.limit),
            remaining: HeaderBuffer::from_u64(0),
            reset: HeaderBuffer::from_u64(reset_unix_s),
            retry_after: Some(HeaderBuffer::from_u64(retry_after_s)),
        }
    }
}

/// Adapter that lets `core::fmt::write!` go straight into a `&mut [u8]`. On
/// overflow we silently truncate; the buffer is sized to fit any value we
/// emit (max 21 ASCII bytes for a `u64`).
struct StackWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl<'a> StackWriter<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, len: 0 }
    }
}

impl Write for StackWriter<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let remaining = self.buf.len().saturating_sub(self.len);
        let to_copy = s.len().min(remaining);
        self.buf[self.len..self.len + to_copy].copy_from_slice(&s.as_bytes()[..to_copy]);
        self.len += to_copy;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
