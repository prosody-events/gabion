//! Format the response headers emitted on a rate-limit rejection.
//!
//! `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` and
//! `Retry-After`. All formatting goes into stack buffers; nothing allocates.

use std::fmt::Write;

use crate::access::{RejectInfo, reset_unix_seconds, retry_after_seconds};

/// Header values formatted for one rejected request. The buffers are sized
/// to fit any `u64`/`u32` value plus padding.
pub struct RejectHeaders {
    pub limit: HeaderBuffer,
    pub remaining: HeaderBuffer,
    pub reset: HeaderBuffer,
    pub retry_after: HeaderBuffer,
    pub body: BodyBuffer,
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
        std::str::from_utf8(self.as_bytes()).expect("formatted via Display")
    }

    fn write_u64(&mut self, value: u64) {
        let mut tmp = StackWriter::new(&mut self.bytes);
        // Display for u64 cannot fail and produces at most 20 ASCII bytes.
        let _ = write!(&mut tmp, "{value}");
        self.len = tmp.len as u8;
    }
}

impl Default for HeaderBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Response body buffer for the rejection text. Bounded at compile time.
#[derive(Clone, Copy)]
pub struct BodyBuffer {
    bytes: [u8; 128],
    len: u8,
}

impl BodyBuffer {
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

    fn write_body(&mut self, info: RejectInfo) {
        let mut tmp = StackWriter::new(&mut self.bytes);
        let over = info.total.saturating_add(1).saturating_sub(info.spec.limit);
        let _ = writeln!(
            &mut tmp,
            "rate limit exceeded: rule={} limit={} over_by={}",
            info.spec.id, info.spec.limit, over
        );
        self.len = tmp.len.min(self.bytes.len()) as u8;
    }
}

impl Default for BodyBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl RejectHeaders {
    /// Build the header set + body for one rejection. Conventions:
    ///
    /// * `X-RateLimit-Limit` — request budget per window (GitHub/Envoy style).
    /// * `X-RateLimit-Remaining` — `0` once we're past the budget.
    /// * `X-RateLimit-Reset` — unix-timestamp seconds at which the fixed
    ///   window's quota resets. Matches the Envoy ratelimit filter and
    ///   GitHub/Twitter.
    /// * `Retry-After` — delta-seconds per RFC 7231 §7.1.3. Always the safe
    ///   upper bound (`window_seconds`) because a client retrying sooner may
    ///   still see their earlier hits and 429 again.
    pub fn build(info: RejectInfo) -> Self {
        let retry_after_s = retry_after_seconds(info);
        let reset_unix_s = reset_unix_seconds(info);
        let mut limit = HeaderBuffer::new();
        limit.write_u64(info.spec.limit);
        let mut remaining = HeaderBuffer::new();
        remaining.write_u64(0);
        let mut reset_h = HeaderBuffer::new();
        reset_h.write_u64(reset_unix_s);
        let mut retry_after = HeaderBuffer::new();
        retry_after.write_u64(retry_after_s);
        let mut body = BodyBuffer::new();
        body.write_body(info);
        Self {
            limit,
            remaining,
            reset: reset_h,
            retry_after,
            body,
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
