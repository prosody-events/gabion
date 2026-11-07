//! Test-only helpers shared across the library's test modules.

use std::io;
use std::sync::{Arc, Mutex};

/// Collects `tracing` output so a test can read the line it asserts on.
#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Runs `body` with a subscriber that records every log line on this thread,
/// and returns its value with the lines.
pub(crate) fn with_logs<T>(body: impl FnOnce() -> T) -> (T, String) {
    let buffer = LogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let value = tracing::subscriber::with_default(subscriber, body);
    let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    (value, logs)
}
