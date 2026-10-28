//! A scripted transport for runtime tests: it records every send attempt and
//! answers it from the test's script.

use std::cell::{Cell, RefCell};
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;

use super::transport::GossipTransport;

/// One `try_send_to` call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Attempt {
    pub dst: SocketAddr,
    pub bytes: Vec<u8>,
    pub accepted: bool,
}

/// The script, and what the runtime tried.
#[derive(Default)]
pub(super) struct Wire {
    pub attempts: RefCell<Vec<Attempt>>,
    /// While set, every send is refused with `WouldBlock`.
    pub refuse: Cell<bool>,
    /// Sends to this address fail with `PermissionDenied`.
    pub forbidden: Cell<Option<SocketAddr>>,
}

impl Wire {
    /// The accepted attempts, in order.
    pub fn accepted(&self) -> Vec<Attempt> {
        let attempts = self.attempts.borrow();
        attempts.iter().filter(|a| a.accepted).cloned().collect()
    }
}

pub(super) struct MockTransport(pub Rc<Wire>);

impl GossipTransport for MockTransport {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(SocketAddr::from(([127, 0, 0, 1], 1)))
    }

    fn send_buffer_bytes(&self) -> Option<usize> {
        Some(212_992)
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let result = if self.0.refuse.get() {
            Err(io::ErrorKind::WouldBlock.into())
        } else if self.0.forbidden.get() == Some(dst) {
            Err(io::ErrorKind::PermissionDenied.into())
        } else {
            Ok(buf.len())
        };
        self.0.attempts.borrow_mut().push(Attempt {
            dst,
            bytes: buf.to_vec(),
            accepted: result.is_ok(),
        });
        result
    }

    async fn recv_from(&self, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        std::future::pending().await
    }
}
