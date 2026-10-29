//! A scripted transport for runtime tests: it records every send attempt and
//! answers it from the test's script.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::task::{Poll, Waker};

use super::transport::GossipTransport;

/// One `try_send_to` call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Attempt {
    pub dst: SocketAddr,
    pub bytes: Vec<u8>,
    pub accepted: bool,
    /// Datagrams still waiting in the inbound queue at this send.
    pub inbound_left: usize,
}

/// The script, and what the runtime tried.
#[derive(Default)]
pub(super) struct Wire {
    pub attempts: RefCell<Vec<Attempt>>,
    /// While set, every send is refused with `WouldBlock`.
    pub refuse: Cell<bool>,
    /// Sends to this address fail with `PermissionDenied`.
    pub forbidden: Cell<Option<SocketAddr>>,
    /// Datagrams the runtime receives, oldest first.
    pub inbound: RefCell<VecDeque<(Vec<u8>, SocketAddr)>>,
    /// The receive buffer the transport reports, which bounds a round's read.
    pub recv_buffer: Cell<Option<usize>>,
    /// The waker the last read of an empty queue armed.
    pub armed: RefCell<Option<Waker>>,
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
            inbound_left: self.0.inbound.borrow().len(),
        });
        result
    }

    fn recv_buffer_bytes(&self) -> Option<usize> {
        self.0.recv_buffer.get()
    }

    /// Pops the oldest queued datagram; with none, it arms the waker once
    /// until a datagram is read. Nothing but a test wakes it.
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        std::future::poll_fn(|cx| {
            let next = self.0.inbound.borrow_mut().pop_front();
            let Some((bytes, src)) = next else {
                (self.0.armed.borrow_mut()).get_or_insert_with(|| cx.waker().clone());
                return Poll::Pending;
            };
            self.0.armed.take();
            buf[..bytes.len()].copy_from_slice(&bytes);
            Poll::Ready(Ok((bytes.len(), src)))
        })
        .await
    }
}
