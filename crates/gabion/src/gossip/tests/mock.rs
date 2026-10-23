//! A transport that records every datagram it sends, for tests that assert
//! on who sent what to whom.

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;

use crate::gossip::GossipTransport;
use crate::gossip::sim::SimTransport;

pub(super) struct Sent {
    pub(super) src: SocketAddr,
    pub(super) dst: SocketAddr,
    pub(super) bytes: Vec<u8>,
}

pub(super) type SentLog = Rc<RefCell<Vec<Sent>>>;

pub(super) struct RecordingTransport {
    inner: SimTransport,
    log: SentLog,
}

impl RecordingTransport {
    pub(super) fn new(inner: SimTransport, log: SentLog) -> Self {
        Self { inner, log }
    }
}

impl GossipTransport for RecordingTransport {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a {
        self.inner.writable()
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let sent = self.inner.try_send_to(buf, dst)?;
        self.log.borrow_mut().push(Sent {
            src: self.inner.addr(),
            dst,
            bytes: buf.to_vec(),
        });
        Ok(sent)
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + 'a {
        self.inner.recv_from(buf)
    }
}
