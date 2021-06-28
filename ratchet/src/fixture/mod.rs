use bytes::{Buf, BufMut, BytesMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{cmp, io};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub fn mock() -> (MockPeer, MockStream) {
    let peer_to_stream = Arc::new(Mutex::new(BytesMut::new()));
    let stream_to_peer = Arc::new(Mutex::new(BytesMut::new()));

    let peer = MockPeer {
        tx_buf: peer_to_stream.clone(),
        rx_buf: stream_to_peer.clone(),
    };
    let stream = MockStream {
        tx_buf: stream_to_peer,
        rx_buf: peer_to_stream,
    };

    (peer, stream)
}

pub struct MockPeer {
    pub rx_buf: Arc<Mutex<BytesMut>>,
    pub tx_buf: Arc<Mutex<BytesMut>>,
}

pub struct MockStream {
    pub rx_buf: Arc<Mutex<BytesMut>>,
    pub tx_buf: Arc<Mutex<BytesMut>>,
}

impl AsyncRead for MockStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let guard = self.rx_buf.lock().unwrap();
        let rx_buf = &(*guard);
        let cnt = cmp::min(rx_buf.remaining(), buf.remaining());

        if cnt == 0 {
            Poll::Pending
        } else {
            buf.put_slice(&rx_buf[..cnt]);
            Poll::Ready(Ok(()))
        }
    }
}

impl AsyncWrite for MockStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut guard = self.tx_buf.lock().unwrap();
        let tx_buf = &mut (*guard);
        tx_buf.put_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
