use bytes::{Buf, BufMut, BytesMut};
use http::{Request, Response, Version};
use httparse::{Error, Status};
use std::io::{Read, Write};
use std::ops::DerefMut;
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

impl MockPeer {
    pub fn write_from<R>(&self, mut readable: R)
    where
        R: ReadBytesMut,
    {
        let mut guard = self.tx_buf.lock().unwrap();
        let buf = guard.deref_mut();

        readable.read(buf);
    }

    /// Write `response` in to this peer's output buffer. This will **not** write the `extensions`
    /// field in the struct. If this is required then you will need to write it manually.
    pub fn write_response(&self, response: Response<()>) {
        self.write_from(ReadableResponse(response));
    }

    pub fn read_into<W>(&self, writer: W) -> Result<Option<W::Out>, W::Error>
    where
        W: FromBytes,
    {
        let mut guard = self.tx_buf.lock().unwrap();
        let buf = guard.deref_mut();

        match writer.write(buf) {
            Ok((read, opt)) => {
                buf.advance(read);
                Ok(opt)
            }
            Err(e) => Err(e),
        }
    }

    pub fn read_request(&self) -> Result<Option<Request<()>>, httparse::Error> {
        self.read_into(WritableRequest)
    }
}

pub trait ReadBytesMut {
    fn read(&self, buf: &mut BytesMut);
}

pub trait FromBytes {
    type Out;
    type Error;

    fn write(&self, buf: &mut BytesMut) -> Result<(usize, Option<Self::Out>), Self::Error>;
}

struct ReadableResponse(Response<()>);

struct WritableRequest;
impl FromBytes for WritableRequest {
    type Out = Request<()>;
    type Error = httparse::Error;

    fn write(&self, buf: &mut BytesMut) -> Result<(usize, Option<Self::Out>), httparse::Error> {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut httparse_request = httparse::Request::new(&mut headers);

        match httparse_request.parse(buf.as_ref()) {
            Ok(Status::Partial) => Ok((0, None)),
            Ok(Status::Complete(count)) => {
                let mut http_request = http::Request::builder();

                if let Some(version) = httparse_request.version {
                    http_request = match version {
                        0 => http_request.version(Version::HTTP_10),
                        1 => http_request.version(Version::HTTP_11),
                        v => unreachable!("{}", v),
                    };
                }

                Ok((
                    count,
                    Some(http_request.body(()).expect("Failed to parse HTTP request")),
                ))
            }
            Err(e) => Err(e),
        }
    }
}

macro_rules! format_bytes {
    ($($arg:tt)*) => {
        format!($($arg)*).as_bytes()
    };
}

impl ReadBytesMut for ReadableResponse {
    fn read(&self, buf: &mut BytesMut) {
        let ReadableResponse(response) = self;

        buf.extend_from_slice(format_bytes!(
            "{:?} {}",
            response.version(),
            response.status()
        ));

        for (name, value) in response.headers() {
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(name.as_str().as_bytes());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(value.as_bytes());
        }

        buf.extend_from_slice(b"\r\n\r\n");
    }
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
