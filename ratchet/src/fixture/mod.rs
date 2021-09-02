use crate::{Request, Response};
use bytes::BytesMut;
use http::Version;
use httparse::Status;
use std::io;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

pub fn mock() -> (MockPeer, MockPeer) {
    let (client, server) = tokio::io::duplex(usize::MAX);
    (MockPeer(client), MockPeer(server))
}

#[derive(Debug)]
pub struct MockPeer(DuplexStream);

impl Deref for MockPeer {
    type Target = DuplexStream;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsyncRead for MockPeer {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for MockPeer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

pub trait ByteReader {
    fn read(&self, buf: &mut BytesMut);
}

pub trait ByteWriter {
    type Out;
    type Error;

    fn write(&self, buf: &mut BytesMut) -> Result<Option<Self::Out>, Self::Error>;
}

struct ReadableResponse(Response);

impl ByteReader for ReadableResponse {
    fn read(&self, buf: &mut BytesMut) {
        let ReadableResponse(response) = self;

        buf.extend_from_slice(format!("{:?} {}", response.version(), response.status()).as_bytes());

        for (name, value) in response.headers() {
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(name.as_str().as_bytes());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(value.as_bytes());
        }

        buf.extend_from_slice(b"\r\n\r\n");
    }
}

struct WritableRequest;
impl ByteWriter for WritableRequest {
    type Out = Request;
    type Error = httparse::Error;

    fn write(&self, buf: &mut BytesMut) -> Result<Option<Self::Out>, httparse::Error> {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut httparse_request = httparse::Request::new(&mut headers);

        match httparse_request.parse(buf.as_ref()) {
            Ok(Status::Partial) => Ok(None),
            Ok(Status::Complete(_)) => {
                let mut http_request = http::Request::builder();

                if let Some(version) = httparse_request.version {
                    http_request = match version {
                        0 => http_request.version(Version::HTTP_10),
                        1 => http_request.version(Version::HTTP_11),
                        v => unreachable!("{}", v),
                    };
                }

                if let Some(path) = httparse_request.path {
                    http_request = http_request.uri(path);
                }

                if let Some(method) = httparse_request.method {
                    http_request = http_request.method(method);
                }

                for header in httparse_request.headers {
                    http_request = http_request.header(header.name, header.value);
                }

                Ok(Some(
                    http_request.body(()).expect("Failed to parse HTTP request"),
                ))
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug)]
pub enum ReadError<R> {
    Reader(R),
    Io(io::Error),
}

impl MockPeer {
    pub async fn copy<R>(&mut self, readable: &mut R) -> io::Result<()>
    where
        R: ByteReader,
    {
        let mut buf = BytesMut::new();
        readable.read(&mut buf);

        self.write_all(&*buf).await
    }

    /// Write `response` in to this peer's output buffer. This will **not** write the `extensions`
    /// field in the struct. If this is required then you will need to write it manually.
    pub async fn write_response(&mut self, response: Response) -> io::Result<()> {
        self.copy(&mut ReadableResponse(response)).await
    }

    pub async fn read_into<W>(&mut self, writer: W) -> Result<W::Out, ReadError<W::Error>>
    where
        W: ByteWriter,
    {
        let mut buf = BytesMut::with_capacity(1024);

        loop {
            self.read_buf(&mut buf).await.map_err(ReadError::Io)?;
            match writer.write(&mut buf).map_err(ReadError::Reader)? {
                Some(obj) => return Ok(obj),
                None => {
                    buf.reserve(1);
                    continue;
                }
            }
        }
    }

    pub async fn read_request(&mut self) -> Result<Request, ReadError<httparse::Error>> {
        self.read_into(WritableRequest).await
    }
}
