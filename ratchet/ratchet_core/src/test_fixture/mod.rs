// Copyright 2015-2021 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{Error, Request};
use bytes::{BufMut, BytesMut};
use http::{Response, Version};
use httparse::Status;
use std::any::{type_name, Any};
use std::error::Error as StdError;
use std::fmt::Debug;
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

struct ReadableResponse(Response<()>);

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

struct ReadableRequest(Request);
impl ByteReader for ReadableRequest {
    fn read(&self, buf: &mut BytesMut) {
        let ReadableRequest(request) = self;

        buf.extend_from_slice(
            format!(
                "{} {} {:?}",
                request.method(),
                request.uri().path_and_query().unwrap(),
                request.version()
            )
            .as_bytes(),
        );

        for (name, value) in request.headers() {
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

struct WritableResponse;
impl ByteWriter for WritableResponse {
    type Out = Response<()>;
    type Error = httparse::Error;

    fn write(&self, buf: &mut BytesMut) -> Result<Option<Self::Out>, httparse::Error> {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut httparse_response = httparse::Response::new(&mut headers);

        match httparse_response.parse(buf.as_ref()) {
            Ok(Status::Partial) => Ok(None),
            Ok(Status::Complete(_)) => {
                let mut http_response = http::Response::builder();

                if let Some(version) = httparse_response.version {
                    http_response = match version {
                        0 => http_response.version(Version::HTTP_10),
                        1 => http_response.version(Version::HTTP_11),
                        v => unreachable!("{}", v),
                    };
                }

                if let Some(code) = httparse_response.code {
                    http_response = http_response.status(code);
                }

                for header in httparse_response.headers {
                    http_response = http_response.header(header.name, header.value);
                }

                Ok(Some(
                    http_response
                        .body(())
                        .expect("Failed to parse HTTP response"),
                ))
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError<R> {
    #[error("Read error: `{0}`")]
    Reader(R),
    #[error("IO error: `{0}`")]
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
    pub async fn write_response(&mut self, response: Response<()>) -> io::Result<()> {
        self.copy(&mut ReadableResponse(response)).await
    }

    /// Write `request` in to this peer's output buffer. This will **not** write the `extensions`
    /// field in the struct. If this is required then you will need to write it manually.
    pub async fn write_request(&mut self, response: Request) -> io::Result<()> {
        self.copy(&mut ReadableRequest(response)).await
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

    pub async fn read_response(&mut self) -> Result<Response<()>, ReadError<httparse::Error>> {
        self.read_into(WritableResponse).await
    }
}

pub fn expect_err<O, T>(result: Result<O, Error>, expected: T)
where
    O: Debug,
    T: StdError + PartialEq + Any,
{
    let error = result.expect_err(&format!("Expected a {}", type_name::<T>()));
    let protocol_error = error.downcast_ref::<T>().unwrap();
    assert_eq!(protocol_error, &expected);
}

pub struct EmptyIo;
impl AsyncRead for EmptyIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for EmptyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
pub struct MirroredIo(pub BytesMut);

impl AsyncRead for MirroredIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let rx_buf = &mut self.as_mut().0;
        let cnt = std::cmp::min(rx_buf.len(), buf.remaining());
        let (a, b) = rx_buf.split_at(cnt);

        buf.put_slice(a);
        *rx_buf = BytesMut::from(b);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for MirroredIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.get_mut().0.put_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}
