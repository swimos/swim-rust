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

use std::{task::{Poll, Context}, pin::Pin, io};

use tokio::{net::TcpStream, io::{AsyncRead, AsyncWrite, ReadBuf}};
use tokio_rustls::TlsStream;
use pin_project::pin_project;

/// Either a simple, unencrypted [`TcpStream`] or a [`TlsStream`]. This allows an implementation
/// of [`ClientConnections`] and [`ServerConnections`] to expose an single socket type to support
/// both secure and unsecure connections.
#[pin_project(project = MaybeTlsProj)]
pub enum MaybeTlsStream {
    Plain(#[pin] TcpStream),
    Tls(#[pin] TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_read(cx, buf),
            MaybeTlsProj::Tls(s) => s.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_write(cx, buf),
            MaybeTlsProj::Tls(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_flush(cx),
            MaybeTlsProj::Tls(s) => s.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_shutdown(cx),
            MaybeTlsProj::Tls(s) => s.poll_shutdown(cx),
        }
    }
}