// Copyright 2015-2021 SWIM.AI inc.
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

use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[pin_project(project = MaybeTls)]
pub enum StreamSwitcher<S, T> {
    Plain(#[pin] S),
    Tls(#[pin] T),
}

impl<S, T> AsyncRead for StreamSwitcher<S, T>
where
    S: AsyncRead + Unpin,
    T: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            MaybeTls::Plain(ref mut s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(ref mut s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl<S, T> AsyncWrite for StreamSwitcher<S, T>
where
    S: AsyncWrite + Unpin,
    T: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.project() {
            MaybeTls::Plain(ref mut s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            MaybeTls::Plain(ref mut s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            MaybeTls::Plain(ref mut s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
