// Copyright 2015-2020 SWIM.AI inc.
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

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

use futures::future::BoxFuture;
use futures::stream::{Fuse, FusedStream};
use futures::task::{Context, Poll};
use futures::FutureExt;
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{lookup_host, TcpListener, TcpStream};

pub trait Listener {
    type Socket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static;
    type AcceptStream: FusedStream<Item = io::Result<(Self::Socket, SocketAddr)>> + Unpin;

    fn into_stream(self) -> Self::AcceptStream;
}

#[pin_project]
#[derive(Debug)]
pub struct WithPeer(#[pin] TcpListener);

impl Stream for WithPeer {
    type Item = io::Result<(TcpStream, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().0.poll_accept(cx).map(Some)
    }
}

impl Listener for TcpListener {
    type Socket = TcpStream;
    type AcceptStream = Fuse<WithPeer>;

    fn into_stream(self) -> Self::AcceptStream {
        WithPeer(self).fuse()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TokioNetworking;

pub trait ExternalConnections: Clone + Send + Sync + 'static {
    type Socket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static;
    type ListenerType: Listener<Socket = Self::Socket>;

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::ListenerType>>;
    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::Socket>>;
    fn lookup(&self, host: String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>>;
}

impl ExternalConnections for TokioNetworking {
    type Socket = TcpStream;
    type ListenerType = TcpListener;

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::ListenerType>> {
        TcpListener::bind(addr).boxed()
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::Socket>> {
        TcpStream::connect(addr).boxed()
    }

    fn lookup(&self, host: String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        lookup_host(host)
            .map(|r| r.map(|it| it.collect::<Vec<_>>()))
            .boxed()
    }
}
